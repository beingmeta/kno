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
#include "framerd/ports.h"

#include <libu8/libu8io.h>
#include <libu8/u8filefns.h>
#include <libu8/u8pathfns.h>

#include <errno.h>
#include <math.h>

#include <archive.h>
#include <archive_entry.h>

static ssize_t maxbufsize = 16*1024*1024;

#if (ARCHIVE_VERSION_NUMBER > 3002000)
#define entry_pathname archive_entry_pathname_utf8
#define entry_uname archive_entry_uname_utf8
#define entry_gname archive_entry_gname_utf8
#define entry_symlink archive_entry_symlink_utf8
#else
#define entry_pathname archive_entry_pathname
#define entry_uname archive_entry_uname
#define entry_gname archive_entry_gname
#define entry_symlink archive_entry_symlink
#endif

fd_ptr_type fd_libarchive_type;
FD_EXPORT int fd_init_libarchive(void) FD_LIBINIT_FN;

static long long int libarchive_initialized = 0;

typedef struct FD_ARCHIVE {
  FD_CONS_HEADER;
  u8_string archive_spec;
  struct archive *fd_archive;
  u8_mutex archive_lock;
  lispval archive_cur;
  lispval archive_opts;
  lispval archive_source;
  lispval archive_refs;} *fd_archive;

typedef struct FD_ARCHIVE_INPUT {
  U8_INPUT_FIELDS;
  ssize_t bytes_read, bytes_total;
  unsigned int archive_owned;
  u8_string archive_eltname, archive_id;
  lispval entry_info;
  struct archive *inport_archive;} *fd_archive_input;

static u8_string archive_errmsg(u8_byte *buf,size_t len,
                                struct FD_ARCHIVE *archive)
{
  return u8_sprintf(buf,len,"(%s)%s",
                    archive->archive_spec,
                    archive_error_string(archive->fd_archive));
}

static lispval new_archive(lispval spec,lispval opts)
{
  int status = -1;
  u8_string use_spec = NULL;
  struct archive *archive = archive_read_new();
  archive_read_support_filter_all(archive);
  archive_read_support_format_all(archive);
  archive_read_support_format_raw(archive);
  if (FD_STRINGP(spec)) {
    long long bufsz = fd_getfixopt(opts,"BUFSIZE",16000);
    u8_string abspath = u8_abspath(FD_CSTRING(spec),NULL);
    status = archive_read_open_filename(archive,abspath,bufsz);
    if (status == ARCHIVE_OK)
      use_spec = abspath;}
  else if (FD_PACKETP(spec)) {
    status = archive_read_open_memory
      (archive,((void *)FD_PACKET_DATA(spec)),FD_PACKET_LENGTH(spec));
    if (status == ARCHIVE_OK)
      use_spec = u8_mkstring("%lldB@0x%llx",
                             FD_PACKET_LENGTH(spec),
                             (unsigned long long) FD_PACKET_DATA(spec));}
  else {
    fd_seterr("InvalidArchiveSpec","new_archive",
              archive_error_string(archive),spec);
    archive_read_close(archive);
    return FD_ERROR_VALUE;}
  if (status < 0) {
    fd_seterr("LibArchiveError","new_archive",
              archive_error_string(archive),
              spec);
    archive_read_close(archive);
    return FD_ERROR_VALUE;}
  else {
    struct FD_ARCHIVE *obj = u8_alloc(struct FD_ARCHIVE);
    FD_INIT_FRESH_CONS(obj,fd_libarchive_type);
    u8_init_mutex(&(obj->archive_lock));
    obj->archive_spec   = use_spec;
    obj->fd_archive     = archive;
    obj->archive_source = spec; fd_incref(spec);
    obj->archive_opts   = opts; fd_incref(opts);
    obj->archive_cur    = FD_FALSE;
    obj->archive_refs   = FD_EMPTY_CHOICE;
    return (lispval) obj;}
}

static  int archive_seek(struct FD_ARCHIVE *archive,lispval seek,
                         struct archive_entry **entryp)
{
  struct archive_entry *entry;
  u8_byte msgbuf[1000];
  int seek_count = (FD_UINTP(seek)) ? (FD_FIX2INT(seek)) : (-1);
  int rv = archive_read_next_header(archive->fd_archive,&entry);
  while (rv == ARCHIVE_OK) {
    if (seek_count == 0) {
      if (entryp) *entryp = entry;
      return 1;}
    else if  (seek_count > 0) {
      seek_count--;}
    else if ( (FD_VOIDP(seek)) || (FD_FALSEP(seek)) ||
              (FD_DEFAULTP(seek)) || (FD_TRUEP(seek)) ) {
      if (entryp) *entryp = entry;
      return 1;}
    else if (FD_STRINGP(seek)) {
      if (strcmp(FD_CSTRING(seek),entry_pathname(entry)) == 0) {
        if (entryp) *entryp = entry;
        return 1;}}
    else if (FD_TYPEP(seek,fd_regex_type)) {
      u8_string name = entry_pathname(entry);
      ssize_t match = fd_regex_op(match,seek,name,strlen(name),0);
      if (match>0) {
        if (entryp) *entryp = entry;
        return 1;}}
    else {
      fd_seterr("BadSeekSpec","archive_seek",
                archive_errmsg(msgbuf,1000,archive),
                seek);
      return -1;}
    rv = archive_read_next_header(archive->fd_archive,&entry);}
  if (rv == ARCHIVE_OK) {}
    return 0;
  fd_seterr("ArchiveError","archive_find",
            archive_errmsg(msgbuf,1000,archive),
            seek);
  return -1;
}

static int unparse_archive(struct U8_OUTPUT *out,lispval x)
{
  struct FD_ARCHIVE *fdarchive = (struct FD_ARCHIVE *)x;
  u8_string format = archive_format_name(fdarchive->fd_archive);
  int n_filters = archive_filter_count(fdarchive->fd_archive);
  u8_puts(out,"#<FileArchive ");
  if (fdarchive->archive_spec) {
    u8_puts(out,fdarchive->archive_spec);
    u8_puts(out," (");}
  int i =0; while (i<n_filters) {
    u8_string filter = archive_filter_name(fdarchive->fd_archive,i);
    if (i>0) u8_putc(out,'|');
    u8_puts(out,filter);
    i++;}
  u8_putc(out,'|');

  if (format) u8_puts(out,format);
  else u8_puts(out,"badformat");
  u8_printf(out,"#!0x%llx>",(unsigned long long)fdarchive);
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

/* Archive input */

static int close_archive_input(struct U8_INPUT *raw_input)
{
  struct FD_ARCHIVE_INPUT *in = (fd_archive_input) raw_input;
  if (in->archive_owned) archive_read_close(in->inport_archive);
  if (in->u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(in->u8_inbuf);
  if (in->u8_streaminfo&U8_STREAM_MALLOCD) u8_free(in);
  return 1;
}

static void compress_input(struct FD_ARCHIVE_INPUT *input)
{
  if (input->u8_read > input->u8_inbuf) {
    size_t n_read = input->u8_read - input->u8_inbuf;
    size_t n = input->u8_inlim - input->u8_read;
    memmove(input->u8_inbuf,input->u8_read,n);
    input->u8_read = input->u8_inbuf;
    input->u8_inlim = input->u8_inlim - n_read;
    input->bytes_read += n_read;}
}

static int read_from_archive(struct U8_INPUT *raw_input)
{
  struct FD_ARCHIVE_INPUT *in = (fd_archive_input) raw_input;
  struct archive *archive = in->inport_archive;
  compress_input(in);
  ssize_t space = in->u8_bufsz - (in->u8_inlim-in->u8_inbuf);
  int tries = 0; double last_wait = 0, wait = 0.1;
  if (space == 0) {
    if (raw_input->u8_bufsz >= maxbufsize) {
      u8_seterr("ArchiveBufferOverflow","read_from_archive",
                u8_mkstring("%s@%lld",in->archive_id,raw_input->u8_bufsz));
      return -1;}
    u8_grow_input_stream(raw_input,-1);
    space = in->u8_bufsz - (in->u8_inlim-in->u8_inbuf);}
  ssize_t rv = archive_read_data(archive,in->u8_inlim,space);
  while ( (rv == ARCHIVE_RETRY) && (tries < 42) ) {
    double next_wait = wait+last_wait;
    u8_sleep(wait);
    rv = archive_read_data(archive,in->u8_inlim,space);
    wait = next_wait;}
  if (rv >= 0) {
    in->u8_inlim = in->u8_inlim+rv;
    return rv;}
  else {
    if (rv == ARCHIVE_FATAL) {
      u8_seterr("ArchiveError","read_from_archive",
                u8_mkstring("%s@%s:%s",
                            archive_error_string(archive),
                            in->archive_eltname,
                            in->archive_id));
      return -1;}
    else if (rv == ARCHIVE_WARN) {
      u8_log(LOG_WARN,"ArchiveRead","%s from %s",
             archive_error_string(archive),
             u8_mkstring("%s@%s:%s",
                         archive_error_string(archive),
                         in->archive_eltname,
                         in->archive_id));
      return space;}
    else if (rv == ARCHIVE_RETRY) {
      u8_seterr("ArchiveTimeout","read_from_archive",
                u8_mkstring("%s@%s:%s",
                            archive_error_string(archive),
                            in->archive_eltname,
                            in->archive_id));
      return -1;}
    else {
      u8_seterr("BadArchiveReturnValue","read_from_archive",
                u8_mkstring("%s@%s:%s",
                            archive_error_string(archive),
                            in->archive_eltname,
                            in->archive_id));
      return -1;}}
}

static lispval entry_info(struct archive_entry *entry);

static fd_port open_archive_input(struct archive *archive,
                                  u8_string archive_id,
                                  u8_string eltname,
                                  struct archive_entry *entry)
{
  struct FD_ARCHIVE_INPUT *in = u8_alloc(struct FD_ARCHIVE_INPUT);
  U8_INIT_INPUT_X((u8_input)in,30000,NULL,U8_STREAM_MALLOCD);
  in->archive_id = u8_strdup(archive_id);
  in->archive_eltname = u8_strdup(eltname);
  in->inport_archive = archive;
  in->u8_fillfn = read_from_archive;
  in->u8_closefn = close_archive_input;
  in->bytes_read = 0;
  if (entry)
    in->bytes_total = archive_entry_size(entry);
  else in->bytes_total = -1;
  if (entry)
    in->entry_info = entry_info(entry);
  else in->entry_info = fd_make_slotmap(5,0,NULL);
  struct FD_PORT *port = u8_alloc(struct FD_PORT);
  FD_INIT_CONS(port,fd_port_type);
  port->port_input = (u8_input)in;
  port->port_output = NULL;
  port->port_id = u8_mkstring("%s..%s",archive_id,eltname);
  port->port_lisprefs = FD_EMPTY;
  return port;
}

/* Top level functions */

static lispval open_archive(lispval spec,lispval path,lispval opts)
{
  if ( (FD_STRINGP(opts)) && (FD_TABLEP(path)) ) {
    lispval swap = path; path=opts; opts=swap;}
  if (FD_ABORTP(path)) return path;
  if ( (FD_STRINGP(path)) || (FD_UINTP(path)) || (FD_TRUEP(path)) ) {
    lispval archive_ptr = (FD_TYPEP(spec,fd_libarchive_type)) ?
      (fd_incref(spec)) :
      (new_archive(spec,opts));
    struct FD_ARCHIVE *archive = (fd_archive) archive_ptr;
    struct archive_entry *entry;
    int rv = archive_seek(archive,path,&entry);
    if (rv<0) return FD_ERROR_VALUE;
    else if (rv == 0) {
      if (fd_testopt(opts,FDSYM_DROP,FD_TRUE)) {
        u8_byte msgbuf[1000];
        fd_seterr("NotFound","open_archive",
                  archive_errmsg(msgbuf,1000,archive),
                  path);
        fd_decref(spec);
        return FD_ERROR;}
      else return FD_FALSE;}
    u8_byte buf[64];
    u8_string pathname =
      (FD_STRINGP(path)) ? (FD_CSTRING(path)) :
      (FD_FIXNUMP(path)) ? (u8_sprintf(buf,64,"%d",FD_FIX2INT(path))) :
      ((u8_string)"root");
    fd_port inport =
      open_archive_input(archive->fd_archive,
                         archive->archive_spec,
                         pathname,
                         entry);
    FD_ADD_TO_CHOICE(inport->port_lisprefs,archive_ptr);
    return LISPVAL(inport);}
  else if ( (FD_FALSEP(path)) || (FD_VOIDP(path)) || (FD_DEFAULTP(path)) )
    return new_archive(spec,opts);
  else return fd_err("BadArchivePath","open_archive",NULL,path);
}

/* Archive entries */

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
  set_string_prop(tbl,"UID",archive_entry_uname(entry));
  set_string_prop(tbl,"GID",archive_entry_gname(entry));
  set_string_prop(tbl,"SYMLINK",archive_entry_symlink(entry));
  set_string_prop(tbl,"PATH",entry_pathname(entry));
  set_string_prop(tbl,"FLAGS",archive_entry_fflags_text(entry));
  if (archive_entry_size_is_set(entry))
    set_int_prop(tbl,"SIZE",archive_entry_size(entry));
  return tbl;
}

static lispval archive_find(lispval obj,lispval seek)
{
  struct FD_ARCHIVE *archive = (struct FD_ARCHIVE *) obj;
  struct archive_entry *entry;
  if (! ( (FD_VOIDP(seek)) || (FD_FALSEP(seek)) || (FD_TRUEP(seek)) ||
          (FD_STRINGP(seek)) || (FD_TYPEP(seek,fd_regex_type)) ||
          (FD_UINTP(seek))) )
    return fd_err("InvalidArchivePathSpec","archive_find",NULL,seek);
  if (FD_TRUEP(seek)) {
    if (FD_VOIDP(archive->archive_cur))
      return FD_FALSE;
    else return fd_incref(archive->archive_cur);}
  int rv = archive_seek(archive,seek,&entry);
  if (rv < 0)
    return FD_ERROR_VALUE;
  else if (rv) {
    ssize_t header_pos = archive_read_header_position(archive->fd_archive);
    lispval info = entry_info(entry);
    set_int_prop(info,"POS",FD_INT(header_pos));
    fd_decref(archive->archive_cur);
    archive->archive_cur = info;
    fd_incref(info);
    return info;}
  else return FD_FALSE;
}

static lispval archive_stat(lispval port)
{
  lispval info;
  struct FD_PORT *p = fd_consptr(struct FD_PORT *,port,fd_port_type);
  struct FD_ARCHIVE_INPUT *in = (fd_archive_input) (p->port_input);
  if (in->u8_closefn != close_archive_input)
    return fd_err("NotAnArchiveStream","archive_stat",NULL,port);
  else info = in->entry_info;

  lispval bytes_read = FD_INT(in->bytes_read);
  fd_store(info,fd_intern("BYTEPOS"),bytes_read);
  fd_decref(bytes_read);

  return fd_incref(info);
}

FD_EXPORT int fd_init_libarchive()
{
  lispval module;
  if (libarchive_initialized)
    return libarchive_initialized;
  else libarchive_initialized = u8_millitime();

  module = fd_new_cmodule("LIBARCHIVE",0,fd_init_libarchive);

  fd_libarchive_type = fd_register_cons_type("archive");
  fd_unparsers[fd_libarchive_type] = unparse_archive;
  fd_recyclers[fd_libarchive_type] = recycle_archive;

  fd_idefn3(module,"ARCHIVE/OPEN",open_archive,1,
            "Opens an archive file",
            -1,FD_VOID,-1,FD_FALSE,-1,FD_FALSE);
  fd_idefn2(module,"ARCHIVE/FIND",archive_find,1,
            "Get the next archive entry (possibly matching a string or regex)",
            fd_libarchive_type,FD_VOID,-1,FD_FALSE);
  fd_idefn1(module,"ARCHIVE/STAT",archive_stat,1,
            "Information about an open archive stream",
            fd_port_type,FD_VOID);


  fd_finish_module(module);

  u8_register_source_file(_FILEINFO);

  return libarchive_initialized;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
