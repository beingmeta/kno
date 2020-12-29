/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/eval.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/frames.h"
#include "kno/numbers.h"
#include "kno/support.h"
#include "kno/ports.h"
#include "kno/cprims.h"

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

static lispval archive_typetag;

#define KNO_ARCHIVE_TYPE 0x3c98f9d03f706c8L
kno_lisp_type kno_archive_type;
KNO_EXPORT int kno_init_libarchive(void) KNO_LIBINIT_FN;

static long long int libarchive_initialized = 0;

typedef struct KNO_ARCHIVE {
  u8_string archive_spec;
  struct archive *kno_archive;
  u8_mutex archive_lock;
  lispval archive_cur;
  lispval archive_opts;
  lispval archive_source;
  lispval archive_refs;} *kno_archive;

typedef struct KNO_ARCHIVE_INPUT {
  U8_INPUT_FIELDS;
  ssize_t bytes_read, bytes_total;
  unsigned int archive_owned;
  u8_string archive_eltname, archive_id;
  lispval entry_info;
  struct archive *inport_archive;} *kno_archive_input;

static void recycle_archive(void *c)
{
  kno_archive a = (kno_archive) c;
  archive_read_close(a->kno_archive);
  u8_destroy_mutex(&(a->archive_lock));
  kno_decref(a->archive_source);
  kno_decref(a->archive_opts);
  archive_read_close(a->kno_archive);
  u8_free(a->archive_spec);
}

static u8_string archive_errmsg(u8_byte *buf,size_t len,
				struct KNO_ARCHIVE *archive)
{
  return u8_sprintf(buf,len,"(%s)%s",
		    archive->archive_spec,
		    archive_error_string(archive->kno_archive));
}

static lispval new_archive(lispval spec,lispval opts)
{
  int status = -1;
  u8_string use_spec = NULL;
  struct archive *archive = archive_read_new();
  archive_read_support_filter_all(archive);
  archive_read_support_format_all(archive);
  archive_read_support_format_raw(archive);
  if (KNO_STRINGP(spec)) {
    long long bufsz = kno_getfixopt(opts,"BUFSIZE",16000);
    u8_string abspath = u8_abspath(KNO_CSTRING(spec),NULL);
    status = archive_read_open_filename(archive,abspath,bufsz);
    if (status == ARCHIVE_OK)
      use_spec = abspath;}
  else if (KNO_PACKETP(spec)) {
    status = archive_read_open_memory
      (archive,((void *)KNO_PACKET_DATA(spec)),KNO_PACKET_LENGTH(spec));
    if (status == ARCHIVE_OK)
      use_spec = u8_mkstring("%lldB@0x%llx",
			     KNO_PACKET_LENGTH(spec),
			     KNO_LONGVAL( KNO_PACKET_DATA (spec) ));}
  else {
    kno_seterr("InvalidArchiveSpec","new_archive",
	       archive_error_string(archive),spec);
    archive_read_close(archive);
    return KNO_ERROR_VALUE;}
  if (status < 0) {
    kno_seterr("LibArchiveError","new_archive",
	       archive_error_string(archive),
	       spec);
    archive_read_close(archive);
    return KNO_ERROR_VALUE;}
  else {
    struct KNO_ARCHIVE *obj = u8_alloc(struct KNO_ARCHIVE);
    u8_init_mutex(&(obj->archive_lock));
    obj->archive_spec   = use_spec;
    obj->kno_archive     = archive;
    obj->archive_source = spec; kno_incref(spec);
    obj->archive_opts   = opts; kno_incref(opts);
    obj->archive_cur    = KNO_FALSE;
    obj->archive_refs   = KNO_EMPTY_CHOICE;
    return kno_wrap_pointer(obj,
			    sizeof(struct KNO_ARCHIVE),
			    recycle_archive,
			    archive_typetag,
			    use_spec);}
}

struct KNO_ARCHIVE *get_archive(lispval arg)
{
  if (KNO_RAW_TYPEP(arg,archive_typetag)) {
    struct KNO_RAWPTR *ptr = (kno_rawptr) arg;
    return (kno_archive) ptr->ptrval;}
  else return NULL;
}

static int archive_seek(struct KNO_ARCHIVE *archive,lispval seek,
			struct archive_entry **entryp)
{
  struct archive_entry *entry;
  u8_byte msgbuf[1000];
  int seek_count = (KNO_UINTP(seek)) ? (KNO_FIX2INT(seek)) : (-1);
  int rv = archive_read_next_header(archive->kno_archive,&entry);
  while (rv == ARCHIVE_OK) {
    if (seek_count == 0) {
      if (entryp) *entryp = entry;
      return 1;}
    else if  (seek_count > 0) {
      seek_count--;}
    else if ( (KNO_VOIDP(seek)) || (KNO_FALSEP(seek)) ||
	      (KNO_DEFAULTP(seek)) || (KNO_TRUEP(seek)) ) {
      if (entryp) *entryp = entry;
      return 1;}
    else if (KNO_STRINGP(seek)) {
      if (strcmp(KNO_CSTRING(seek),entry_pathname(entry)) == 0) {
	if (entryp) *entryp = entry;
	return 1;}}
    else if (KNO_TYPEP(seek,kno_regex_type)) {
      u8_string name = entry_pathname(entry);
      ssize_t match = kno_regex_op(rx_exactmatch,seek,name,strlen(name),0);
      if (match>0) {
	if (entryp) *entryp = entry;
	return 1;}}
    else {
      kno_seterr("BadSeekSpec","archive_seek",
		 archive_errmsg(msgbuf,1000,archive),
		 seek);
      return -1;}
    rv = archive_read_next_header(archive->kno_archive,&entry);}
  if (rv == ARCHIVE_OK) {}
  return 0;
  kno_seterr("ArchiveError","archive_find",
	     archive_errmsg(msgbuf,1000,archive),
	     seek);
  return -1;
}

#if 0
static int unparse_archive(struct U8_OUTPUT *out,lispval x)
{
  struct KNO_ARCHIVE *knoarchive = (struct KNO_ARCHIVE *)x;
  u8_string format = archive_format_name(knoarchive->kno_archive);
  int n_filters = archive_filter_count(knoarchive->kno_archive);
  u8_puts(out,"#<FileArchive ");
  if (knoarchive->archive_spec) {
    u8_puts(out,knoarchive->archive_spec);
    u8_puts(out," (");}
  int i =0; while (i<n_filters) {
    u8_string filter = archive_filter_name(knoarchive->kno_archive,i);
    if (i>0) u8_putc(out,'|');
    u8_puts(out,filter);
    i++;}
  u8_putc(out,'|');

  if (format) u8_puts(out,format);
  else u8_puts(out,"badformat");
  u8_printf(out,"#!0x%llx>",KNO_LONGVAL(knoarchive));
  return 1;
}
#endif

/* Archive input */

static int close_archive_input(struct U8_INPUT *raw_input)
{
  struct KNO_ARCHIVE_INPUT *in = (kno_archive_input) raw_input;
  if (in->archive_owned) archive_read_close(in->inport_archive);
  if (in->u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(in->u8_inbuf);
  if (in->u8_streaminfo&U8_STREAM_MALLOCD) u8_free(in);
  return 1;
}

static void compress_input(struct KNO_ARCHIVE_INPUT *input)
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
  struct KNO_ARCHIVE_INPUT *in = (kno_archive_input) raw_input;
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

static kno_port open_archive_input(struct archive *archive,
				   u8_string archive_id,
				   u8_string eltname,
				   struct archive_entry *entry)
{
  struct KNO_ARCHIVE_INPUT *in = u8_alloc(struct KNO_ARCHIVE_INPUT);
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
  else in->entry_info = kno_make_slotmap(5,0,NULL);
  struct KNO_PORT *port = u8_alloc(struct KNO_PORT);
  KNO_INIT_CONS(port,kno_ioport_type);
  port->port_input = (u8_input)in;
  port->port_output = NULL;
  port->port_id = u8_mkstring("%s..%s",archive_id,eltname);
  port->port_lisprefs = KNO_EMPTY;
  return port;
}

/* Top level functions */

KNO_DEFCPRIM("archive/open",open_archive,
	     KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
	     "Opens an archive file",
	     {"spec",kno_any_type,KNO_VOID},
	     {"path",kno_any_type,KNO_FALSE},
	     {"opts",kno_any_type,KNO_FALSE})
static lispval open_archive(lispval spec,lispval path,lispval opts)
{
  if ( (KNO_STRINGP(opts)) && (KNO_TABLEP(path)) ) {
    lispval swap = path; path=opts; opts=swap;}
  if (KNO_ABORTP(path)) return path;
  if ( (KNO_STRINGP(path)) || (KNO_UINTP(path)) || (KNO_TRUEP(path)) ) {
    lispval archive_ptr =
      (KNO_TYPEP(spec,kno_archive_type)) ? (kno_incref(spec)) :
      (new_archive(spec,opts));
    struct KNO_ARCHIVE *archive = (kno_archive) archive_ptr;
    struct archive_entry *entry;
    int rv = archive_seek(archive,path,&entry);
    if (rv<0) return KNO_ERROR_VALUE;
    else if (rv == 0) {
      if (kno_testopt(opts,KNOSYM_DROP,KNO_TRUE)) {
	u8_byte msgbuf[1000];
	kno_seterr("NotFound","open_archive",
		   archive_errmsg(msgbuf,1000,archive),
		   path);
	kno_decref(spec);
	return KNO_ERROR;}
      else return KNO_FALSE;}
    u8_byte buf[64];
    u8_string pathname =
      (KNO_STRINGP(path)) ? (KNO_CSTRING(path)) :
      (KNO_FIXNUMP(path)) ? (u8_sprintf(buf,64,"%d",KNO_FIX2INT(path))) :
      ((u8_string)"root");
    kno_port inport =
      open_archive_input(archive->kno_archive,
			 archive->archive_spec,
			 pathname,
			 entry);
    KNO_ADD_TO_CHOICE(inport->port_lisprefs,archive_ptr);
    return LISPVAL(inport);}
  else if ( (KNO_FALSEP(path)) || (KNO_VOIDP(path)) || (KNO_DEFAULTP(path)) )
    return new_archive(spec,opts);
  else return kno_err("BadArchivePath","open_archive",NULL,path);
}

/* Archive entries */

static void set_time_prop(lispval tbl,u8_string slotname,time_t t)
{
  if (t >= 0) {
    lispval v = kno_time2timestamp(t);
    kno_store(tbl,kno_intern(slotname),v);
    kno_decref(v);}
}

static void set_string_prop(lispval tbl,u8_string slotname,u8_string s)
{
  if (s) {
    lispval v = knostring(s);
    kno_store(tbl,kno_intern(slotname),v);
    kno_decref(v);}
}

static void set_int_prop(lispval tbl,u8_string slotname,long long ival)
{
  lispval v = KNO_INT2LISP(ival);
  kno_store(tbl,kno_intern(slotname),v);
  kno_decref(v);
}

static lispval entry_info(struct archive_entry *entry)
{
  lispval tbl = kno_make_slotmap(7,0,NULL);
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

KNO_DEFCPRIM("archive/find",archive_find,
	     KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	     "Get the next archive entry (possibly matching a "
	     "string or regex)",
	     {"obj",KNO_ARCHIVE_TYPE,KNO_VOID},
	     {"seek",kno_any_type,KNO_FALSE})
static lispval archive_find(lispval obj,lispval seek)
{
  struct KNO_ARCHIVE *archive = get_archive(obj);
  struct archive_entry *entry;
  if (! ( (KNO_VOIDP(seek)) || (KNO_FALSEP(seek)) || (KNO_TRUEP(seek)) ||
	  (KNO_STRINGP(seek)) || (KNO_TYPEP(seek,kno_regex_type)) ||
	  (KNO_UINTP(seek))) )
    return kno_err("InvalidArchivePathSpec","archive_find",NULL,seek);
  if (KNO_TRUEP(seek)) {
    if (KNO_VOIDP(archive->archive_cur))
      return KNO_FALSE;
    else return kno_incref(archive->archive_cur);}
  int rv = archive_seek(archive,seek,&entry);
  if (rv < 0)
    return KNO_ERROR_VALUE;
  else if (rv) {
    ssize_t header_pos = archive_read_header_position(archive->kno_archive);
    lispval info = entry_info(entry);
    set_int_prop(info,"POS",KNO_INT(header_pos));
    kno_decref(archive->archive_cur);
    archive->archive_cur = info;
    kno_incref(info);
    return info;}
  else return KNO_FALSE;
}

KNO_DEFCPRIM("archive/stat",archive_stat,
	     KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	     "Information about an open archive stream",
	     {"port",kno_ioport_type,KNO_VOID})
static lispval archive_stat(lispval port)
{
  lispval info;
  struct KNO_PORT *p = kno_consptr(struct KNO_PORT *,port,kno_ioport_type);
  struct KNO_ARCHIVE_INPUT *in = (kno_archive_input) (p->port_input);
  if (in->u8_closefn != close_archive_input)
    return kno_err("NotAnArchiveStream","archive_stat",NULL,port);
  else info = in->entry_info;

  lispval bytes_read = KNO_INT(in->bytes_read);
  kno_store(info,kno_intern("bytepos"),bytes_read);
  kno_decref(bytes_read);

  return kno_incref(info);
}

static lispval libarchive_module;

KNO_EXPORT int kno_init_libarchive()
{
  if (libarchive_initialized)
    return libarchive_initialized;
  else libarchive_initialized = u8_millitime();

  libarchive_module = kno_new_cmodule("libarchive",0,kno_init_libarchive);

  archive_typetag = kno_intern("LIBARCHIVE");

  link_local_cprims();

  kno_finish_cmodule(libarchive_module);

  u8_register_source_file(_FILEINFO);

  return libarchive_initialized;
}

static void link_local_cprims()
{
  KNO_LINK_CPRIM("archive/stat",archive_stat,1,libarchive_module);
  KNO_LINK_CPRIM("archive/open",open_archive,3,libarchive_module);
  KNO_LINK_CPRIM("archive/find",archive_find,2,libarchive_module);
}
