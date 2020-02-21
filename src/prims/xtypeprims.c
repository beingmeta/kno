/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

/* #define KNO_INLINE_EVAL 1 */

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/numbers.h"
#include "kno/eval.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/frames.h"
#include "kno/streams.h"
#include "kno/xtypes.h"
#include "kno/ports.h"
#include "kno/cprims.h"

#include <libu8/u8streamio.h>
#include <libu8/u8crypto.h>
#include <libu8/u8filefns.h>

#include <zlib.h>

#ifndef KNO_DTWRITE_SIZE
#define KNO_DTWRITE_SIZE 10000
#endif

static lispval fixsyms_symbol, refs_symbol, xtrefs_typetag, append_symbol;

DEFPRIM3("write-xtype",write_xtype_prim,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	 "(WRITE-XTYPE *obj* *stream* [*opts*]) "
	 "writes a xtype representation of *obj* to "
	 "*stream* at file position *pos* *(defaults to the "
	 "current file position of the stream). *max*, if "
	 "provided, is the maximum size of *obj*'s Xtype "
	 "representation. It is an error if the object has "
	 "a larger representation and the value may also be "
	 "used for allocating temporary buffers, etc.",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_FALSE);

static lispval write_xtype_prim(lispval object,lispval dest,lispval opts)
{
  struct XTYPE_REFS *refs = NULL;
  lispval refs_arg = kno_getopt(opts,refs_symbol,KNO_VOID);
  if (KNO_RAW_TYPEP(refs_arg,xtrefs_typetag))
    refs = KNO_RAWPTR_VALUE(refs_arg);
  else NO_ELSE;
  kno_decref(refs_arg);

  kno_compress_type compress = kno_compression_type(opts,KNO_NOCOMPRESS);

  int append = kno_testopt(opts,append_symbol,KNO_VOID);

  u8_string filename = (KNO_STRINGP(dest)) ? (KNO_CSTRING(dest)) : (NULL);
  u8_string tmpfile = ( (filename) && (append==0) ) ? 
    (u8_mkstring("%s.part",filename)) : 
    (NULL);
  struct KNO_STREAM *out = NULL;
  
  if (tmpfile) {
    out = kno_open_file(tmpfile,KNO_FILE_CREATE);
    if (out == NULL) {
      u8_free(tmpfile);
      return KNO_ERROR;}}
  else if (filename) {
    int flags = (u8_file_existsp(filename)) ? (KNO_FILE_MODIFY) : (KNO_FILE_CREATE);
    out = kno_open_file(filename,flags);
    if (out == NULL) return KNO_ERROR;
    if (append) kno_endpos(out);}
  else if (KNO_TYPEP(dest,kno_stream_type))
    out = (kno_stream) dest;
  else return kno_err("NotStreamOrFilename","write_xtype",NULL,dest);

  kno_outbuf outbuf = kno_writebuf(out);
  ssize_t rv = kno_compress_xtype(outbuf,object,refs,compress);
  if (rv < 0) {
    if (tmpfile) u8_free(tmpfile);
    if (filename) kno_free_stream(out);
    return KNO_ERROR;}
  if (filename) kno_free_stream(out);
  if (tmpfile) {
    int move_result = u8_movefile(tmpfile,filename);
    if (move_result<0) {
      u8_log(LOG_WARN,"MoveFailed",
	     "Couldn't move the completed file into %s, leaving in %s",
	     filename,tmpfile);
      u8_seterr("MoveFailed","lisp2file",u8_strdup(filename));
      u8_free(tmpfile);
      return KNO_ERROR_VALUE;}
    else u8_free(tmpfile);}
  return KNO_INT(rv);
}

DEFPRIM2("read-xtype",read_xtype_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(read-xtype *source* [*opts*])` "
	 "reads xtype representations from *source*, which can be a filename "
	 "or a binary stream. The *opts* arg can specify `xrefs` to indicate "
	 "xrefs to use when reading, or `count` to indicate the number of "
	 "objects to read. By default, all representations until the end of data "
	 "are read from the source.",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_FALSE);
static lispval read_xtype_prim(lispval source,lispval opts)
{
  struct XTYPE_REFS *refs = NULL;
  lispval refs_arg = kno_getopt(opts,refs_symbol,KNO_VOID);
  if (KNO_RAW_TYPEP(refs_arg,xtrefs_typetag))
    refs = KNO_RAWPTR_VALUE(refs_arg);
  else NO_ELSE;
  long long count = kno_getfixopt(opts,"count",-1);
  struct KNO_STREAM _in, *in;
  int close_stream = 0;

  if (KNO_STRINGP(source)) {
    in=kno_init_file_stream(&_in,CSTRING(source),KNO_FILE_READ,
			   ( (KNO_USE_MMAP) ? (KNO_STREAM_MMAPPED) : (0)),
			    ( (KNO_USE_MMAP) ? (0) : (kno_filestream_bufsize)));
    if (in == NULL) return KNO_ERROR; else close_stream=1;}
  else if (TYPEP(source,kno_stream_type))
    in = (kno_stream) source;
  else return kno_err("NotAFileOrStream","read-xtypes",NULL,source);

  long long i = 0;
  kno_inbuf inbuf = kno_readbuf(in);
  lispval object = kno_read_xtype(inbuf,refs), results = KNO_EMPTY;
  while ( (!(KNO_EODP(object))) && ( (count<0) || (i<count) ) ) {
    if (KNO_ABORTP(object)) {
      kno_decref(results);
      if (close_stream) kno_close_stream(in,KNO_STREAM_FREEDATA);
      return object;}
    CHOICE_ADD(results,object);
    object = kno_read_xtype(inbuf,refs);
    i++;}
  if (close_stream) kno_close_stream(in,KNO_STREAM_FREEDATA);
  return results;
}

DEFPRIM4("read-xtype-at",read_xtype_at_prim,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(1),
	 "(READ-XTYPE-AT *stream* [*opts*] [*off*] [*len*]) "
	 "reads the xtype representation store at *off* in "
	 "*stream*. If *off* is not provided, it reads from "
	 "the current position of the stream; if *len* is "
	 "provided, it is a maximum size of the xtype "
	 "representation and is used to prefetch bytes from "
	 "the file when possible.",
	 kno_stream_type,KNO_VOID,-1,KNO_VOID,
	 kno_fixnum_type,KNO_VOID,
	 kno_fixnum_type,KNO_VOID);
static lispval read_xtype_at_prim(lispval stream,lispval opts_arg,
				  lispval pos,lispval len)
{
  struct KNO_STREAM *ds=
    kno_consptr(struct KNO_STREAM *,stream,kno_stream_type);
  struct XTYPE_REFS *refs = NULL;
  if (KNO_RAW_TYPEP(opts_arg,xtrefs_typetag))
    refs = KNO_RAWPTR_VALUE(opts_arg);
  else if (TABLEP(opts_arg)) {
    lispval refs_opt = kno_getopt(opts_arg,refs_symbol,KNO_VOID);
    if (KNO_RAW_TYPEP(refs_opt,xtrefs_typetag))
      refs = KNO_RAWPTR_VALUE(opts_arg);
    /* We assume it won't be freed while we still hold onto the
       opts object. */
    kno_decref(refs_opt);}
  if (KNO_VOIDP(pos)) {
    lispval object = kno_read_xtype(kno_readbuf(ds),refs);
    if (object == KNO_EOD)
      return KNO_EOF;
    else return object;}
  else if (KNO_VOIDP(len)) {
    long long filepos = KNO_FIX2INT(pos);
    if (filepos<0)
      return kno_type_error("file position","read_type",pos);
    kno_lock_stream(ds);
    kno_setpos(ds,filepos);
    lispval object = kno_read_xtype(kno_readbuf(ds),refs);
    kno_unlock_stream(ds);
    return object;}
  else {
    long long off     = KNO_FIX2INT(pos);
    ssize_t   n_bytes = KNO_FIX2INT(len);
    struct KNO_INBUF _inbuf,
      *in = kno_open_block(ds,&_inbuf,off,n_bytes,0);
    lispval object = kno_read_xtype(in,refs);
    kno_close_inbuf(in);
    return object;}
}

DEFPRIM4("write-xtype-at",write_xtype_at_prim,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
	 "(WRITE-XTYPE-AT *object* *stream* [*off*] [*opts/refs*]) "
	 "reads the xtype representation store at *off* in "
	 "*stream*. If *off* is not provided, it reads from "
	 "the current position of the stream; if *len* is "
	 "provided, it is a maximum size of the xtype "
	 "representation and is used to prefetch bytes from "
	 "the file when possible.",
	 kno_any_type,KNO_VOID,kno_stream_type,KNO_VOID,
	 kno_any_type,KNO_FALSE,kno_fixnum_type,KNO_VOID);
static lispval write_xtype_at_prim(lispval object,lispval stream,lispval pos,
				   lispval opts_arg)
{
  struct KNO_STREAM *ds=
    kno_consptr(struct KNO_STREAM *,stream,kno_stream_type);
  struct XTYPE_REFS *refs = NULL;
  lispval opts = KNO_FALSE;
  if (KNO_RAW_TYPEP(opts_arg,xtrefs_typetag))
    refs = KNO_RAWPTR_VALUE(opts_arg);
  else if (TABLEP(opts_arg)) {
    lispval refs_opt = kno_getopt(opts_arg,refs_symbol,KNO_VOID);
    if (KNO_RAW_TYPEP(refs_opt,xtrefs_typetag))
      refs = KNO_RAWPTR_VALUE(opts_arg);
    /* We assume it won't be freed while we still hold onto the
       opts object. */
    kno_decref(refs_opt);
    opts = opts_arg;}
  kno_compress_type compress = kno_compression_type(opts,KNO_NOCOMPRESS);
  if (KNO_FIXNUMP(pos)) {
    kno_off_t goto_pos = kno_getint(pos);
    int rv = kno_setpos(ds,goto_pos);
    if (rv<0) return KNO_ERROR;}
  ssize_t len = kno_compress_xtype(kno_writebuf(ds),object,refs,compress);
  if (len<0)
    return KNO_ERROR;
  else return KNO_INT(len);
}

static int scheme_xtypeprims_initialized = 0;

lispval xtypeprims_module;

KNO_EXPORT void kno_init_xtypeprims_c()
{
  if (scheme_xtypeprims_initialized) return;
  scheme_xtypeprims_initialized = 1;
  kno_init_scheme();
  kno_init_drivers();
  xtypeprims_module =
    kno_new_cmodule("xtypeprims",(KNO_MODULE_DEFAULT),kno_init_xtypeprims_c);
  u8_register_source_file(_FILEINFO);
  fixsyms_symbol = kno_intern("LOUDSYMS");
  refs_symbol = kno_intern("XREFS");
  xtrefs_typetag = kno_intern("%xtype-refs");
  append_symbol = kno_intern("append");

  link_local_cprims();

  kno_finish_module(xtypeprims_module);
}

static void link_local_cprims()
{
  KNO_LINK_PRIM("write-xtype",write_xtype_prim,3,kno_scheme_module);
  KNO_LINK_PRIM("read-xtype",read_xtype_prim,2,xtypeprims_module);
  KNO_LINK_PRIM("write-xtype-at",write_xtype_at_prim,4,kno_scheme_module);
  KNO_LINK_PRIM("read-xtype-at",read_xtype_at_prim,4,kno_scheme_module);
}
