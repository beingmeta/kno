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

static lispval fixsyms_symbol, refs_symbol, xtrefs_typetag;

DEFPRIM4("read-xtype",read_xtype,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(1),
	 "(READ-XTYPE *stream* *opts/refs* [*off*] [*len*]) "
	 "reads the xtype representation store at *off* in "
	 "*stream*. If *off* is not provided, it reads from "
	 "the current position of the stream; if *len* is "
	 "provided, it is a maximum size of the xtype "
	 "representation and is used to prefetch bytes from "
	 "the file when possible.",
	 kno_stream_type,KNO_VOID,-1,KNO_VOID,
	 kno_fixnum_type,KNO_VOID,
	 kno_fixnum_type,KNO_VOID);
static lispval read_xtype(lispval stream,lispval opts_arg,
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

DEFPRIM5("write-xtype",write_xtype,KNO_MAX_ARGS(5)|KNO_MIN_ARGS(2),
	 "(WRITE-XTYPE *obj* *stream* [*opts/refs/compress*] [*pos*] [*max*]) "
	 "writes a xtype representation of *obj* to "
	 "*stream* at file position *pos* *(defaults to the "
	 "current file position of the stream). *max*, if "
	 "provided, is the maximum size of *obj*'s Xtype "
	 "representation. It is an error if the object has "
	 "a larger representation and the value may also be "
	 "used for allocating temporary buffers, etc.",
	 kno_any_type,KNO_VOID,kno_stream_type,KNO_VOID,
	 kno_any_type,KNO_FALSE,
	 kno_fixnum_type,KNO_VOID,
	 kno_fixnum_type,KNO_VOID);
static lispval write_xtype(lispval object,lispval stream,
			   lispval opts_arg,
			   lispval pos,
			   lispval max_bytes)
{
  struct KNO_STREAM *ds=
    kno_consptr(struct KNO_STREAM *,stream,kno_stream_type);
  kno_compress_type compress =
    kno_compression_type(opts_arg,KNO_NOCOMPRESS);
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
  if (! ( (KNO_VOIDP(pos)) ||
	  ( (KNO_INTEGERP(pos)) && (kno_numcompare(pos,KNO_FIXZERO) >= 0))) )
    return kno_err(kno_TypeError,"write_bytes","filepos",pos);
  long long byte_len = (KNO_VOIDP(max_bytes)) ? (-1) : (KNO_FIX2INT(max_bytes));
  unsigned char *bytes = NULL;
  ssize_t n_bytes = -1, init_len = (byte_len>0) ? (byte_len) : (1000);
  unsigned char *bytebuf =  u8_big_alloc(init_len);
  KNO_OUTBUF out = { 0 };
  KNO_INIT_OUTBUF(&out,bytebuf,init_len,KNO_BIGALLOC_BUFFER);
  n_bytes    = kno_write_xtype(&out,object,refs);
  bytebuf    = out.buffer;
  if (!(KNO_VOIDP(max_bytes))) {
    ssize_t max_len = kno_getint(max_bytes);
    if ( (out.bufwrite-out.buffer) > max_len) {
      kno_seterr("TooBig","write_xtype",
		 u8_mkstring("The object's Xtype representation was longer "
			     "than %lld bytes",max_len),
		 object);
      kno_close_outbuf(&out);
      return KNO_ERROR;}}
  if (KNO_VOIDP(pos)) {
    int rv = kno_write_bytes(kno_writebuf(ds),bytes,n_bytes);
    u8_big_free(bytes);
    if (rv<0)
      return KNO_ERROR;
    else return KNO_INT(n_bytes);}
  int rv = 0;
  kno_off_t filepos = kno_getint(pos);
#if HAVE_PREAD
  ssize_t to_write = n_bytes;
  unsigned char *point=bytes;
  while (to_write>0) {
    ssize_t delta = pwrite(ds->stream_fileno,point,to_write,filepos);
    if (delta>0) {
      to_write -= delta;
      point    += delta;
      filepos  += delta;}
    else if (delta<0) {rv=-1; break;}
    else break;}
#else
  rv = kno_lock_stream(ds);
  rv = kno_setpos(ds,filepos);
  if (rv>=0) rv = kno_write_bytes(kno_writebuf(ds),bytes,n_bytes);
  rv = kno_unlock_stream(ds);
#endif
  u8_big_free(bytes);
  if (rv<0)
    return KNO_ERROR;
  else {
    kno_flush_stream(ds);
    fsync(ds->stream_fileno);
    return KNO_INT(n_bytes);}
}

/* Reading and writing XTYPEs */

DEFPRIM3("xtype->file",xtype2file,
	 KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2)|KNO_NDOP,
	 "`(XTYPE->FILE *object* *filename* [*opts/refs*])` "
	 "**undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_FALSE);
static lispval xtype2file(lispval object,lispval filename,
			  lispval opts_arg)
{
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
  if (STRINGP(filename)) {
    u8_string temp_name = u8_mkstring("%s.part",CSTRING(filename));
    struct KNO_STREAM *out = kno_open_file(temp_name,KNO_FILE_CREATE);
    struct KNO_OUTBUF *outstream = NULL;
    ssize_t bytes;
    if (out == NULL) return KNO_ERROR;
    else outstream = kno_writebuf(out);
    bytes = kno_write_xtype(outstream,object,refs);
    if (bytes<0) {
      kno_free_stream(out);
      u8_free(temp_name);
      return KNO_ERROR;}
    kno_free_stream(out);
    int rv = u8_movefile(temp_name,CSTRING(filename));
    if (rv<0) {
      u8_log(LOG_WARN,"MoveFailed",
	     "Couldn't move the completed file into %s, leaving in %s",
	     CSTRING(filename),temp_name);
      u8_seterr("MoveFailed","lisp2file",u8_strdup(CSTRING(filename)));
      u8_free(temp_name);
      return KNO_ERROR_VALUE;}
    else {
      u8_free(temp_name);
      return KNO_INT(bytes);}}
  else if (TYPEP(filename,kno_stream_type)) {
    KNO_DECL_OUTBUF(tmp,KNO_DTWRITE_SIZE);
    struct KNO_STREAM *stream=
      kno_consptr(struct KNO_STREAM *,filename,kno_stream_type);
    ssize_t bytes = kno_write_xtype(&tmp,object,refs);
    if (bytes<0) {
      kno_close_outbuf(&tmp);
      return KNO_ERROR;}
    else {
      bytes=kno_stream_write(stream,bytes,tmp.buffer);
      kno_close_outbuf(&tmp);
      if (bytes<0)
	return KNO_ERROR;
      else return KNO_INT(bytes);}}
  else return kno_type_error(_("string"),"xtype2file",filename);
}

#define flushp(b) ( (((b).buflim)-((b).bufwrite)) < ((b).buflen)/5 )

static ssize_t write_xtypes(lispval xtypes,struct KNO_STREAM *out,
			    xtype_refs refs)
{
  ssize_t bytes=0, rv=0;
  kno_off_t start= kno_endpos(out);
  if ( start != out->stream_maxpos ) start=-1;
  struct KNO_OUTBUF tmp = { 0 };
  unsigned char tmpbuf[1000];
  KNO_INIT_BYTE_OUTBUF(&tmp,tmpbuf,1000);
  if (CHOICEP(xtypes)) {
    /* This writes out the objects sequentially, writing into memory
       first and then to disk, to reduce the danger of malformed
       XTYPEs on the disk. */
    DO_CHOICES(xtype,xtypes) {
      ssize_t write_size=0;
      if ( (flushp(tmp)) ) {
	write_size=kno_stream_write(out,tmp.bufwrite-tmp.buffer,tmp.buffer);
	tmp.bufwrite=tmp.buffer;}
      if (write_size>=0) {
	ssize_t xtype_size=kno_write_xtype(&tmp,xtype,refs);
	if (xtype_size<0)
	  write_size=xtype_size;
	else if (flushp(tmp)) {
	  write_size=kno_stream_write(out,tmp.bufwrite-tmp.buffer,tmp.buffer);
	  tmp.bufwrite=tmp.buffer;}
	else write_size=xtype_size;}
      if (write_size<0) {
	rv=write_size;
	KNO_STOP_DO_CHOICES;
	break;}
      else bytes=bytes+write_size;}
    if (tmp.bufwrite > tmp.buffer) {
      ssize_t written = kno_stream_write
	(out,tmp.bufwrite-tmp.buffer,tmp.buffer);
      tmp.bufwrite=tmp.buffer;
      bytes += written;}}
  else {
    bytes=kno_write_xtype(&tmp,xtypes,refs);
    if (bytes>0)
      rv=kno_stream_write (out,tmp.bufwrite-tmp.buffer,tmp.buffer);}
  kno_close_outbuf(&tmp);
  if (rv<0) {
    if (start>=0) {
      int rv = ftruncate(out->stream_fileno,start);
      if (rv<0) {
	int got_err = errno; errno=0;
	u8_log(LOG_WARN,"TruncateFailed",
	       "Couldn't undo write to %s (errno=%d:%s)",
	       out->streamid,got_err,u8_strerror(got_err));}}
    return rv;}
  else return bytes;
}

DEFPRIM3("xtypes->file+",add_xtypes2file,
	 KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2)|KNO_NDOP,
	 "`(XTYPES->FILE+ *xtypes* *filename* [*opts/refs*])"
	 " **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_FALSE);
static lispval add_xtypes2file(lispval object,lispval filename,
			       lispval opts_arg)
{
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
  if (STRINGP(filename)) {
    struct KNO_STREAM *stream;
    if (u8_file_existsp(CSTRING(filename)))
      stream = kno_open_file(CSTRING(filename),KNO_FILE_MODIFY);
    else stream = kno_open_file(CSTRING(filename),KNO_FILE_CREATE);
    if (stream == NULL)
      return KNO_ERROR;
    ssize_t bytes = write_xtypes(object,stream,refs);
    kno_close_stream(stream,KNO_STREAM_CLOSE_FULL);
    u8_free(stream);
    if (bytes<0)
      return KNO_ERROR;
    else return KNO_INT(bytes);}
  else if (TYPEP(filename,kno_stream_type)) {
    struct KNO_STREAM *stream=
      kno_consptr(struct KNO_STREAM *,filename,kno_stream_type);
    ssize_t bytes=write_xtypes(object,stream,refs);
    if (bytes<0)
      return KNO_ERROR;
    else return KNO_INT(bytes);}
  else return kno_type_error(_("string"),"add_xtypes2file",filename);
}

DEFPRIM2("file->xtype",file2xtype,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(FILE->XTYPE *filename* [*opts/refs*])`"
	 "**undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_FALSE);
static lispval file2xtype(lispval filename,lispval opts_arg)
{
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
  if (STRINGP(filename)) {
    struct KNO_STREAM _in, *in =
      kno_init_file_stream(&_in,CSTRING(filename),KNO_FILE_READ,
			   ( (KNO_USE_MMAP) ? (KNO_STREAM_MMAPPED) : (0)),
			   ( (KNO_USE_MMAP) ? (0) : (kno_filestream_bufsize)));
    if (in == NULL)
      return KNO_ERROR;
    else {
      kno_inbuf inbuf = kno_readbuf(in);
      lispval object = kno_read_xtype(inbuf,refs);
      kno_close_stream(in,KNO_STREAM_FREEDATA);
      return object;}}
  else if (TYPEP(filename,kno_stream_type)) {
    struct KNO_STREAM *in=
      kno_consptr(struct KNO_STREAM *,filename,kno_stream_type);
    lispval object = kno_read_xtype(kno_readbuf(in),refs);
    if (object == KNO_EOD) return KNO_EOF;
    else return object;}
  else return kno_type_error(_("string"),"read_xtype",filename);
}

DEFPRIM2("file->xtypes",file2xtypes,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(FILE->XTYPES *arg0* [*opts/refs*])`"
	 "**undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_FALSE);
static lispval file2xtypes(lispval filename,lispval opts_arg)
{
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
  if (STRINGP(filename)) {
    struct KNO_STREAM _in, *in =
      kno_init_file_stream(&_in,CSTRING(filename),KNO_FILE_READ,
			   ( (KNO_USE_MMAP) ? (KNO_STREAM_MMAPPED) : (0)),
			   ( (KNO_USE_MMAP) ? (0) : (kno_filestream_bufsize)));
    lispval results = EMPTY, object = VOID;
    if (in == NULL)
      return KNO_ERROR;
    else {
      kno_inbuf inbuf = kno_readbuf(in);
      object = kno_read_xtype(inbuf,refs);
      while (!(KNO_EODP(object))) {
	if (KNO_ABORTP(object)) {
	  kno_decref(results);
	  kno_close_stream(in,KNO_STREAM_FREEDATA);
	  return object;}
	CHOICE_ADD(results,object);
	object = kno_read_xtype(inbuf,refs);}
      kno_close_stream(in,KNO_STREAM_FREEDATA);
      return results;}}
  else return kno_type_error(_("string"),"file2xtypes",filename);
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

  link_local_cprims();

  kno_finish_module(xtypeprims_module);
}

static void link_local_cprims()
{
  lispval scheme_module = kno_scheme_module;

  KNO_LINK_PRIM("file->xtypes",file2xtypes,2,xtypeprims_module);
  KNO_LINK_PRIM("file->xtype",file2xtype,2,xtypeprims_module);
  KNO_LINK_PRIM("xtypes->file+",add_xtypes2file,3,xtypeprims_module);
  KNO_LINK_PRIM("xtype->file",xtype2file,3,xtypeprims_module);
  KNO_LINK_PRIM("write-xtype",write_xtype,5,kno_scheme_module);
  KNO_LINK_PRIM("read-xtype",read_xtype,4,kno_scheme_module);

  KNO_LINK_ALIAS("xtype->file+",add_xtypes2file,xtypeprims_module);
}
