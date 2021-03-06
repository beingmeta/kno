/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

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
#include "kno/getsource.h"
#include "kno/cprims.h"

#include <libu8/u8streamio.h>
#include <libu8/u8crypto.h>
#include <libu8/u8filefns.h>

#include <zlib.h>

#ifndef KNO_DTWRITE_SIZE
#define KNO_DTWRITE_SIZE 10000
#endif

static lispval refs_symbol, append_symbol, embed_symbol;

static u8_string get_filedata(u8_string path,ssize_t *lenp)
{
  if (u8_file_existsp(path))
    return u8_filedata(path,lenp);
  else {
    const unsigned char *data = kno_get_source(path,"bytes",NULL,NULL,lenp);
    if (data == NULL) kno_seterr3(kno_FileNotFound,"filestring",path);
    return data;}
}


DEFC_PRIM("write-xtype",write_xtype_prim,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "(WRITE-XTYPE *obj* *stream* [*opts*]) "
	  "writes a xtype representation of *obj* to "
	  "*stream* at file position *pos* *(defaults to the "
	  "current file position of the stream). *max*, if "
	  "provided, is the maximum size of *obj*'s Xtype "
	  "representation. It is an error if the object has "
	  "a larger representation and the value may also be "
	  "used for allocating temporary buffers, etc.",
	  {"object",kno_any_type,KNO_VOID},
	  {"dest",kno_any_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_FALSE})
static lispval write_xtype_prim(lispval object,lispval dest,lispval opts)
{
  if (KNO_CHOICEP(dest)) {
    lispval result = KNO_EMPTY;
    KNO_DO_CHOICES(d,dest) {
      lispval r = write_xtype_prim(object,d,opts);
      if (KNO_ABORTED(r)) {
	kno_decref(result);
	return r;}
      else {KNO_ADD_TO_CHOICE(result,r);}}
    return result;}
  lispval refs_arg = kno_getxrefs(opts);
  struct XTYPE_REFS *refs = (KNO_RAW_TYPEP(refs_arg,kno_xtrefs_typetag)) ?
    ( KNO_RAWPTR_VALUE(refs_arg)) :
    (NULL);

  kno_compress_type compress = kno_compression_type(opts,KNO_NOCOMPRESS);

  int append = kno_testopt(opts,append_symbol,KNO_VOID), close_stream = 0;
  int embed = kno_testopt(opts,embed_symbol,KNO_VOID);

  u8_string filename = (KNO_STRINGP(dest)) ? (KNO_CSTRING(dest)) : (NULL);
  u8_string tmpfile = ( (filename) && (append==0) ) ?
    (u8_mkstring("%s.part",filename)) :
    (NULL);
  struct KNO_STREAM *out = NULL;
  struct KNO_OUTBUF _outbuf, *outbuf = NULL;

  if (tmpfile) {
    out = kno_open_file(tmpfile,KNO_FILE_CREATE);
    if (out == NULL) {
      u8_free(tmpfile);
      kno_decref(refs_arg);
      return KNO_ERROR;}
    outbuf=kno_writebuf(out);
    close_stream=1;}
  else if (filename) {
    int flags = (u8_file_existsp(filename)) ?
      (KNO_FILE_MODIFY) : (KNO_FILE_CREATE);
    out = kno_open_file(filename,flags);
    if (out == NULL) {
      kno_decref(refs_arg);
      return KNO_ERROR;}
    outbuf=kno_writebuf(out);
    close_stream = 1;
    if (append) kno_endpos(out);}
  else if (KNO_TYPEP(dest,kno_stream_type)) {
    out = (kno_stream) dest;
    if (append) kno_endpos(out);
    outbuf=kno_writebuf(out);}
  else if (KNO_FALSEP(dest)) {
    /* Write to packet */
    KNO_INIT_BYTE_OUTPUT(&_outbuf,2000);
    outbuf=&_outbuf;}
  else {
    kno_decref(refs_arg);
    return kno_err("NotStreamOrFilename","write_xtype",NULL,dest);}

  ssize_t rv = kno_compress_xtype(outbuf,object,refs,compress,embed);
  kno_decref(refs_arg);
  if (rv < 0) {
    if (tmpfile) u8_free(tmpfile);
    if (close_stream) kno_free_stream(out);
    else if (KNO_FALSEP(dest))
      kno_close_outbuf(&_outbuf);
    else NO_ELSE;
    return KNO_ERROR;}
  if (KNO_FALSEP(dest))
    return kno_init_packet(NULL,rv,_outbuf.buffer);
  if (close_stream) kno_free_stream(out);
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

DEFC_PRIM("read-xtype",read_xtype_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "reads xtype representations from *source*, which "
	  "can be a filename or a binary stream. The *opts* "
	  "arg can specify `xrefs` to indicate xrefs to use "
	  "when reading, or `count` to indicate the number "
	  "of objects to read. By default, all "
	  "representations until the end of data are read "
	  "from the source.",
	  {"source",kno_any_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_FALSE})
static lispval read_xtype_prim(lispval source,lispval opts)
{
  lispval refs_arg = kno_getxrefs(opts);
  struct XTYPE_REFS *refs = (KNO_RAW_TYPEP(refs_arg,kno_xtrefs_typetag)) ?
    ( KNO_RAWPTR_VALUE(refs_arg)) :
    (NULL);

  long long count = kno_getfixopt(opts,"count",-1);
  struct KNO_STREAM _in, *in;
  int close_stream = 0;

  if (KNO_PACKETP(source)) {
    struct KNO_STRING *packet = (kno_string) source;
    struct KNO_INBUF _in, *in = &_in;
    KNO_INIT_BYTE_INPUT(&_in,packet->str_bytes,packet->str_bytelen);
    lispval object = kno_read_xtype(in,refs);
    return object;}

  if (KNO_STRINGP(source)) {
    u8_string sourcepath = KNO_CSTRING(source);
    if (u8_file_existsp(sourcepath)) {
      in=kno_init_file_stream(&_in,sourcepath,KNO_FILE_READ,
			      ( (KNO_USE_MMAP) ? (KNO_STREAM_MMAPPED) : (0)),
			      ( (KNO_USE_MMAP) ? (0) : (kno_filestream_bufsize)));
      if (in == NULL) {
	kno_decref(refs_arg);
	return KNO_ERROR;}
      else close_stream=1;}
    else {
      ssize_t len = -1;
      const unsigned char *filedata = get_filedata(sourcepath,&len);
      if (filedata == NULL) {
	kno_seterr3(kno_FileNotFound,"open_stream",sourcepath);
	return KNO_ERROR;}
      in=kno_init_byte_stream(&_in,sourcepath,KNO_FILE_READ,len,filedata);
      close_stream=1;}}
  else if (TYPEP(source,kno_stream_type))
    in = (kno_stream) source;
  else {
    kno_decref(refs_arg);
    return kno_err("NotAFileOrStream","read-xtypes",NULL,source);}

  long long i = 0;
  kno_inbuf inbuf = kno_readbuf(in);
  lispval object = kno_read_xtype(inbuf,refs), results = KNO_EMPTY;
  while ( (!(KNO_EODP(object))) && ( (count<0) || (i<count) ) ) {
    if (KNO_ABORTP(object)) {
      kno_decref(results);
      kno_decref(refs_arg);
      if (close_stream) kno_close_stream(in,KNO_STREAM_FREEDATA);
      return object;}
    CHOICE_ADD(results,object);
    object = kno_read_xtype(inbuf,refs);
    i++;}
  kno_decref(refs_arg);
  if (close_stream) kno_close_stream(in,KNO_STREAM_FREEDATA);
  return results;
}

DEFC_PRIM("read-xtype-at",read_xtype_at_prim,
	  KNO_MAX_ARGS(4)|KNO_MIN_ARGS(1),
	  "(READ-XTYPE-AT *stream* [*opts*] [*off*] [*len*]) "
	  "reads the xtype representation store at *off* in "
	  "*stream*. If *off* is not provided, it reads from "
	  "the current position of the stream; if *len* is "
	  "provided, it is a maximum size of the xtype "
	  "representation and is used to prefetch bytes from "
	  "the file when possible.",
	  {"stream",kno_stream_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_VOID},
	  {"pos",kno_fixnum_type,KNO_VOID},
	  {"len",kno_fixnum_type,KNO_VOID})
static lispval read_xtype_at_prim(lispval stream,lispval opts,
				  lispval pos,lispval len)
{
  struct KNO_STREAM *ds=
    kno_consptr(struct KNO_STREAM *,stream,kno_stream_type);
  lispval refs_arg = kno_getxrefs(opts);
  struct XTYPE_REFS *refs = (KNO_RAW_TYPEP(refs_arg,kno_xtrefs_typetag)) ?
    ( KNO_RAWPTR_VALUE(refs_arg)) :
    (NULL);
  if (KNO_VOIDP(pos)) {
    lispval object = kno_read_xtype(kno_readbuf(ds),refs);
    kno_decref(refs_arg);
    if (object == KNO_EOD) {
      kno_decref(refs_arg);
      return object;}
    else return object;}
  else if (KNO_VOIDP(len)) {
    long long filepos = KNO_FIX2INT(pos);
    if (filepos<0) {
      kno_decref(refs_arg);
      return kno_type_error("file position","read_type",pos);}
    kno_lock_stream(ds);
    kno_setpos(ds,filepos);
    lispval object = kno_read_xtype(kno_readbuf(ds),refs);
    kno_decref(refs_arg);
    kno_unlock_stream(ds);
    return object;}
  else {
    long long off     = KNO_FIX2INT(pos);
    ssize_t   n_bytes = KNO_FIX2INT(len);
    struct KNO_INBUF _inbuf,
      *in = kno_open_block(ds,&_inbuf,off,n_bytes,0);
    lispval object = kno_read_xtype(in,refs);
    kno_decref(refs_arg);
    kno_close_inbuf(in);
    return object;}
}

DEFC_PRIM("write-xtype-at",write_xtype_at_prim,
	  KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "(WRITE-XTYPE-AT *object* *stream* [*off*] [*opts/refs*]) "
	  "reads the xtype representation store at *off* in "
	  "*stream*. If *off* is not provided, it reads from "
	  "the current position of the stream; if *len* is "
	  "provided, it is a maximum size of the xtype "
	  "representation and is used to prefetch bytes from "
	  "the file when possible.",
	  {"object",kno_any_type,KNO_VOID},
	  {"stream",kno_stream_type,KNO_VOID},
	  {"pos",kno_any_type,KNO_FALSE},
	  {"opts",kno_fixnum_type,KNO_VOID})
static lispval write_xtype_at_prim(lispval object,lispval stream,lispval pos,
				   lispval opts)
{
  struct KNO_STREAM *ds=
    kno_consptr(struct KNO_STREAM *,stream,kno_stream_type);
  lispval refs_arg = kno_getxrefs(opts);
  struct XTYPE_REFS *refs = (KNO_RAW_TYPEP(refs_arg,kno_xtrefs_typetag)) ?
    ( KNO_RAWPTR_VALUE(refs_arg)) :
    (NULL);
  int embed = kno_testopt(opts,embed_symbol,KNO_VOID);
  kno_compress_type compress = kno_compression_type(opts,KNO_NOCOMPRESS);
  ssize_t len = -1;
#if HAVE_PREAD
  struct KNO_OUTBUF tmpout = { 0 };
  kno_off_t filepos = (KNO_FIXNUMP(pos)) ? (kno_getint(pos)) : (kno_getpos(ds));
  if (filepos<0) {
    kno_decref(refs_arg);
    return kno_err("BadFilePos","write_xtype_at_prim",ds->streamid,pos);}
  KNO_INIT_BYTE_OUTPUT(&tmpout,8000);
  len = kno_compress_xtype(&tmpout,object,refs,compress,embed);
  if (len>0) {
    ssize_t to_write = len; int retries = 0;
    unsigned char *point=tmpout.buffer;
    while (to_write>0) {
      ssize_t delta = pwrite(ds->stream_fileno,point,to_write,filepos);
      if (delta>0) {
	to_write -= delta;
	point    += delta;
	filepos  += delta;
	retries=0;}
      else if (delta<0) {len=-1; break;}
      else if (retries>7) {
	kno_seterr("WriteFailed","write_xtype_at_prim",ds->streamid,VOID);
	len=-1;
	break;}
      else {
	retries++;
	u8_sleep(0.005*retries);}}}
  kno_close_outbuf(&tmpout);
#else
  if (KNO_FIXNUMP(pos)) {
    kno_off_t goto_pos = kno_getint(pos);
    int rv = kno_setpos(ds,goto_pos);
    if (rv<0) return KNO_ERROR;}
  len = kno_compress_xtype(kno_writebuf(ds),object,refs,compress);
#endif
  kno_decref(refs_arg);
  if (len<0)
    return KNO_ERROR;
  else return KNO_INT(len);
}

/* File initializations */

static int scheme_xtypeprims_initialized = 0;

KNO_EXPORT void kno_init_xtypeprims_c()
{
  if (scheme_xtypeprims_initialized) return;
  scheme_xtypeprims_initialized = 1;
  kno_init_scheme();
  kno_init_drivers();
  u8_register_source_file(_FILEINFO);
  refs_symbol = kno_intern("XREFS");
  append_symbol = kno_intern("append");
  embed_symbol = kno_intern("embed");

  link_local_cprims();
}

static void link_local_cprims()
{
  KNO_LINK_CPRIM("write-xtype",write_xtype_prim,3,kno_binio_module);
  KNO_LINK_CPRIM("read-xtype",read_xtype_prim,2,kno_binio_module);
  KNO_LINK_CPRIM("write-xtype-at",write_xtype_at_prim,4,kno_binio_module);
  KNO_LINK_CPRIM("read-xtype-at",read_xtype_at_prim,4,kno_binio_module);
}
