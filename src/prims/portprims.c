/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
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
#include "kno/streams.h"
#include "kno/zipsource.h"
#include "kno/dtypeio.h"
#include "kno/xtypes.h"
#include "kno/ports.h"
#include "kno/pprint.h"
#include "kno/history.h"
#include "kno/cprims.h"

#include <libu8/u8streamio.h>
#include <libu8/u8crypto.h>

#include <zlib.h>

#define KNO_ZIPSOURCE_TYPE 0x7fa3dc

KNO_EXPORT kno_compress_type kno_compression_type(lispval,kno_compress_type);

static lispval refs_symbol, nrefs_symbol, lookup_symbol, embed_symbol;

u8_condition kno_UnknownEncoding=_("Unknown encoding");

static long long getposfixopt(lispval opts,lispval sym,long long dflt)
{
  lispval intval = kno_getopt(opts,sym,KNO_VOID);
  if (KNO_UINTP(intval))
    return (long long) (KNO_FIX2INT(intval));
  kno_decref(intval);
  return dflt;
}

DEF_KNOSYM(addsyms);
DEF_KNOSYM(addoids);

#define printout_eval(x,env) kno_eval((x),(env),kno_stackptr)

/* Making ports */

KNO_EXPORT lispval kno_make_port(U8_INPUT *in,U8_OUTPUT *out,u8_string id)
{
  struct KNO_PORT *port = u8_alloc(struct KNO_PORT);
  KNO_INIT_CONS(port,kno_ioport_type);
  port->annotations = KNO_EMPTY;
  port->port_input = in;
  port->port_output = out;
  port->port_id = id;
  port->port_lisprefs = KNO_EMPTY;
  return LISP_CONS(port);
}

static u8_output get_output_port(lispval portarg)
{
  if ((VOIDP(portarg))||(KNO_TRUEP(portarg)))
    return u8_current_output;
  else if (KNO_PORTP(portarg)) {
    struct KNO_PORT *p=
      kno_consptr(struct KNO_PORT *,portarg,kno_ioport_type);
    return p->port_output;}
  else return NULL;
}

static u8_input get_input_port(lispval portarg)
{
  if ( (VOIDP(portarg)) || (KNO_TRUEP(portarg)) )
    return u8_current_input;
  else if (KNO_PORTP(portarg)) {
    struct KNO_PORT *p=
      kno_consptr(struct KNO_PORT *,portarg,kno_ioport_type);
    return p->port_input;}
  else return NULL;
}

DEFC_PRIM("port?",portp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns #t if *object* is an i/o port.",
	  {"arg",kno_any_type,KNO_VOID})
static lispval portp(lispval arg)
{
  if (KNO_PORTP(arg))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("input-port?",input_portp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns #t if *object* is an input port.",
	  {"arg",kno_any_type,KNO_VOID})
static lispval input_portp(lispval arg)
{
  if (KNO_PORTP(arg)) {
    struct KNO_PORT *p=
      kno_consptr(struct KNO_PORT *,arg,kno_ioport_type);
    if (p->port_input)
      return KNO_TRUE;
    else return KNO_FALSE;}
  else return KNO_FALSE;
}

DEFC_PRIM("output-port?",output_portp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns #t if *object* is an output port.",
	  {"arg",kno_any_type,KNO_VOID})
static lispval output_portp(lispval arg)
{
  if (KNO_PORTP(arg)) {
    struct KNO_PORT *p=
      kno_consptr(struct KNO_PORT *,arg,kno_ioport_type);
    if (p->port_output)
      return KNO_TRUE;
    else return KNO_FALSE;}
  else return KNO_FALSE;
}

/* Identifying end of file */

DEFC_PRIM("eof-object?",eofp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns #t if *object* is an end of file "
	  "indicators.",
	  {"x",kno_any_type,KNO_VOID})
static lispval eofp (lispval x)
{
  if (KNO_EOFP(x)) return KNO_TRUE; else return KNO_FALSE;
}

/* DTYPE streams */

DEFC_PRIM("packet->dtype",packet2dtype,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "parses the DType representation in *packet* and "
	  "returns the corresponding object.",
	  {"packet",kno_packet_type,KNO_VOID})
static lispval packet2dtype(lispval packet)
{
  lispval object;
  struct KNO_INBUF in = { 0 };
  KNO_INIT_BYTE_INPUT(&in,KNO_PACKET_DATA(packet),
		      KNO_PACKET_LENGTH(packet));
  object = kno_read_dtype(&in);
  return object;
}

DEFC_PRIM("dtype->packet",lisp2packet,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "returns a packet containing the DType "
	  "representation of object. *bufsize*, if provided, "
	  "specifies the initial size of the output buffer "
	  "to be reserved.",
	  {"object",kno_any_type,KNO_VOID},
	  {"initsize",kno_fixnum_type,KNO_VOID})
static lispval lisp2packet(lispval object,lispval initsize)
{
  size_t size = FIX2INT(initsize);
  struct KNO_OUTBUF out = { 0 };
  KNO_INIT_BYTE_OUTPUT(&out,size);
  int bytes = kno_write_dtype(&out,object);
  if (bytes<0)
    return KNO_ERROR;
  else if ( (BUFIO_ALLOC(&out)) == KNO_HEAP_BUFFER )
    return kno_init_packet(NULL,bytes,out.buffer);
  else {
    lispval packet = kno_make_packet
      (NULL,out.bufwrite-out.buffer,out.buffer);
    kno_close_outbuf(&out);
    return packet;}
}

/* XTYPE streams */

DEFC_PRIM("decode-xtype",decode_xtype,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "parses the XTYPE representation in *packet* and "
	  "returns the corresponding object.",
	  {"packet",kno_packet_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_VOID})
static lispval decode_xtype(lispval packet,lispval opts)
{
  lispval object;
  lispval refs_arg = kno_getxrefs(opts);
  struct XTYPE_REFS *refs = 
    (KNO_RAW_TYPEP(opts,kno_xtrefs_typetag)) ?
    (KNO_RAWPTR_VALUE(opts)) :
    (NULL);
  struct KNO_INBUF in = { 0 };
  KNO_INIT_BYTE_INPUT(&in,KNO_PACKET_DATA(packet),
		      KNO_PACKET_LENGTH(packet));
  object = kno_read_xtype(&in,refs);
  kno_decref(refs_arg);
  return object;
}

static lispval compress_xtype(kno_compress_type compression,
			      kno_outbuf uncompressed);

DEFC_PRIM("encode-xtype",encode_xtype,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "returns a packet containing the XType "
	  "representation of object. *bufsize*, if provided, "
	  "specifies the initial size of the output buffer "
	  "to be reserved.",
	  {"object",kno_any_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_FALSE})
static lispval encode_xtype(lispval object,lispval opts)
{
  size_t size = getposfixopt(opts,KNOSYM_BUFSIZE,8000);
  struct KNO_OUTBUF out = { 0 };
  KNO_INIT_BYTE_OUTPUT(&out,size);
  lispval refs_arg = kno_getxrefs(opts);
  struct XTYPE_REFS *refs = (KNO_RAW_TYPEP(refs_arg,kno_xtrefs_typetag)) ?
    (KNO_RAWPTR_VALUE(refs_arg)) : (NULL);
  ssize_t bytes = (kno_testopt(opts,embed_symbol,KNO_FALSE)) ?
    (kno_write_xtype(&out,object,refs)) :
    (kno_embed_xtype(&out,object,refs));
  kno_decref(refs_arg);
  if (bytes<0) return KNO_ERROR;
  kno_compress_type compression = kno_compression_type(opts,KNO_NOCOMPRESS);
  if (compression == KNO_NOCOMPRESS) {
    if ( (BUFIO_ALLOC(&out)) == KNO_HEAP_BUFFER )
      return kno_init_packet(NULL,bytes,out.buffer);
    else {
      lispval packet = kno_make_packet
	(NULL,out.bufwrite-out.buffer,out.buffer);
      kno_close_outbuf(&out);
      return packet;}}
  else return compress_xtype(compression,&out);
}

DEFC_PRIM("precode-xtype",precode_xtype,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "returns a 'precoded' xtype encoded with a particular "
	  "set of XREFs. Writing out this XTYPE just writes the "
	  "precoded bytes, which can be much more efficient.",
	  {"object",kno_any_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_FALSE})
static lispval precode_xtype(lispval object,lispval opts)
{
  size_t size = getposfixopt(opts,KNOSYM_BUFSIZE,8000);
  struct KNO_OUTBUF out = { 0 };
  KNO_INIT_BYTE_OUTPUT(&out,size);
  lispval refs_arg = kno_getxrefs(opts);
  struct XTYPE_REFS *refs = (KNO_RAW_TYPEP(refs_arg,kno_xtrefs_typetag)) ?
    (KNO_RAWPTR_VALUE(refs_arg)) : (NULL);
  ssize_t bytes = kno_write_xtype(&out,object,refs);
  if (bytes<0) {
    kno_decref(refs_arg);
    return KNO_ERROR;}
  kno_compress_type compression = kno_compression_type(opts,KNO_NOCOMPRESS);
  lispval packet = KNO_VOID;
  if ( (compression == KNO_NOCOMPRESS) &&
       ( (BUFIO_ALLOC(&out)) == KNO_HEAP_BUFFER ) )
    packet = kno_init_packet(NULL,bytes,out.buffer);
  else {
    if (compression == KNO_NOCOMPRESS)
      packet = kno_make_packet(NULL,out.bufwrite-out.buffer,out.buffer);
    else packet = compress_xtype(compression,&out);
    kno_close_outbuf(&out);}
  lispval result = (refs) ?
    (kno_init_compound(NULL,KNOSYM_XTYPE,0,2,packet,refs_arg)) :
    (kno_init_compound(NULL,KNOSYM_XTYPE,0,1,packet));
  if (KNO_ABORTED(result)) {
    kno_decref(packet);
    kno_decref(refs_arg);}
  return result;
}

static lispval compress_xtype(kno_compress_type compression,
			      kno_outbuf uncompressed)
{
    int compression_code =
      (compression == KNO_SNAPPY) ? (xt_snappy) :
      (compression == KNO_ZLIB) ? (xt_zlib) :
      (compression == KNO_ZLIB9) ? (xt_zlib) :
      (compression == KNO_ZSTD) ? (xt_zstd) :
      (compression == KNO_ZSTD9) ? (xt_zstd) :
      (compression == KNO_ZSTD19) ? (xt_zstd) :
      (-1);
    if (compression_code < 0)
      return kno_err("BadCompressionCode","encode_xtype",NULL,KNO_VOID);
    ssize_t compressed_len = -1;
    unsigned char *compressed =
      kno_compress(compression,&compressed_len,
		   uncompressed->buffer,
		   uncompressed->bufwrite-uncompressed->buffer,
		   NULL);
    if (compressed == NULL) {
      kno_close_outbuf(uncompressed);
      return KNO_ERROR;}
    else {
      ssize_t output_len = 1+1+1+9+compressed_len;
      lispval packet = kno_make_packet(NULL,output_len,NULL);
      if (KNO_ABORTED(packet)) {
	kno_close_outbuf(uncompressed);
	return KNO_ERROR;}
      struct KNO_STRING *str = (kno_string) packet;
      unsigned char *data = (unsigned char *) str->str_bytes;
      struct KNO_OUTBUF cmpout = { 0 };
      KNO_INIT_OUTBUF(&cmpout,data,output_len,0);
      kno_write_byte(&cmpout,xt_compressed);
      kno_write_byte(&cmpout,compression_code);
      kno_write_byte(&cmpout,xt_packet);
      kno_write_varint(&cmpout,compressed_len);
      kno_write_bytes(&cmpout,compressed,compressed_len);
      str->str_bytelen = cmpout.bufwrite-cmpout.buffer;
      u8_big_free(compressed);
      return packet;}
}

DEFC_PRIM("xtype/refs",make_xtype_refs,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "returns a rawptr object for an xtype refs object.",
	  {"vec",kno_vector_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_FALSE})
static lispval make_xtype_refs(lispval vec,lispval opts)
{
  struct XTYPE_REFS *refs = u8_alloc(struct XTYPE_REFS);
  unsigned int flags = 0;
  if ( (kno_testopt(opts,KNOSYM_READONLY,KNO_VOID)) &&
       ( (!(kno_testopt(opts,KNOSYM_READONLY,KNO_FALSE))) ||
	 (!(kno_testopt(opts,KNOSYM_READONLY,KNO_DEFAULT)) ) ) )
    flags |= XTYPE_REFS_READ_ONLY;
  else {
    if (kno_testopt(opts,KNOSYM(addoids),KNO_FALSE))
      flags |= XTYPE_REFS_ADD_OIDS;
    if (kno_testopt(opts,KNOSYM(addsyms),KNO_FALSE))
      flags |= XTYPE_REFS_ADD_SYMS;}
  size_t n_refs = KNO_VECTOR_LENGTH(vec);
  size_t len    = ( flags & XTYPE_REFS_READ_ONLY) ? (n_refs) :
    ((n_refs/256)+2)*256;
  lispval *inits = KNO_VECTOR_ELTS(vec), *elts = u8_alloc_n(len,lispval);
  size_t i = 0; while (i<len) {
    lispval elt = inits[i];
    kno_incref(elt);
    elts[i]=elt;
    i++;}
  kno_init_xrefs(refs,n_refs,len,flags,-1,elts,NULL);
  return kno_wrap_xrefs(refs);
}

DEFC_PRIM("xtype/refs/encode",xtype_refs_encode,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "returns the numeric reference code for *ref* in "
	  "*xtrefs* or false otherwise.",
	  {"refs_arg",kno_rawptr_type,KNO_VOID},
	  {"val",kno_any_type,KNO_FALSE},
	  {"add",kno_any_type,KNO_FALSE})
static lispval xtype_refs_encode(lispval refs_arg,lispval val,lispval add)
{
  if (RARELY(!(KNO_RAW_TYPEP(refs_arg,kno_xtrefs_typetag))))
    return kno_err("NotXTypeRefs","xtype_refs_encode",NULL,refs_arg);
  struct XTYPE_REFS *refs = KNO_RAWPTR_VALUE(refs_arg);
  int add_flag = (KNO_FALSEP(add)) ? (0) :
    ( (KNO_VOIDP(add)) || (KNO_DEFAULTP(add)) ) ? (-1) : (1);
  if ( (add_flag == 1) && (refs->xt_refs_flags & XTYPE_REFS_READ_ONLY) )
    return kno_err("ReadOnlyXTRefs","xtype_refs_encode",NULL,refs_arg);
  ssize_t off = kno_xtype_ref(val,refs,add_flag);
  if (off<0)
    return KNO_FALSE;
  else return KNO_INT(off);
}

DEFC_PRIM("xtype/refs/decode",xtype_refs_decode,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "returns the object associated with the numeric "
	  "reference code *offset* in *xtrefs*.",
	  {"refs_arg",kno_rawptr_type,KNO_VOID},
	  {"off_arg",kno_fixnum_type,KNO_FALSE})
static lispval xtype_refs_decode(lispval refs_arg,lispval off_arg)
{
  if (RARELY(!(KNO_RAW_TYPEP(refs_arg,kno_xtrefs_typetag))))
    return kno_err("NotXTypeRefs","xtype_refs_decode",NULL,refs_arg);
  struct XTYPE_REFS *refs = KNO_RAWPTR_VALUE(refs_arg);
  ssize_t off = KNO_FIX2INT(off_arg);
  if (off<0)
    return kno_err("InvalidXRefCode","xtype_refs_decode",NULL,off_arg);
  else if (off < refs->xt_n_refs)
    return kno_incref(refs->xt_refs[off]);
  else return kno_err("XRefRangeError","xtype_refs_decode",NULL,off_arg);
}

DEFC_PRIM("xtype/refs/count",xtype_refs_count,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns the number of objects encoded in *xtrefs*",
	  {"refs_arg",kno_rawptr_type,KNO_VOID})
static lispval xtype_refs_count(lispval refs_arg)
{
  if (RARELY(!(KNO_RAW_TYPEP(refs_arg,kno_xtrefs_typetag))))
    return kno_err("NotXTypeRefs","xtype_refs_count",NULL,refs_arg);
  struct XTYPE_REFS *refs = KNO_RAWPTR_VALUE(refs_arg);
  return KNO_INT(refs->xt_n_refs);
}

/* Output strings */

DEFC_PRIM("open-output-string",open_output_string,
	  KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
	  "returns an output string stream")
static lispval open_output_string()
{
  U8_OUTPUT *out = u8_alloc(struct U8_OUTPUT);
  U8_INIT_OUTPUT(out,256);
  return kno_make_port(NULL,out,u8_strdup("output string"));
}

DEFC_PRIM("open-input-string",open_input_string,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns an input stream reading from *string*.",
	  {"arg",kno_string_type,KNO_VOID})
static lispval open_input_string(lispval arg)
{
  if (STRINGP(arg)) {
    U8_INPUT *in = u8_alloc(struct U8_INPUT);
    U8_INIT_STRING_INPUT(in,KNO_STRING_LENGTH(arg),u8_strdup(CSTRING(arg)));
    in->u8_streaminfo = in->u8_streaminfo|U8_STREAM_OWNS_BUF;
    return kno_make_port(in,NULL,u8_strdup("input string"));}
  else return kno_type_error(_("string"),"open_input_string",arg);
}

DEFC_PRIM("portid",portid_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns the id string (if any) for *port*.",
	  {"port_arg",kno_any_type,KNO_VOID})
static lispval portid_prim(lispval port_arg)
{
  if (KNO_PORTP(port_arg)) {
    struct KNO_PORT *port = (struct KNO_PORT *)port_arg;
    if (port->port_id)
      return kno_mkstring(port->port_id);
    else return KNO_FALSE;}
  else return kno_type_error(_("port"),"portid",port_arg);
}

DEFC_PRIM("portdata",portdata_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns the buffered data for *port*. If *port* "
	  "is a string stream, this is the output to date to "
	  "the port.",
	  {"port_arg",kno_any_type,KNO_VOID})
static lispval portdata_prim(lispval port_arg)
{
  if (KNO_PORTP(port_arg)) {
    struct KNO_PORT *port = (struct KNO_PORT *)port_arg;
    if (port->port_output)
      return kno_substring(port->port_output->u8_outbuf,
			   port->port_output->u8_write);
    else return kno_substring(port->port_output->u8_outbuf,
			      port->port_output->u8_outlim);}
  else return kno_type_error(_("port"),"portdata",port_arg);
}

/* Simple STDIO */

DEFC_PRIM("write",write_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "writes a textual represenntation of *object* to "
	  "*port*. The implementation strives to make `READ` "
	  "be able to convert the output of `WRITE` to "
	  "`EQUAL?` objects.\nIf *port* is #t or not "
	  "provided, the current output, which is usually "
	  "the stdout, is used. Otherwise, it must be an "
	  "output port.",
	  {"x",kno_any_type,KNO_VOID},
	  {"portarg",kno_any_type,KNO_VOID})
static lispval write_prim(lispval x,lispval portarg)
{
  U8_OUTPUT *out = get_output_port(portarg);
  if (out) {
    kno_unparse(out,x);
    u8_flush(out);
    return VOID;}
  else return kno_type_error(_("output port"),"write_prim",portarg);
}

DEFC_PRIM("display",display_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "writes a textual represenntation of *object* to "
	  "*port*. This makes no special attempts to make "
	  "it's output parsable by `READ`\nIf *port* is #t or "
	  "not provided, the current output, which is "
	  "usually the stdout, is used. Otherwise, it must "
	  "be an output port.",
	  {"x",kno_any_type,KNO_VOID},
	  {"portarg",kno_any_type,KNO_VOID})
static lispval display_prim(lispval x,lispval portarg)
{
  U8_OUTPUT *out = get_output_port(portarg);
  if (out) {
    if (STRINGP(x))
      u8_puts(out,CSTRING(x));
    else kno_unparse(out,x);
    u8_flush(out);
    return VOID;}
  else return kno_type_error(_("output port"),"display_prim",portarg);
}

DEFC_PRIM("putchar",putchar_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "the character *char* to *port*. *char* must be "
	  "either a character object or a positive integer "
	  "corresponding to a Unicode code point.\nIf *port* "
	  "is #t or not provided, the current default "
	  "output, is used. Otherwise, it must be an output "
	  "port.",
	  {"char_arg",kno_any_type,KNO_VOID},
	  {"port",kno_any_type,KNO_VOID})
static lispval putchar_prim(lispval char_arg,lispval port)
{
  int ch;
  U8_OUTPUT *out = get_output_port(port);
  if (out) {
    if (KNO_CHARACTERP(char_arg))
      ch = KNO_CHAR2CODE(char_arg);
    else if (KNO_UINTP(char_arg))
      ch = FIX2INT(char_arg);
    else return kno_type_error("character","putchar_prim",char_arg);
    u8_putc(out,ch);
    return VOID;}
  else return kno_type_error(_("output port"),"putchar_prim",port);
}

DEFC_PRIM("newline",newline_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	  "emits a newline to *port*. If *port* is #t or not "
	  "provided, the current output, which is usually "
	  "the stdout, is used. Otherwise, it must be an "
	  "output port.",
	  {"portarg",kno_any_type,KNO_VOID})
static lispval newline_prim(lispval portarg)
{
  U8_OUTPUT *out = get_output_port(portarg);
  if (out) {
    u8_puts(out,"\n");
    u8_flush(out);
    return VOID;}
  else return kno_type_error(_("output port"),"newline_prim",portarg);
}

/* PRINTOUT handlers */

static int printout_helper(U8_OUTPUT *out,lispval x)
{
  if (KNO_ABORTED(x))
    return 0;
  else if (VOIDP(x))
    return 1;
  if (out == NULL) out = u8_current_output;
  if (STRINGP(x))
    u8_puts(out,CSTRING(x));
  else if ( (out->u8_streaminfo) & (KNO_U8STREAM_HISTORIC) ) {
    lispval history = kno_thread_get(KNOSYM_HISTORY_TAG);
    if (KNO_HISTORYP(history))  {
      int num = kno_history_add(history,x,VOID);
      if (num < 0)
	kno_unparse(out,x);
      else {
	u8_printf(out,"(#%d=) ",num);
	kno_unparse(out,x);}}
    else kno_unparse(out,x);}
  else kno_unparse(out,x);
  return 1;
}

KNO_EXPORT
lispval kno_printout(lispval body,kno_lexenv env)
{
  kno_stack _stack=kno_stackptr;
  U8_OUTPUT *out = u8_current_output;
  while (PAIRP(body)) {
    lispval value = kno_eval(KNO_CAR(body),env,_stack);
    if (KNO_ABORTED(value)) {
      u8_flush(out);
      return value;}
    else if (printout_helper(out,value))
      kno_decref(value);
    else return value;
    body = KNO_CDR(body);}
  u8_flush(out);
  return VOID;
}

KNO_EXPORT
lispval kno_printout_to(U8_OUTPUT *out,lispval body,kno_lexenv env)
{
  kno_stack _stack=kno_stackptr;
  u8_output prev = u8_current_output;
  u8_set_default_output(out);
  while (PAIRP(body)) {
    lispval value = kno_eval(KNO_CAR(body),env,_stack);
    if (KNO_ABORTED(value)) {
      u8_flush(out);
      u8_set_default_output(prev);
      return value;}
    else if (printout_helper(out,value))
      kno_decref(value);
    else {
      u8_flush(out);
      u8_set_default_output(prev);
      return value;}
    body = KNO_CDR(body);}
  u8_flush(out);
  u8_set_default_output(prev);
  return VOID;
}

/* Special output functions */

DEFC_PRIM("substringout",substringout,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
	  "emits a substring of *string* to the default "
	  "output.",
	  {"arg",kno_string_type,KNO_VOID},
	  {"start",kno_fixnum_type,KNO_VOID},
	  {"end",kno_fixnum_type,KNO_VOID})
static lispval substringout(lispval arg,lispval start,lispval end)
{
  u8_output output = u8_current_output;
  u8_string string = CSTRING(arg); unsigned int len = STRLEN(arg);
  if (VOIDP(start)) u8_putn(output,string,len);
  else if (!(KNO_UINTP(start)))
    return kno_type_error("uint","substringout",start);
  else if (VOIDP(end)) {
    unsigned int byte_start = u8_byteoffset(string,FIX2INT(start),len);
    u8_putn(output,string+byte_start,len-byte_start);}
  else if (!(KNO_UINTP(end)))
    return kno_type_error("uint","substringout",end);
  else {
    unsigned int byte_start = u8_byteoffset(string,FIX2INT(start),len);
    unsigned int byte_end = u8_byteoffset(string,FIX2INT(end),len);
    u8_putn(output,string+byte_start,byte_end-byte_start);}
  return VOID;
}

DEFC_PRIM("uniscape",uniscape,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "emits a unicode escaped version of *string* to "
	  "the default output. All non-ascii characters "
	  "except for those in *except_string* are encoded "
	  "as \\uXXXX escape sequences",
	  {"arg",kno_string_type,KNO_VOID},
	  {"excluding",kno_string_type,KNO_VOID})
static lispval uniscape(lispval arg,lispval excluding)
{
  u8_string input = ((STRINGP(arg))?(CSTRING(arg)):
		     (kno_lisp2string(arg)));
  u8_string exstring = ((STRINGP(excluding))?
			(CSTRING(excluding)):
			((u8_string)""));
  u8_output output = u8_current_output;
  u8_string string = input;
  const u8_byte *scan = string;
  int c = u8_sgetc(&scan);
  while (c>0) {
    if ((c>=0x80)||(strchr(exstring,c))) {
      u8_printf(output,"\\u%04x",c);}
    else u8_putc(output,c);
    c = u8_sgetc(&scan);}
  if (!(STRINGP(arg))) u8_free(input);
  return VOID;
}

DEFC_EVALFN("PRINTOUT-TO",printout_to_evalfn,KNO_EVALFN_NOTAIL,
	    "`(PRINTOUT-TO *port* ...*args*)` generates output from "
	    "*args* which is written to *port*. "
	    "Returns VOID")
static lispval printout_to_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval dest_arg = kno_get_arg(expr,1);
  if (KNO_VOIDP(dest_arg))
    return kno_err(kno_SyntaxError,"printout_to_evalfn",NULL,expr);
  lispval dest = kno_eval(dest_arg,env,_stack);
  if (ABORTED(dest)) return dest;
  u8_output f = NULL;
  if (KNO_PORTP(dest)) {
    struct KNO_PORT *p = (kno_port) dest;
    f = p->port_output;}
  if (f == NULL) {
    lispval err = kno_type_error("output port","printout_to_evalfn",dest);
    kno_decref(dest);
    return err;}
  u8_output oldf = u8_current_output;
  u8_set_default_output(f);
  {lispval body = kno_get_body(expr,2);
    KNO_DOLIST(ex,body)  {
      lispval value = kno_eval(ex,env,_stack);
      if (ABORTED(value)) {
	kno_decref(dest);
	u8_set_default_output(oldf);
	return value;}
      else if (printout_helper(f,value))
	kno_decref(value);
      else {
	kno_decref(dest);
	u8_set_default_output(oldf);
	return value;}}}
  u8_flush(f);
  u8_set_default_output(oldf);
  kno_decref(dest);
  return VOID;
}

DEFC_EVALFN("PRINTOUT",printout_evalfn,KNO_EVALFN_NOTAIL,
	    "`(PRINTOUT ...*args*)` generates output from "
	    "*args* which is written to the standard output. "
	    "Returns VOID")
static lispval printout_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  return kno_printout(kno_get_body(expr,1),env);
}

DEFC_EVALFN("LINEOUT",lineout_evalfn,KNO_EVALFN_NOTAIL,
	    "`(LINEOUT ...*args*)` generates output from "
	    "*args* which is written to the standard output "
	    "with a trailing newline. Returns VOID.")
static lispval lineout_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  U8_OUTPUT *out = u8_current_output;
  lispval value = kno_printout(kno_get_body(expr,1),env);
  if (KNO_ABORTP(value)) return value;
  u8_putc(out,'\n');
  u8_flush(out);
  return VOID;
}

DEFC_EVALFN("STRINGOUT",stringout_evalfn,KNO_EVALFN_NOTAIL,
	    "`(STRINGOUT ...*args*)` generates output from "
	    "*args* and returns the output as a string.")
static lispval stringout_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  struct U8_OUTPUT out; lispval result; u8_byte buf[256];
  U8_INIT_OUTPUT_X(&out,256,buf,0);
  result = kno_printout_to(&out,kno_get_body(expr,1),env);
  if (!(KNO_ABORTP(result))) {
    kno_decref(result);
    result = kno_make_string
      (NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);}
  u8_close_output(&out);
  return result;
}

DEFC_EVALFN("INDENTOUT",indentout_evalfn,KNO_EVALFN_NOTAIL,
	    "`(INDENTOUT *indent* ... *args*)` generates output from "
	    "*args*, preceding each line of output with either *indent* "
	    "(if it's a string) or *indent* spaces (if it's a positive "
	    "integer")
static lispval indentout_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval indent_expr = kno_get_arg(expr,1);
  if (KNO_VOIDP(indent_expr))
    return kno_err(kno_SyntaxError,"indentout_evalfn",NULL,expr);
  lispval indent_val = kno_eval(indent_expr,env,_stack);
  u8_string indent_string = NULL; int indent_len = 4;
  if (ABORTED(indent_val))
    return indent_val;
  else if (STRINGP(indent_val)) {
    if (KNO_STRLEN(indent_val)==0) {
      kno_decref(indent_val);
      return kno_printout(kno_get_body(expr,2),env);}
    else indent_string = KNO_CSTRING(indent_val);}
  else if ( (KNO_FIXNUMP(indent_val)) && ((KNO_FIX2INT(indent_val)) >= 0) ) {
    indent_len = KNO_FIX2INT(indent_val);
    if (indent_len == 0) return kno_printout(kno_get_body(expr,2),env);}
  else if (KNO_FALSEP(indent_val))
    return kno_printout(kno_get_body(expr,2),env);
  else {
    u8_byte buf[100];
    lispval err = kno_err("InvalidIndent","indentout_evalfn",
			  u8_bprintf(buf,"%q",indent_expr),
			  indent_val);
    kno_decref(indent_val);
    return err;}
  u8_byte indent_buf[indent_len+1];
  if (indent_string == NULL) {
    int i = 0; while (i<indent_len) indent_buf[i++]=' ';
    indent_buf[i]='\0';
    indent_string=indent_buf;}
  else indent_len = KNO_STRLEN(indent_val);
  struct U8_OUTPUT out;
  U8_INIT_OUTPUT(&out,1000);
  lispval result = kno_printout_to(&out,kno_get_body(expr,2),env);
  if (KNO_ABORTED(result)) {
    u8_close_output(&out);
    kno_decref(indent_val);
    return result;}
  else {
    U8_OUTPUT *curout = u8_current_output;
    u8_string start = out.u8_outbuf, scan = strchr(start,'\n');
    while (scan) {
      u8_putn(curout,indent_string,indent_len);
      u8_putn(curout,start,(scan-start)+1);
      start = scan+1;
      scan = strchr(start,'\n');}
    if ( (start) && (*start) ) {
      u8_putn(curout,indent_string,indent_len);
      u8_puts(curout,start);}
    u8_flush(curout);
    u8_close_output(&out);
    kno_decref(indent_val);
    return KNO_VOID;}
}

/* Functions to be used in printout bodies */

DEFC_PRIM("$histstring",histstring_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "declares and returns a string with a history "
	  "reference for *object*. *label*, if provided, "
	  "specifies a non-numeric label to use.",
	  {"x",kno_any_type,KNO_VOID},
	  {"label",kno_any_type,KNO_VOID})
static lispval histstring_prim(lispval x,lispval label)
{
  lispval history = kno_thread_get(KNOSYM_HISTORY_TAG);
  if (KNO_HISTORYP(history)) {
    lispval ref = kno_history_add(history,x,label);
    if ( (KNO_FALSEP(ref)) || (KNO_VOIDP(ref)) || (KNO_EMPTYP(ref)) ) {
      return KNO_FALSE;}
    else {
      u8_byte buf[32];
      u8_bprintf(buf,"#%q",ref);
      kno_decref(ref);
      return kno_make_string(NULL,-1,buf);}}
  else return KNO_FALSE;
}

DEFC_PRIM("$histref",histref_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "declares and outputs a history reference for "
	  "*object* to the current output. *label*, if "
	  "provided, specifies a non-numeric label to use.",
	  {"x",kno_any_type,KNO_VOID},
	  {"label",kno_any_type,KNO_VOID})
static lispval histref_prim(lispval x,lispval label)
{
  lispval history = kno_thread_get(KNOSYM_HISTORY_TAG);
  if (KNO_HISTORYP(history)) {
    U8_OUTPUT *out = u8_current_output;
    lispval ref = kno_history_add(history,x,label);
    if ( (KNO_FALSEP(ref)) || (KNO_VOIDP(ref)) || (KNO_EMPTYP(ref)) )
      return VOID;
    else {
      u8_printf(out,"#%q",ref);
      kno_decref(ref);
      return VOID;}}
  else return VOID;
}

DEFC_PRIM("$histval",histval_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "declares and outputs a history reference for "
	  "*object* to the current output. *label*, if "
	  "provided, specifies a non-numeric label to use.",
	  {"x",kno_any_type,KNO_VOID},
	  {"label",kno_any_type,KNO_VOID})
static lispval histval_prim(lispval x,lispval label)
{
  lispval history = kno_thread_get(KNOSYM_HISTORY_TAG);
  if (KNO_HISTORYP(history))  {
    U8_OUTPUT *out = u8_current_output;
    lispval ref = kno_history_add(history,x,label);
    if ( (KNO_FALSEP(ref)) || (KNO_VOIDP(ref)) || (KNO_EMPTYP(ref)) ) {
      u8_printf(out,"%q",x);
      return VOID;}
    else {
      u8_printf(out,"(#%q) %q",ref,x);
      return VOID;}}
  else {
    U8_OUTPUT *out = u8_current_output;
    u8_printf(out,"%q",x);
    return VOID;}
}

/* Input operations! */

DEFC_PRIM("getchar",getchar_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	  "reads a single character from *port*. If *port* "
	  "is #t or not provided, the current default input, "
	  "is used. Otherwise, it must be an input port.",
	  {"port",kno_any_type,KNO_VOID})
static lispval getchar_prim(lispval port)
{
  U8_INPUT *in = get_input_port(port);
  if (in) {
    int ch = -1;
    if (in) ch = u8_getc(in);
    if (ch<0) return KNO_EOF;
    else return KNO_CODE2CHAR(ch);}
  else return kno_type_error(_("input port"),"getchar_prim",port);
}

DEFC_PRIM("getline",getline_prim,
	  KNO_MAX_ARGS(4)|KNO_MIN_ARGS(0),
	  "reads a single 'line' from *port* as a string. If "
	  "*port* is #t or not provided, the current default "
	  "input is used. Otherwise, it must be an input "
	  "port.\n* *eol* is the string that indicates the "
	  "line end, defaulting to a single newline; this "
	  "sequence is consumed but not included in the "
	  "returned string;\n* *maxchars* indicates the "
	  "maximum number of characters to read while "
	  "waiting for *eol*;\n* *eof* an \"end of file\" "
	  "sequence which causes `GETLINE` to return #eof "
	  "when encountered.",
	  {"port",kno_any_type,KNO_VOID},
	  {"eos_arg",kno_any_type,KNO_VOID},
	  {"lim_arg",kno_any_type,KNO_VOID},
	  {"eof_marker",kno_any_type,KNO_VOID})
static lispval getline_prim(lispval port,lispval eos_arg,
			    lispval lim_arg,
			    lispval eof_marker)
{
  U8_INPUT *in = get_input_port(port);
  if ( (VOIDP(eof_marker)) || (KNO_DEFAULTP(eof_marker)) )
    eof_marker = KNO_EOF;
  if (in) {
    u8_string data, eos;
    ssize_t lim, size = 0;
    if (in == NULL)
      return kno_type_error(_("input port"),"getline_prim",port);
    if ( (VOIDP(eos_arg)) || (KNO_DEFAULTP(eos_arg)) )
      eos="\n";
    else if (STRINGP(eos_arg)) eos = CSTRING(eos_arg);
    else return kno_type_error(_("string"),"getline_prim",eos_arg);
    if ( (VOIDP(lim_arg)) || (KNO_DEFAULTP(lim_arg)) )
      lim = 0;
    else if (FIXNUMP(lim_arg)) lim = FIX2INT(lim_arg);
    else return kno_type_error(_("fixum"),"getline_prim",eos_arg);
    data = u8_gets_x(NULL,lim,in,eos,&size);
    if (data)
      if (strlen(data)<size) {
	/* Handle embedded NUL */
	struct U8_OUTPUT out;
	const u8_byte *scan = data, *limit = scan+size;
	U8_INIT_OUTPUT(&out,size+8);
	while (scan<limit) {
	  if (*scan)
	    u8_putc(&out,u8_sgetc(&scan));
	  else u8_putc(&out,0);}
	u8_free(data);
	return kno_stream2string(&out);}
      else return kno_init_string(NULL,size,data);
    else if (size<0)
      if (errno == EAGAIN)
	return KNO_EOF;
      else return KNO_ERROR;
    else return kno_incref(eof_marker);}
  else return kno_type_error(_("input port"),"getline_prim",port);
}

DEFC_PRIM("unescape-string",unescape_string_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "interprets escape characters in *string* and "
	  "returns the corresponding unescaped version. "
	  "Escaped character includes C character escapes "
	  "(e.g. \\n or \\f) as well as numeric unicode "
	  "escapes (e.g. \\u0065 or \\u2323)",
	  {"string",kno_string_type,KNO_VOID})
static lispval unescape_string_prim(lispval string)
{
  struct U8_INPUT in;
  U8_INIT_STRING_INPUT(&in,KNO_STRLEN(string),KNO_STRDATA(string));
  return kno_decode_string(&in);
}

DEFC_PRIM("escape-string",escape_string_prim,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
	  "generates an escaped version of *string*, "
	  "specifically escaping control characters and the "
	  "string delimiter (\"). If *toascii* is not false, "
	  "non-ascii Unicode characters are also escaped. If "
	  "*maxlen* is not false, the result is truncated at "
	  "*maxlen* bytes.",
	  {"string",kno_string_type,KNO_VOID},
	  {"ascii",kno_constant_type,KNO_FALSE},
	  {"maxlen",kno_fixnum_type,KNO_VOID})
static lispval escape_string_prim(lispval string,lispval ascii,lispval maxlen)
{
  struct U8_OUTPUT out;
  U8_INIT_OUTPUT(&out,KNO_STRLEN(string));
  int rv = kno_escape_string
    (&out,string,(!(KNO_FALSEP(ascii))),
     (KNO_FIXNUMP(maxlen)) ? (KNO_FIX2INT(maxlen)) : (-1) );
  if (rv<0) {
    u8_close_output(&out);
    return KNO_ERROR;}
  return kno_stream_string(&out);
}

DEFC_PRIM("escapeout",escapeout_prim,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
	  "outputs an escaped version of *string* to the "
	  "default output. This specifically escaping "
	  "control characters and the string delimiter (\"). "
	  "If *toascii* is not false, non-ascii Unicode "
	  "characters are also escaped. If *maxlen* is not "
	  "false, the result is truncated at *maxlen* bytes.",
	  {"string",kno_string_type,KNO_VOID},
	  {"ascii",kno_constant_type,KNO_FALSE},
	  {"maxlen",kno_fixnum_type,KNO_VOID})
static lispval escapeout_prim(lispval string,lispval ascii,lispval maxlen)
{
  u8_output out = u8_current_output;
  int rv = kno_escape_string
    (out,string,(!(KNO_FALSEP(ascii))),
     (KNO_FIXNUMP(maxlen)) ? (KNO_FIX2INT(maxlen)) : (-1) );
  if (rv<0)
    return KNO_ERROR;
  else return KNO_VOID;
}

DEFC_PRIM("read",read_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	  "reads an object from *port*. If *port* is #t or "
	  "not provided, the current default input is used. "
	  "Otherwise, it must be an input port.",
	  {"port",kno_any_type,KNO_VOID})
static lispval read_prim(lispval port)
{
  if (STRINGP(port)) {
    struct U8_INPUT in;
    U8_INIT_STRING_INPUT(&in,STRLEN(port),CSTRING(port));
    return kno_parser(&in);}
  else {
    U8_INPUT *in = get_input_port(port);
    if (in) {
      int c = kno_skip_whitespace(in);
      if (c<0) return KNO_EOF;
      else return kno_parser(in);}
    else return kno_type_error(_("input port"),"read_prim",port);}
}

/* Reading records */

static off_t find_substring(u8_string string,lispval strings,
			    ssize_t len,ssize_t *lenp);
static ssize_t get_more_data(u8_input in,ssize_t lim);
static lispval record_reader(lispval port,lispval ends,lispval limit_arg);

DEFC_PRIM("read-record",read_record_prim,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "Reads a string from *ports* terminated by *ends* "
	  "(which is a string or a regex). This returns #f if "
	  "no *ends* match is found before *limit* characters.",
	  {"ports",kno_any_type,KNO_VOID},
	  {"ends",kno_any_type,KNO_VOID},
	  {"limit_arg",kno_any_type,KNO_VOID})
static lispval read_record_prim(lispval ports,lispval ends,
				lispval limit_arg)
{
  lispval results = EMPTY;
  DO_CHOICES(port,ports) {
    lispval result = record_reader(port,ends,limit_arg);
    CHOICE_ADD(results,result);}
  return results;
}

static lispval record_reader(lispval port,lispval ends,
			     lispval limit_arg)
{
  U8_INPUT *in = get_input_port(port);
  ssize_t lim, matchlen = 0, maxbuf=in->u8_bufsz;
  off_t off = -1;

  if (in == NULL)
    return kno_type_error(_("input port"),"record_reader",port);
  if (VOIDP(limit_arg)) lim = -1;
  else if (FIXNUMP(limit_arg))
    lim = FIX2INT(limit_arg);
  else return kno_type_error(_("fixnum"),"record_reader",limit_arg);

  if (VOIDP(ends)) {}
  else {
    DO_CHOICES(end,ends)
      if (!((STRINGP(end))||(TYPEP(end,kno_regex_type))))
	return kno_type_error(_("string"),"record_reader",end);}
  while (1) {
    if (VOIDP(ends)) {
      u8_string found = strstr(in->u8_read,"\n");
      if (found) {
	off = found-in->u8_read;
	matchlen = 1;}}
    else off = find_substring(in->u8_read,ends,
			      in->u8_inlim-in->u8_read,
			      &matchlen);
    if (off>=0) {
      size_t record_len = off+matchlen;
      lispval result = kno_make_string(NULL,record_len,in->u8_read);
      in->u8_read+=record_len;
      return result;}
    else if ((lim>0) && ((in->u8_inlim-in->u8_read)>lim))
      return KNO_EOF;
    else if (in->u8_fillfn) {
      if ((in->u8_inlim-in->u8_read)>=(maxbuf-16))
	maxbuf=maxbuf*2;
      ssize_t more_data = get_more_data(in,maxbuf);
      if (more_data>0) continue;
      else return KNO_EOF;}
    else return KNO_EOF;}
}

static off_t find_substring(u8_string string,lispval strings,
			    ssize_t len_arg,ssize_t *lenp)
{
  ssize_t len = (len_arg<0)?(strlen(string)):(len_arg);
  off_t off = -1; ssize_t matchlen = -1;
  DO_CHOICES(s,strings) {
    if (STRINGP(s)) {
      u8_string next = strstr(string,CSTRING(s));
      if (next) {
	if (off<0) {
	  off = next-string; matchlen = STRLEN(s);}
	else if ((next-string)<off) {
	  off = next-string;
	  if (matchlen<(STRLEN(s))) {
	    matchlen = STRLEN(s);}}
	else {}}}
    else if (TYPEP(s,kno_regex_type)) {
      off_t starts = kno_regex_op(rx_search,s,string,len,0);
      ssize_t matched_len = (starts<0)?(-1):
	(kno_regex_op(rx_matchlen,s,string+starts,len,0));
      if ((starts<0)||(matched_len<=0)) continue;
      else if ((off<0)||((starts<off)&&(matched_len>0))) {
	off = starts;
	matchlen = matched_len;}
      else {}}}
  if (off<0) return off;
  *lenp = matchlen;
  return off;
}

static ssize_t get_more_data(u8_input in,ssize_t lim)
{
  if ((in->u8_inbuf == in->u8_read)&&
      ((in->u8_inlim - in->u8_inbuf) == in->u8_bufsz)) {
    /* This is the case where the buffer is full of unread data */
    size_t bufsz = in->u8_bufsz;
    if (bufsz>=lim)
      return -1;
    else {
      size_t new_size = ((bufsz*2)>=U8_BUF_THROTTLE_POINT)?
	(bufsz+(U8_BUF_THROTTLE_POINT/2)):
	(bufsz*2);
      if (new_size>lim) new_size = lim;
      new_size = u8_grow_input_stream(in,new_size);
      if (new_size > bufsz)
	return in->u8_fillfn(in);
      else return 0;}}
  else return in->u8_fillfn(in);
}

/* Dumping the stack */

static lispval dumpstack_helper(lispval dest,int concise)
{
  U8_OUTPUT *out = NULL; int close_out = 0;
  struct U8_OUTPUT tmpout;
  if (KNO_FALSEP(dest)) {
    U8_INIT_STATIC_OUTPUT(tmpout,10000);
    out=&tmpout;}
  else if (STRINGP(dest)) {
    out = (u8_output) u8_open_output_file(CSTRING(dest),NULL,-1,-1);
    if (out == NULL) return KNO_ERROR;
    close_out = 1;}
  else out = get_output_port(dest);
  struct KNO_STACK *stack = kno_stackptr, *scan = stack;
  while (scan) {
    knodbg_show_stack_frame(out,scan,concise);
    u8_putc(out,'\n');
    scan=scan->stack_caller;}
  u8_flush(out);
  if (close_out) u8_close_output(out);
  if (KNO_STRINGP(dest))
    u8_log(LOGNOTICE,"StackDumped","%s stack dumped to %s",
	   ((concise)?("Concise"):("Detailed")),
	   KNO_CSTRING(dest));
  if (KNO_FALSEP(dest))
    return kno_init_string(NULL,u8_outbuf_len(&tmpout),tmpout.u8_outbuf);
  else return KNO_VOID;
}

DEFC_PRIM("dumpstack",dumpstack_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	  "Concisely writes the current stack, to *dest*, which can be "
	  "a port, #t for stdout, or a filename. If *dest* is #f, the stack "
	  "is written to a string and returned, otherwise `dumpstack` "
	  "returns VOID",
	  {"dest",kno_any_type,KNO_VOID})
static lispval dumpstack_prim(lispval dest)
{
  return dumpstack_helper(dest,1);
}

DEFC_PRIM("dumpstack/full",dumpstack_full_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	  "Verbosely dumps the current stack, to *dest*, which can be "
	  "a port, #t for stdout, or a filename.",
	  {"dest",kno_any_type,KNO_VOID})
static lispval dumpstack_full_prim(lispval dest)
{
  return dumpstack_helper(dest,0);
}

/* PPRINT lisp primitives */

static lispval column_symbol, depth_symbol, detail_symbol;
static lispval maxcol_symbol, width_symbol, margin_symbol;
static lispval maxelts_symbol, maxchars_symbol, maxbytes_symbol;
static lispval maxkeys_symbol, listmax_symbol, vecmax_symbol, choicemax_symbol;

#define PPRINT_MARGINBUF_SIZE 256

static lispval pprinter(int n,kno_argvec args)
{
  U8_OUTPUT *out=NULL;
  struct U8_OUTPUT tmpout;
  struct PPRINT_CONTEXT ppcxt={0};
  int arg_i=0, used[7]={0};
  int indent=0, close_port=0, stringout=0, col=0, depth=0;
  lispval opts = VOID, port_arg=VOID, obj = args[0]; used[0]=1;
  if (n>7) return kno_err(kno_TooManyArgs,"lisp_pprint",NULL,args[0]);
  while (arg_i<n)
    if (used[arg_i]) arg_i++;
    else {
      lispval arg = args[arg_i];
      if ( (out == NULL) &&
	   (out = get_output_port(arg)) )
	used[arg_i]=1;
      else if (KNO_FIXNUMP(arg)) {
	ppcxt.pp_maxcol = KNO_FIX2INT(arg);
	used[arg_i]=1;}
      else if (KNO_STRINGP(arg)) {
	ppcxt.pp_margin = CSTRING(arg);
	ppcxt.pp_margin_len = KNO_STRLEN(arg);
	used[arg_i]= 1;}
      else if (KNO_TABLEP(arg)) {
	opts=arg;
	used[arg_i]=1;}
      else if (KNO_FALSEP(arg)) {
	stringout=1; used[arg_i]=1;}
      else u8_log(LOG_WARN,"BadPPrintArg","%q",arg);
      arg_i++;}
  if ( (KNO_PAIRP(opts)) || (KNO_TABLEP(opts)) ) {
    lispval maxelts = kno_getopt(opts,maxelts_symbol,KNO_VOID);
    lispval colval = kno_getopt(opts,column_symbol,KNO_VOID);
    lispval depthval = kno_getopt(opts,depth_symbol,KNO_VOID);
    lispval maxchars = kno_getopt(opts,maxchars_symbol,KNO_VOID);
    lispval maxbytes = kno_getopt(opts,maxbytes_symbol,KNO_VOID);
    lispval maxkeys = kno_getopt(opts,maxkeys_symbol,KNO_VOID);
    lispval list_max = kno_getopt(opts,listmax_symbol,KNO_VOID);
    lispval vec_max = kno_getopt(opts,vecmax_symbol,KNO_VOID);
    lispval choice_max = kno_getopt(opts,choicemax_symbol,KNO_VOID);
    if (KNO_FIXNUMP(colval))   col   = KNO_FIX2INT(colval);
    if (KNO_FIXNUMP(depthval)) depth = KNO_FIX2INT(depthval);

    if (KNO_FIXNUMP(maxelts)) ppcxt.pp_maxelts=KNO_FIX2INT(maxelts);
    if (KNO_FIXNUMP(maxchars)) ppcxt.pp_maxchars=KNO_FIX2INT(maxchars);
    if (KNO_FIXNUMP(maxbytes)) ppcxt.pp_maxbytes=KNO_FIX2INT(maxbytes);
    if (KNO_FIXNUMP(maxkeys)) ppcxt.pp_maxkeys=KNO_FIX2INT(maxkeys);
    if (KNO_FIXNUMP(list_max)) ppcxt.pp_list_max=KNO_FIX2INT(list_max);
    if (KNO_FIXNUMP(vec_max)) ppcxt.pp_vector_max=KNO_FIX2INT(vec_max);
    if (KNO_FIXNUMP(choice_max)) ppcxt.pp_choice_max=KNO_FIX2INT(choice_max);
    kno_decref(maxbytes); kno_decref(maxchars);
    kno_decref(maxelts); kno_decref(maxkeys);
    kno_decref(list_max); kno_decref(vec_max);
    kno_decref(colval); kno_decref(depthval);
    kno_decref(choice_max);
    if (ppcxt.pp_margin == NULL) {
      lispval margin_opt = kno_getopt(opts,margin_symbol,KNO_VOID);
      if (KNO_STRINGP(margin_opt)) {
	ppcxt.pp_margin=CSTRING(margin_opt);
	ppcxt.pp_margin_len=KNO_STRLEN(margin_opt);}
      else if ( (KNO_UINTP(margin_opt)) ) {
	long long margin_width = KNO_FIX2INT(margin_opt);
	u8_byte *margin = alloca(margin_width+1);
	u8_byte *scan=margin, *limit=scan+margin_width;
	while (scan<limit) *scan++=' ';
	*scan='\0';
	ppcxt.pp_margin=margin;
	/* Since it's all ASCII spaces, margin_len (bytes) =
	   margin_width (chars) */
	ppcxt.pp_margin_len=margin_width;}
      else if ( (KNO_VOIDP(margin_opt)) || (KNO_DEFAULTP(margin_opt)) ) {}
      else u8_log(LOG_WARN,"BadPPrintMargin","%q",margin_opt);
      kno_decref(margin_opt);}
    if (ppcxt.pp_maxcol>0) {
      lispval maxcol = kno_getopt(opts,maxcol_symbol,KNO_VOID);
      if (KNO_VOIDP(maxcol))
	maxcol = kno_getopt(opts,width_symbol,KNO_VOID);
      if (KNO_FIXNUMP(maxcol))
	ppcxt.pp_maxcol=KNO_FIX2INT(maxcol);
      else if ( (KNO_VOIDP(maxcol)) || (KNO_DEFAULTP(maxcol)) )
	ppcxt.pp_maxcol=0;
      else ppcxt.pp_maxcol=-1;
      kno_decref(maxcol);}}
  if (out == NULL) {
    lispval port = kno_getopt(opts,KNOSYM_OUTPUT,VOID);
    if (!(VOIDP(port))) out = get_output_port(port);
    if (out)
      port_arg=port;
    else {kno_decref(port);}}
  if (out == NULL) {
    lispval filename = kno_getopt(opts,KNOSYM_FILENAME,VOID);
    if (!(VOIDP(filename))) {
      out = (u8_output) u8_open_output_file(CSTRING(filename),NULL,-1,-1);
      if (out == NULL) {
	kno_decref(filename);
	return KNO_ERROR_VALUE;}
      else {
	kno_decref(filename);
	close_port=1;}}}
  if (stringout) {
    U8_INIT_OUTPUT(&tmpout,1000);
    out=&tmpout;}
  else out = get_output_port(VOID);
  col = kno_pprinter(out,obj,indent,col,depth,NULL,NULL,&ppcxt);
  if (stringout)
    return kno_init_string(NULL,tmpout.u8_write-tmpout.u8_outbuf,
			   tmpout.u8_outbuf);
  else {
    u8_flush(out);
    if (close_port) u8_close_output(out);
    kno_decref(port_arg);
    return KNO_INT(col);}
}

DEFC_PRIMN("pprinter",lisp_pprinter,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
	   "(pprinter *object* *port* *width* *margin*)\n"
	   "Generates a formatted representation of *object*, "
	   "without a trailing newline, on *port*. If *port "
	   "is #f the representation is returned as a string, "
	   "otherwise the length of the last output line (the "
	   "'current column') is returned. *width*, if "
	   "provided, is a positive integer, and *margin* is "
	   "either a positive integer or a string to be used "
	   "as left-side indentation.")
static lispval lisp_pprinter(int n,kno_argvec args)
{
  return pprinter(n,args);
}

DEFC_PRIMN("pprint",lisp_pprint,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
	   "(pprint *object* *port* *width* *margin*)\n"
	   "Generates a formatted representation of *object*, "
	   "without a trailing newline, on *port*. If *port "
	   "is #f the representation is returned as a string, "
	   "otherwise VOID is returned. *width*, if "
	   "provided, is a positive integer, and *margin* is "
	   "either a positive integer or a string to be used "
	   "as left-side indentation.")
static lispval lisp_pprint(int n,kno_argvec args)
{
  lispval v = pprinter(n,args);
  if (KNO_STRINGP(v)) return v;
  kno_decref(v);
  return KNO_VOID;
}

DEFC_PRIMN("$pprint",lisp_4pprint,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
	   "($pprint *object* *width* *margin*)\n"
	   "Generates a formatted representation of *object*, "
	   "without a trailing newline, on *port*. Returns "
	   "VOID. *width*, if provided, is a positive integer, "
	   "and *margin* is either a positive integer or a "
	   "string to be used as indentation.")
static lispval lisp_4pprint(int n,kno_argvec args)
{
  lispval inner_args[4];
  inner_args[0]=args[0];
  inner_args[1]=KNO_VOID;
  if (n>1) inner_args[2]=args[1];
  if (n>2) inner_args[3]=args[2];
  lispval result = (n>1) ? (pprinter(n+1,inner_args)) : (pprinter(n,args));
  if (KNO_ABORTED(result)) return result;
  return KNO_VOID;
}


/* LIST object */

static int get_stringopt(lispval opts,lispval optname,u8_string *strval)
{
  lispval v = kno_getopt(opts,optname,KNO_VOID);
  if (KNO_VOIDP(v)) {
    return 0;}
  else if (KNO_STRINGP(v)) {
    *strval = KNO_CSTRING(v);
    kno_decref(v);
    return 1;}
  else {
    if (KNO_SYMBOLP(optname))
      kno_seterr("BadStringOpt","lisp_list_object",KNO_SYMBOL_NAME(optname),v);
    else if (KNO_STRINGP(optname))
      kno_seterr("BadStringOpt","lisp_list_object",KNO_CSTRING(optname),v);
    else kno_seterr("BadStringOpt","lisp_list_object",NULL,v);
    kno_decref(v);
    return -1;}
}

static int get_fixopt(lispval opts,lispval optname,long long *intval)
{
  lispval v = kno_getopt(opts,optname,KNO_VOID);
  if (KNO_VOIDP(v)) return 0;
  else if (KNO_FIXNUMP(v)) {
    *intval = KNO_FIX2INT(v);
    return 1;}
  else if (KNO_FALSEP(v)) {
    *intval = 0;
    return 1;}
  else {
    if (KNO_SYMBOLP(optname))
      kno_seterr("BadFixOpt","lisp_list_object",KNO_SYMBOL_NAME(optname),v);
    else if (KNO_STRINGP(optname))
      kno_seterr("BadStringOpt","lisp_list_object",KNO_CSTRING(optname),v);
    else kno_seterr("BadFixOpt","lisp_list_object",NULL,v);
    kno_decref(v);
    return -1;}
}

static lispval label_symbol, width_symbol, depth_symbol, output_symbol;

DEFC_PRIM("listdata",lisp_listdata,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "output a formatted textual representation of "
	  "*object* to *port*, controlled by *opts*.",
	  {"object",kno_any_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_VOID},
	  {"stream",kno_any_type,KNO_VOID})
static lispval lisp_listdata(lispval object,lispval opts,lispval stream)
{
  u8_string label=NULL, pathref=NULL, indent="";
  long long width = 100, detail = -1;
  if (KNO_FIXNUMP(opts)) {
    detail = KNO_FIX2INT(opts);
    opts = KNO_FALSE;}
  else if (KNO_TRUEP(opts)) {
    detail = 0;
    opts = KNO_FALSE;}
  else if (get_stringopt(opts,label_symbol,&label)<0)
    return KNO_ERROR;
  else if (get_stringopt(opts,margin_symbol,&indent)<0)
    return KNO_ERROR;
  else if (get_fixopt(opts,width_symbol,&width)<0)
    return KNO_ERROR;
  else if (get_fixopt(opts,depth_symbol,&detail)<0)
    return KNO_ERROR;
  else if (get_fixopt(opts,detail_symbol,&detail)<0)
    return KNO_ERROR;
  else NO_ELSE;
  if (KNO_VOIDP(stream))
    stream = kno_getopt(opts,output_symbol,KNO_VOID);
  else kno_incref(stream);
  U8_OUTPUT *out = get_output_port(stream);
  int rv = kno_list_object(out,object,label,pathref,indent,NULL,width,detail);
  u8_flush(out);
  kno_decref(stream);
  if (rv<0)
    return KNO_ERROR;
  else return KNO_VOID;
}

/* Base 64 stuff */

DEFC_PRIM("base64->packet",from_base64_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "converts the BASE64 encoding in string into a "
	  "data packet",
	  {"string",kno_string_type,KNO_VOID})
static lispval from_base64_prim(lispval string)
{
  const u8_byte *string_data = CSTRING(string);
  ssize_t string_len = STRLEN(string), data_len;
  unsigned char *data=
    u8_read_base64(string_data,string_data+string_len,&data_len);
  if (data)
    return kno_init_packet(NULL,data_len,data);
  else return KNO_ERROR;
}

DEFC_PRIM("packet->base64",to_base64_prim,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
	  "converts a packet into a string containing it's "
	  "BASE64 representation.",
	  {"packet",kno_packet_type,KNO_VOID},
	  {"nopad",kno_any_type,KNO_VOID},
	  {"urisafe",kno_any_type,KNO_VOID})
static lispval to_base64_prim(lispval packet,lispval nopad,
			      lispval urisafe)
{
  const u8_byte *packet_data = KNO_PACKET_DATA(packet);
  ssize_t packet_len = KNO_PACKET_LENGTH(packet), ascii_len;
  char *ascii_string =
    u8_write_base64(packet_data,packet_len,&ascii_len);
  if (ascii_string) {
    if (KNO_TRUEP(nopad)) {
      char *scan = ascii_string+(ascii_len-1);
      while (*scan=='=') {*scan='\0'; scan--; ascii_len--;}}
    if (KNO_TRUEP(urisafe)) {
      char *scan = ascii_string, *limit = ascii_string+ascii_len;
      while (scan<limit)  {
	if (*scan=='+') *scan++='-';
	else if (*scan=='/') *scan++='_';
	else scan++;}}
    return kno_init_string(NULL,ascii_len,ascii_string);}
  else return KNO_ERROR;
}

DEFC_PRIM("->base64",any_to_base64_prim,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
	  "converts a string or packet into a string "
	  "containing it's BASE64 representation.",
	  {"arg",kno_any_type,KNO_VOID},
	  {"nopad",kno_any_type,KNO_VOID},
	  {"urisafe",kno_any_type,KNO_VOID})
static lispval any_to_base64_prim(lispval arg,lispval nopad,
				  lispval urisafe)
{
  ssize_t data_len, ascii_len;
  const u8_byte *data; char *ascii_string;
  if (PACKETP(arg)) {
    data = KNO_PACKET_DATA(arg);
    data_len = KNO_PACKET_LENGTH(arg);}
  else if ( (STRINGP(arg)) || (TYPEP(arg,kno_secret_type)) ) {
    data = CSTRING(arg);
    data_len = STRLEN(arg);}
  else return kno_type_error("packet or string","any_to_base64_prim",arg);
  ascii_string = u8_write_base64(data,data_len,&ascii_len);
  if (ascii_string) {
    if (KNO_TRUEP(nopad)) {
      char *scan = ascii_string+(ascii_len-1);
      while (*scan=='=') {*scan='\0'; scan--; ascii_len--;}}
    if (KNO_TRUEP(urisafe)) {
      char *scan = ascii_string, *limit = ascii_string+ascii_len;
      while (scan<limit)  {
	if (*scan=='+') *scan++='-';
	else if (*scan=='/') *scan++='_';
	else scan++;}}
    return kno_init_string(NULL,ascii_len,ascii_string);}
  else return KNO_ERROR;
}

/* Base 16 stuff */

DEFC_PRIM("base16->packet",from_base16_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "converts the hex encoding in string into a data "
	  "packet",
	  {"string",kno_string_type,KNO_VOID})
static lispval from_base16_prim(lispval string)
{
  const u8_byte *string_data = CSTRING(string);
  ssize_t string_len = STRLEN(string), data_len;
  unsigned char *data = u8_read_base16(string_data,string_len,&data_len);
  if (data)
    return kno_init_packet(NULL,data_len,data);
  else return KNO_ERROR;
}

DEFC_PRIM("packet->base16",to_base16_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "converts the data packet *packet* into a "
	  "hexadecimal string.",
	  {"packet",kno_packet_type,KNO_VOID})
static lispval to_base16_prim(lispval packet)
{
  const u8_byte *packet_data = KNO_PACKET_DATA(packet);
  unsigned int packet_len = KNO_PACKET_LENGTH(packet);
  char *ascii_string = u8_write_base16(packet_data,packet_len);
  if (ascii_string)
    return kno_init_string(NULL,packet_len*2,ascii_string);
  else return KNO_ERROR;
}

/* Making zipfiles */

static int string_isasciip(const unsigned char *data,int len)
{
  const unsigned char *scan = data, *limit = scan+len;
  while (scan<limit)
    if (*scan>127) return 0;
    else scan++;
  return 1;
}

#define FDPP_FASCII 1
#define FDPP_FPART 2
#define FDPP_FEXTRA 4
#define FDPP_FNAME 8
#define FDPP_FCOMMENT 16

DEFC_PRIM("gzip",gzip_prim,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
	  "GZIP encodes the string or packet *arg*. If "
	  "*file* is provided the compressed data is written "
	  "to it; otherwise, the compressed data is returned "
	  "as a packet. When provided *comment* (also a "
	  "string) is added to the compressed content",
	  {"arg",kno_any_type,KNO_VOID},
	  {"filename",kno_string_type,KNO_VOID},
	  {"comment",kno_string_type,KNO_VOID})
static lispval gzip_prim(lispval arg,lispval filename,lispval comment)
{
  if (!((STRINGP(arg)||PACKETP(arg))))
    return kno_type_error("string or packet","x2zipfile_prim",arg);
  else {
    u8_condition error = NULL;
    const unsigned char *data=
      ((STRINGP(arg))?(CSTRING(arg)):(KNO_PACKET_DATA(arg)));
    ssize_t data_len=
      ((STRINGP(arg))?(STRLEN(arg)):(KNO_PACKET_LENGTH(arg)));
    struct KNO_OUTBUF out = { 0 };
    int flags = 0; /* FDPP_FHCRC */
    time_t now = time(NULL); u8_int4 crc, intval;
    KNO_INIT_BYTE_OUTPUT(&out,1024); memset(out.buffer,0,1024);
    kno_write_byte(&out,31); kno_write_byte(&out,139);
    kno_write_byte(&out,8); /* Using default */
    /* Compute flags */
    if ((STRINGP(arg))&&(string_isasciip(CSTRING(arg),STRLEN(arg))))
      flags = flags|FDPP_FASCII;
    if (STRINGP(filename)) flags = flags|FDPP_FNAME;
    if (STRINGP(comment)) flags = flags|FDPP_FCOMMENT;
    kno_write_byte(&out,flags);
    intval = kno_flip_word((unsigned int)now);
    kno_write_4bytes(&out,intval);
    kno_write_byte(&out,2); /* Max compression */
    kno_write_byte(&out,3); /* Assume Unix */
    /* No extra fields */
    if (STRINGP(filename)) {
      u8_string text = CSTRING(filename), end = text+STRLEN(filename);
      ssize_t len;
      unsigned char *string=
	u8_localize(latin1_encoding,&text,end,'\\',0,NULL,&len);
      kno_write_bytes(&out,string,len); kno_write_byte(&out,'\0');
      u8_free(string);}
    if (STRINGP(comment)) {
      ssize_t len;
      u8_string text = CSTRING(comment), end = text+STRLEN(comment);
      unsigned char *string=
	u8_localize(latin1_encoding,&text,end,'\\',0,NULL,&len);
      kno_write_bytes(&out,string,len); kno_write_byte(&out,'\0');
      u8_free(string);}
    /*
      crc = u8_crc32(0,(void *)out.start,out.ptr-out.start);
      kno_write_byte(&out,((crc)&(0xFF)));
      kno_write_byte(&out,((crc>>8)&(0xFF)));
    */
    {
      int zerror;
      unsigned long dsize = data_len, csize, csize_max;
      Bytef *dbuf = (Bytef *)data, *cbuf;
      csize = csize_max = dsize+(dsize/1000)+13;
      cbuf = u8_malloc(csize_max); memset(cbuf,0,csize);
      while ((zerror = compress2(cbuf,&csize,dbuf,dsize,9)) < Z_OK)
	if (zerror == Z_MEM_ERROR) {
	  error=_("ZLIB ran out of memory"); break;}
	else if (zerror == Z_BUF_ERROR) {
	  /* We don't use realloc because there's not point in copying
	     the data and we hope the overhead of free/malloc beats
	     realloc when we're doubling the buffer size. */
	  u8_free(cbuf);
	  cbuf = u8_malloc(csize_max*2);
	  if (cbuf == NULL) {
	    error=_("OIDPOOL compress ran out of memory"); break;}
	  csize = csize_max = csize_max*2;}
	else if (zerror == Z_DATA_ERROR) {
	  error=_("ZLIB compress data error"); break;}
	else {
	  error=_("Bad ZLIB return code"); break;}
      if (error == NULL) {
	kno_write_bytes(&out,cbuf+2,csize-6);}
      u8_free(cbuf);}
    if (error) {
      kno_seterr(error,"x2zipfile",NULL,VOID);
      kno_close_outbuf(&out);
      return KNO_ERROR;}
    crc = u8_crc32(0,data,data_len);
    intval = kno_flip_word(crc); kno_write_4bytes(&out,intval);
    intval = kno_flip_word(data_len); kno_write_4bytes(&out,intval);
    if ( (BUFIO_ALLOC(&out)) == KNO_HEAP_BUFFER )
      return kno_init_packet(NULL,out.bufwrite-out.buffer,out.buffer);
    else {
      lispval packet = kno_make_packet(NULL,out.bufwrite-out.buffer,out.buffer);
      kno_close_outbuf(&out);
      return packet;}}
}

DEFC_PRIM("compress",compress_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "Compresses *data* into a packet using *method*.",
	  {"arg",kno_any_type,KNO_VOID},
	  {"method",kno_symbol_type,KNO_VOID})
static lispval compress_prim(lispval arg,lispval method)
{
  if (!((STRINGP(arg)||PACKETP(arg))))
    return kno_type_error("string or packet","x2zipfile_prim",arg);
  kno_compress_type ctype = (KNO_VOIDP(method)) ? (KNO_ZSTD9) :
    (kno_compression_type(method,KNO_BADTYPE));
  if (ctype==KNO_BADTYPE)
    return kno_err("BadCompressionType","compress_prim",
		   KNO_SYMBOL_NAME(method),KNO_VOID);
  else {
    const unsigned char *data=
      ((STRINGP(arg))?(CSTRING(arg)):(KNO_PACKET_DATA(arg)));
    ssize_t data_len=
      ((STRINGP(arg))?(STRLEN(arg)):(KNO_PACKET_LENGTH(arg)));
    ssize_t compressed_len = -1;
    unsigned char *compressed = kno_compress
      (ctype,&compressed_len,data,data_len,NULL);
    if (compressed) {
      lispval vec = kno_make_packet(NULL,compressed_len,compressed);
      u8_big_free(compressed);
      return vec;}
    else return KNO_ERROR;}
}

DEFC_PRIM("uncompress",uncompress_prim,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "Uncompresses *data* from packet *method*. If "
	  "*encoding* is specified, attempts to convert the "
	  "result into a string using the designated "
	  "encoding.",
	  {"arg",kno_packet_type,KNO_VOID},
	  {"method",kno_symbol_type,KNO_VOID},
	  {"encoding",kno_any_type,KNO_FALSE})
static lispval uncompress_prim(lispval arg,lispval method,
			       lispval encoding)
{
  kno_compress_type ctype = (KNO_VOIDP(method)) ? (KNO_ZSTD9) :
    (kno_compression_type(method,KNO_BADTYPE));
  if (ctype==KNO_BADTYPE)
    return kno_err("BadCompressionType","compress_prim",
		   KNO_SYMBOL_NAME(method),KNO_VOID);
  else {
    int return_packet = (FALSEP(encoding)) ||
      (DEFAULTP(encoding)) || (VOIDP(encoding));
    struct U8_TEXT_ENCODING *enc = (return_packet) ? (NULL) :
      (KNO_TRUEP(encoding)) ? (u8_get_encoding("UTF-8")) :
      (STRINGP(encoding)) ? (u8_get_encoding(CSTRING(encoding))) :
      (SYMBOLP(encoding)) ? (u8_get_encoding(SYM_NAME(encoding))) :
      (NULL);
    if ((!return_packet) && (enc==NULL))
      return kno_type_error(_("text encoding"),"uncompress",encoding);
    const unsigned char *data = KNO_PACKET_DATA(arg);
    ssize_t data_len = KNO_PACKET_LENGTH(arg);
    ssize_t uncompressed_len = -1;
    unsigned char *uncompressed = kno_uncompress
      (ctype,&uncompressed_len,data,data_len,NULL);
    if (uncompressed) {
      if (enc == NULL)
	return kno_init_packet(NULL,uncompressed_len,uncompressed);
      struct U8_OUTPUT out;
      const u8_byte *scan = uncompressed;
      const u8_byte *limit = uncompressed+uncompressed_len;
      U8_INIT_OUTPUT(&out,2*uncompressed_len);
      if (u8_convert(enc,0,&out,&scan,limit)<0) {
	lispval packet = kno_make_packet(NULL,uncompressed_len,uncompressed);
	u8_free(out.u8_outbuf);
	u8_big_free(uncompressed);
	return packet;}
      else {
	u8_big_free(uncompressed);
	return kno_stream2string(&out);}}
    else return KNO_ERROR;}
}

/* Zipsource operations */

DEFC_PRIM("zipsource?",zipsourcep_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns true if *arg* is a zipsource.",
	  {"arg",kno_any_type,KNO_VOID})
static lispval zipsourcep_prim(lispval arg)
{
  if (KNO_RAW_TYPEP(arg,KNOSYM_ZIPSOURCE))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("zipsource/exists?",zipsource_existsp_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "Returns *true* if *path* exists in the zipsource *arg*.",
	  {"arg",KNO_ZIPSOURCE_TYPE,KNO_VOID},
	  {"path",kno_string_type,KNO_VOID})
static lispval zipsource_existsp_prim(lispval arg,lispval path)
{
  int rv = kno_zipsource_existsp(arg,KNO_CSTRING(path));
  if (rv<0) return KNO_ERROR;
  else if (rv) return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("zipsource/info",zipsource_info_prim,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "Returns metadata for *path* in the zipsource *arg* or "
	  "#f i the path doesn't exist",
	  {"arg",KNO_ZIPSOURCE_TYPE,KNO_VOID},
	  {"path",kno_string_type,KNO_VOID},
	  {"follow_arg",kno_any_type,KNO_FALSE})
static lispval zipsource_info_prim(lispval arg,lispval path,
				   lispval follow_arg)
{
  int follow = (KNO_TRUEP(follow_arg));
  return kno_zipsource_info(arg,KNO_CSTRING(path),follow);
}

DEFC_PRIM("zipsource/content",zipsource_content_prim,
	  KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
	  "Returns the content/data for *path* in the zipsource. If "
	  "*enc_arg* is defined it is a character encoding for string "
	  "conversion or \"bytes\" to indicate that a data packet should "
	  "be returned. If *follow_arg* is specified, this follows symlinks "
	  "in the zipsource *arg*.",
	  {"arg",KNO_ZIPSOURCE_TYPE,KNO_VOID},
	  {"path",kno_string_type,KNO_VOID},
	  {"enc_arg",kno_any_type,KNO_VOID},
	  {"follow_arg",kno_any_type,KNO_TRUE})
static lispval zipsource_content_prim(lispval arg,lispval path,
				      lispval enc_arg,
				      lispval follow_arg)
{
  u8_string enc_name = NULL;
  int follow = (!(KNO_FALSEP(follow_arg)));
  if ( (KNO_VOIDP(enc_arg)) || (KNO_FALSEP(enc_arg)) || (KNO_DEFAULTP(enc_arg)) )
    enc_name = "auto";
  else if (KNO_STRINGP(enc_arg))
    enc_name = KNO_CSTRING(enc_arg);
  else if (KNO_TRUEP(enc_arg))
    enc_name = "bytes";
  else if (KNO_SYMBOLP(enc_arg))
    enc_name = KNO_SYMBOL_NAME(enc_arg);
  else return kno_err("BadEncodingArg","zipsource_content_prim",
		      KNO_CSTRING(path),enc_arg);
  return kno_zipsource_content(arg,KNO_CSTRING(path),enc_name,follow);
}

DEFC_PRIM("zipsource/get",zipsource_get_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns true if *arg* is a zipsource.",
	  {"path",kno_string_type,KNO_VOID})
static lispval zipsource_get_prim(lispval path)
{
  return kno_get_zipsource(KNO_CSTRING(path));
}

DEFC_PRIM("zipsource/open",zipsource_open_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "Returns true if *arg* is a zipsource.",
	  {"path",kno_string_type,KNO_VOID},
	  {"opts",kno_opts_type,KNO_VOID})
static lispval zipsource_open_prim(lispval path,lispval opts)
{
  return kno_open_zipsource(KNO_CSTRING(path),opts);
}

/* Port type operations */

/* The port type */

static int unparse_port(struct U8_OUTPUT *out,lispval x)
{
  struct KNO_PORT *p = kno_consptr(kno_port,x,kno_ioport_type);
  if ((p->port_input) && (p->port_output) && (p->port_id))
    u8_printf(out,"#<I/O Port (%s) #!%x>",p->port_id,x);
  else if ((p->port_input) && (p->port_output))
    u8_printf(out,"#<I/O Port #!%x>",x);
  else if ((p->port_input)&&(p->port_id))
    u8_printf(out,"#<Input Port (%s) #!%x>",p->port_id,x);
  else if (p->port_input)
    u8_printf(out,"#<Input Port #!%x>",x);
  else if (p->port_id)
    u8_printf(out,"#<Output Port (%s) #!%x>",p->port_id,x);
  else u8_printf(out,"#<Output Port #!%x>",x);
  return 1;
}

static void recycle_port(struct KNO_RAW_CONS *c)
{
  struct KNO_PORT *p = (struct KNO_PORT *)c;
  if (p->port_input) {
    u8_close_input(p->port_input);}
  if (p->port_output) {
    u8_close_output(p->port_output);}
  if (p->port_id) u8_free(p->port_id);
  if (p->port_lisprefs != KNO_NULL) kno_decref(p->port_lisprefs);
  if (p->annotations) kno_decref(p->annotations);
  if (KNO_MALLOCD_CONSP(c)) u8_free(c);
}

/* Initializing some symbols */

static void init_portprims_symbols()
{
  column_symbol=kno_intern("column");
  depth_symbol=kno_intern("depth");
  maxcol_symbol=kno_intern("maxcol");
  width_symbol=kno_intern("width");
  margin_symbol=kno_intern("margin");
  maxelts_symbol=kno_intern("maxelts");
  maxchars_symbol=kno_intern("maxchars");
  maxbytes_symbol=kno_intern("maxbytes");
  maxkeys_symbol=kno_intern("maxkeys");
  listmax_symbol=kno_intern("listmax");
  vecmax_symbol=kno_intern("vecmax");
  choicemax_symbol=kno_intern("choicemax");
  label_symbol = kno_intern("label");
  output_symbol = kno_intern("output");
  detail_symbol = kno_intern("detail");
  refs_symbol = kno_intern("refs");
  nrefs_symbol = kno_intern("nrefs");
  lookup_symbol = kno_intern("lookup");
  embed_symbol = kno_intern("embed");
}

/* The init function */

KNO_EXPORT void kno_init_portprims_c()
{
  u8_register_source_file(_FILEINFO);

  kno_register_tag_type(KNOSYM_ZIPSOURCE,KNO_ZIPSOURCE_TYPE);

  kno_unparsers[kno_ioport_type]=unparse_port;
  kno_recyclers[kno_ioport_type]=recycle_port;

  kno_tablefns[kno_ioport_type]=kno_annotated_tablefns;

  init_portprims_symbols();
  link_local_cprims();

  KNO_LINK_EVALFN(kno_textio_module,printout_to_evalfn);
  KNO_LINK_EVALFN(kno_textio_module,printout_evalfn);
  KNO_LINK_EVALFN(kno_textio_module,lineout_evalfn);
  KNO_LINK_EVALFN(kno_textio_module,stringout_evalfn);
  KNO_LINK_EVALFN(kno_textio_module,indentout_evalfn);
}

static void link_local_cprims()
{
  KNO_LINK_CPRIMN("pprinter",lisp_pprinter,kno_textio_module);
  KNO_LINK_CPRIMN("pprint",lisp_pprint,kno_textio_module);
  KNO_LINK_CPRIMN("$pprint",lisp_4pprint,kno_textio_module);
  KNO_LINK_CPRIM("read-record",read_record_prim,3,kno_textio_module);
  KNO_LINK_CPRIM("read",read_prim,1,kno_textio_module);
  KNO_LINK_CPRIM("getline",getline_prim,4,kno_textio_module);
  KNO_LINK_CPRIM("getchar",getchar_prim,1,kno_textio_module);
  KNO_LINK_CPRIM("$histval",histval_prim,2,kno_textio_module);
  KNO_LINK_CPRIM("$histref",histref_prim,2,kno_textio_module);
  KNO_LINK_CPRIM("$histstring",histstring_prim,2,kno_textio_module);
  KNO_LINK_CPRIM("unescape-string",unescape_string_prim,1,kno_textio_module);
  KNO_LINK_CPRIM("uniscape",uniscape,2,kno_textio_module);
  KNO_LINK_CPRIM("escape-string",escape_string_prim,3,kno_textio_module);
  KNO_LINK_CPRIM("escapeout",escapeout_prim,3,kno_textio_module);
  KNO_LINK_CPRIM("substringout",substringout,3,kno_textio_module);
  KNO_LINK_CPRIM("newline",newline_prim,1,kno_textio_module);
  KNO_LINK_CPRIM("putchar",putchar_prim,2,kno_textio_module);
  KNO_LINK_CPRIM("display",display_prim,2,kno_textio_module);
  KNO_LINK_CPRIM("write",write_prim,2,kno_textio_module);
  KNO_LINK_CPRIM("portdata",portdata_prim,1,kno_textio_module);
  KNO_LINK_CPRIM("portid",portid_prim,1,kno_textio_module);
  KNO_LINK_CPRIM("open-input-string",open_input_string,1,kno_textio_module);
  KNO_LINK_CPRIM("open-output-string",open_output_string,0,kno_textio_module);
  KNO_LINK_CPRIM("eof-object?",eofp,1,kno_textio_module);
  KNO_LINK_CPRIM("port?",portp,1,kno_textio_module);
  KNO_LINK_CPRIM("input-port?",input_portp,1,kno_textio_module);
  KNO_LINK_CPRIM("output-port?",output_portp,1,kno_textio_module);

  KNO_LINK_CPRIM("dtype->packet",lisp2packet,2,kno_textio_module);
  KNO_LINK_CPRIM("packet->dtype",packet2dtype,1,kno_textio_module);
  KNO_LINK_CPRIM("encode-xtype",encode_xtype,2,kno_textio_module);
  KNO_LINK_CPRIM("decode-xtype",decode_xtype,2,kno_textio_module);
  KNO_LINK_CPRIM("precode-xtype",precode_xtype,2,kno_textio_module);
  KNO_LINK_CPRIM("xtype/refs",make_xtype_refs,2,kno_textio_module);
  KNO_LINK_CPRIM("xtype/refs/encode",xtype_refs_encode,3,kno_textio_module);
  KNO_LINK_CPRIM("xtype/refs/decode",xtype_refs_decode,2,kno_textio_module);
  KNO_LINK_CPRIM("xtype/refs/count",xtype_refs_count,1,kno_textio_module);

  KNO_LINK_CPRIM("packet->base16",to_base16_prim,1,kno_textio_module);
  KNO_LINK_CPRIM("base16->packet",from_base16_prim,1,kno_textio_module);
  KNO_LINK_CPRIM("->base64",any_to_base64_prim,3,kno_textio_module);
  KNO_LINK_CPRIM("packet->base64",to_base64_prim,3,kno_textio_module);
  KNO_LINK_CPRIM("base64->packet",from_base64_prim,1,kno_textio_module);
  KNO_LINK_CPRIM("listdata",lisp_listdata,3,kno_textio_module);

  KNO_LINK_CPRIM("gzip",gzip_prim,3,kno_textio_module);
  KNO_LINK_CPRIM("compress",compress_prim,2,kno_textio_module);
  KNO_LINK_CPRIM("uncompress",uncompress_prim,3,kno_textio_module);

  KNO_LINK_CPRIM("dumpstack",dumpstack_prim,1,kno_textio_module);
  KNO_LINK_CPRIM("dumpstack/full",dumpstack_full_prim,1,kno_textio_module);

  KNO_LINK_CPRIM("zipsource?",zipsourcep_prim,1,kno_textio_module);
  KNO_LINK_CPRIM("zipsource/exists?",zipsource_existsp_prim,2,kno_textio_module);
  KNO_LINK_CPRIM("zipsource/info",zipsource_info_prim,3,kno_textio_module);
  KNO_LINK_CPRIM("zipsource/content",zipsource_content_prim,4,kno_textio_module);
  KNO_LINK_CPRIM("->zipsource",zipsource_get_prim,1,kno_textio_module);
  KNO_LINK_CPRIM("zipsource/open",zipsource_open_prim,2,kno_textio_module);
  KNO_LINK_ALIAS("zipsource/get",zipsource_content_prim,kno_textio_module);

  KNO_LINK_ALIAS("eof?",eofp,kno_textio_module);
  KNO_LINK_ALIAS("write-char",putchar_prim,kno_textio_module);
  KNO_LINK_ALIAS("decode-string",unescape_string_prim,kno_textio_module);

}
