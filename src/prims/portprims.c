/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_PROVIDE_FASTEVAL 1

#include "kno/knosource.h"
#include "kno/dtype.h"
#include "kno/eval.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/frames.h"
#include "kno/streams.h"
#include "kno/dtypeio.h"
#include "kno/ports.h"
#include "kno/pprint.h"

#include "kno/cprims.h"

#include <libu8/u8streamio.h>
#include <libu8/u8crypto.h>

#include <zlib.h>

u8_condition kno_UnknownEncoding=_("Unknown encoding");

#define fast_eval(x,env) (kno_stack_eval(x,env,_stack,0))

/* Making ports */

KNO_EXPORT lispval kno_make_port(U8_INPUT *in,U8_OUTPUT *out,u8_string id)
{
  struct KNO_PORT *port = u8_alloc(struct KNO_PORT);
  KNO_INIT_CONS(port,kno_port_type);
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
      kno_consptr(struct KNO_PORT *,portarg,kno_port_type);
    return p->port_output;}
  else return NULL;
}

static u8_input get_input_port(lispval portarg)
{
  if (VOIDP(portarg))
    return NULL; /* get_default_output(); */
  else if (KNO_PORTP(portarg)) {
    struct KNO_PORT *p=
      kno_consptr(struct KNO_PORT *,portarg,kno_port_type);
    return p->port_input;}
  else return NULL;
}

KNO_DEFPRIM("PORT?",portp,KNO_MAX_ARGS(1),
            "`(PORT? *object*)` returns #t if *object* is an i/o port.")
  (lispval arg)
{
  if (KNO_PORTP(arg))
    return KNO_TRUE;
  else return KNO_FALSE;
}

KNO_DEFPRIM("INPUT-PORT?",input_portp,KNO_MAX_ARGS(1),
            "`(INPUT-PORT? *object*)` returns #t "
            "if *object* is an input port.")
  (lispval arg)
{
  if (KNO_PORTP(arg)) {
    struct KNO_PORT *p=
      kno_consptr(struct KNO_PORT *,arg,kno_port_type);
    if (p->port_input)
      return KNO_TRUE;
    else return KNO_FALSE;}
  else return KNO_FALSE;
}

KNO_DEFPRIM("OUTPUT-PORT?",output_portp,MAX_ARGS(1),
           "`(OUTPUT-PORT? *object*)` returns #t "
           "if *object* is an output port.")
  (lispval arg)
{
  if (KNO_PORTP(arg)) {
    struct KNO_PORT *p=
      kno_consptr(struct KNO_PORT *,arg,kno_port_type);
    if (p->port_output)
      return KNO_TRUE;
    else return KNO_FALSE;}
  else return KNO_FALSE;
}

/* Identifying end of file */

KNO_DCLPRIM("EOF-OBJECT?",eofp,MAX_ARGS(1),
           "`(EOF-OBJECT? *object*)` returns #t "
           "if *object* is an end of file indicators.")
static lispval eofp (lispval x)
{
  if (KNO_EOFP(x)) return KNO_TRUE; else return KNO_FALSE;
}

/* DTYPE streams */

KNO_DCLPRIM1("PACKET->DTYPE",packet2dtype,MAX_ARGS(1),
            "`(PACKET->DTYPE *packet*)` parses the DType "
            "representation in *packet* and returns the "
            "corresponding object.",
            kno_packet_type,KNO_VOID)
static lispval packet2dtype(lispval packet)
{
  lispval object;
  struct KNO_INBUF in = { 0 };
  KNO_INIT_BYTE_INPUT(&in,KNO_PACKET_DATA(packet),
                     KNO_PACKET_LENGTH(packet));
  object = kno_read_dtype(&in);
  return object;
}

KNO_DCLPRIM2("DTYPE->PACKET",lisp2packet,MIN_ARGS(1),
            "`(DTYPE->PACKET *object* [*bufsize*])` returns a packet "
            "containing the DType representation of object. "
            "*bufsize*, if provided, specifies the initial size "
            "of the output buffer to be reserved.",
            -1,KNO_VOID,kno_fixnum_type,KNO_VOID)
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

/* Output strings */

DCLPRIM("OPEN-OUTPUT-STRING",open_output_string,MAX_ARGS(0),
        "`(OPEN-OUTPUT-STRING)` returns an output string stream")
static lispval open_output_string()
{
  U8_OUTPUT *out = u8_alloc(struct U8_OUTPUT);
  U8_INIT_OUTPUT(out,256);
  return kno_make_port(NULL,out,u8_strdup("output string"));
}

DCLPRIM1("OPEN-INPUT-STRING",open_input_string,MIN_ARGS(1),
         "`(OPEN-INPUT-STRING *string*)` returns an input stream "
         "reading from *string*.",
         kno_string_type,KNO_VOID)
static lispval open_input_string(lispval arg)
{
  if (STRINGP(arg)) {
    U8_INPUT *in = u8_alloc(struct U8_INPUT);
    U8_INIT_STRING_INPUT(in,KNO_STRING_LENGTH(arg),u8_strdup(CSTRING(arg)));
    in->u8_streaminfo = in->u8_streaminfo|U8_STREAM_OWNS_BUF;
    return kno_make_port(in,NULL,u8_strdup("input string"));}
  else return kno_type_error(_("string"),"open_input_string",arg);
}

DCLPRIM("PORTID",portid_prim,MAX_ARGS(1),
        "`(PORTID *port*)` returns the id string (if any) "
        "for *port*.")
static lispval portid_prim(lispval port_arg)
{
  if (KNO_PORTP(port_arg)) {
    struct KNO_PORT *port = (struct KNO_PORT *)port_arg;
    if (port->port_id)
      return lispval_string(port->port_id);
    else return KNO_FALSE;}
  else return kno_type_error(_("port"),"portid",port_arg);
}

DCLPRIM("PORTDATA",portdata_prim,MAX_ARGS(1),
        "`(PORTDATA *port*)` returns the buffered data for *port*. "
        "If *port* is a string stream, this is the output to date "
        "to the port.")
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

DCLPRIM("WRITE",write_prim,MAX_ARGS(2)|MIN_ARGS(1),
        "`(WRITE *object* [*port*])` writes a textual represenntation "
        "of *object* to *port*. The implementation strives to "
        "make `READ` be able to convert the output of `WRITE` to "
        "`EQUAL?` objects.\n"
        "If *port* is #t or not provided, the current output, "
        "which is usually the stdout, is used. Otherwise, it "
        "must be an output port.")
static lispval write_prim(lispval x,lispval portarg)
{
  U8_OUTPUT *out = get_output_port(portarg);
  if (out) {
    kno_unparse(out,x);
    u8_flush(out);
    return VOID;}
  else return kno_type_error(_("output port"),"write_prim",portarg);
}

DCLPRIM("DISPLAY",display_prim,MAX_ARGS(2)|MIN_ARGS(1),
        "`(DISPLAY *object* [*port*])` writes a textual represenntation "
        "of *object* to *port*. This makes no special attempts "
        "to make it's output parsable by `READ`\n"
        "If *port* is #t or not provided, the current output, "
        "which is usually the stdout, is used. Otherwise, it "
        "must be an output port.")
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

DCLPRIM("PUTCHAR",putchar_prim,MAX_ARGS(2)|MIN_ARGS(1),
         "`(PUTCHAR *char* [*port*])` the character *char* to *port*. "
        "*char* must be either a character object or a positive "
        "integer corresponding to a Unicode code point.\n"
        "If *port* is #t or not provided, the current default output, "
        "is used. Otherwise, it must be an output port.")
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

DCLPRIM("NEWLINE",newline_prim,MAX_ARGS(1)|MIN_ARGS(0),
        "`(NEWLINE [*port*])` emits a newline to *port*. "
        "If *port* is #t or not provided, the current output, "
        "which is usually the stdout, is used. Otherwise, it "
        "must be an output port.")
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
  if (KNO_ABORTP(x)) return 0;
  else if (VOIDP(x)) return 1;
  if (out == NULL) out = u8_current_output;
  if (STRINGP(x))
    u8_puts(out,CSTRING(x));
  else kno_unparse(out,x);
  return 1;
}

KNO_EXPORT
lispval kno_printout(lispval body,kno_lexenv env)
{
  struct KNO_STACK *_stack=kno_stackptr;
  U8_OUTPUT *out = u8_current_output;
  while (PAIRP(body)) {
    lispval value = fast_eval(KNO_CAR(body),env);
    if (printout_helper(out,value)) kno_decref(value);
    else return value;
    body = KNO_CDR(body);}
  u8_flush(out);
  return VOID;
}

KNO_EXPORT
lispval kno_printout_to(U8_OUTPUT *out,lispval body,kno_lexenv env)
{
  struct KNO_STACK *_stack=kno_stackptr;
  u8_output prev = u8_current_output;
  u8_set_default_output(out);
  while (PAIRP(body)) {
    lispval value = fast_eval(KNO_CAR(body),env);
    if (printout_helper(out,value)) kno_decref(value);
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

DCLPRIM3("SUBSTRINGOUT",substringout,MIN_ARGS(1),
         "`(SUBSTRINGOUT *string* *start* *end*)` emits a substring of "
         "*string* to the default output.",
         kno_string_type,VOID,
         kno_fixnum_type,VOID,
         kno_fixnum_type,VOID)
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

DCLPRIM2("UNISCAPE",uniscape,MIN_ARGS(1),
         "`(UNISCAPE *string* [*except_string*])` emits a unicode escaped "
         "version of *string* to the default output. All non-ascii "
         "characters except for those in *except_string* are encoded as "
         "\\uXXXX escape sequences",
         kno_string_type,VOID,
         kno_string_type,VOID)
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

static lispval printout_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  return kno_printout(kno_get_body(expr,1),env);
}
static lispval lineout_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  U8_OUTPUT *out = u8_current_output;
  lispval value = kno_printout(kno_get_body(expr,1),env);
  if (KNO_ABORTP(value)) return value;
  u8_putc(out,'\n');
  u8_flush(out);
  return VOID;
}

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

/* Input operations! */

DCLPRIM("GETCHAR",getchar_prim,MAX_ARGS(1)|MIN_ARGS(0),
         "`(GETCHAR [*port*])` reads a single character from *port*. "
        "If *port* is #t or not provided, the current default input, "
        "is used. Otherwise, it must be an input port.")
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

DCLPRIM("GETLINE",getline_prim,MAX_ARGS(4)|MIN_ARGS(0),
         "`(GETLINE [*port*] [*eol*] [*maxchars*] [*eof*])` "
        "reads a single 'line' from *port* as a string. "
        "If *port* is #t or not provided, the current default "
        "input is used. Otherwise, it must be an input port.\n"
        "* *eol* is the string that indicates the line end, defaulting "
        "to a single newline; this sequence is consumed but not "
        "included in the returned string;\n"
        "* *maxchars* indicates the maximum number of characters "
        "to read while waiting for *eol*;\n"
        "* *eof* an \"end of file\" sequence which causes "
        "`GETLINE` to return #eof when encountered.")
static lispval getline_prim(lispval port,lispval eos_arg,
                            lispval lim_arg,
                            lispval eof_marker)
{
  U8_INPUT *in = get_input_port(port);
  if (VOIDP(eof_marker)) eof_marker = EMPTY;
  else if (KNO_TRUEP(eof_marker)) eof_marker = KNO_EOF;
  else {}
  if (in) {
    u8_string data, eos;
    int lim, size = 0;
    if (in == NULL)
      return kno_type_error(_("input port"),"getline_prim",port);
    if (VOIDP(eos_arg)) eos="\n";
    else if (STRINGP(eos_arg)) eos = CSTRING(eos_arg);
    else return kno_type_error(_("string"),"getline_prim",eos_arg);
    if (VOIDP(lim_arg)) lim = 0;
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

DCLPRIM("READ",read_prim,MAX_ARGS(4)|MIN_ARGS(0),
         "`(READ [*port*])` "
        "reads an object from *port*. "
        "If *port* is #t or not provided, the current default "
        "input is used. Otherwise, it must be an input port.")
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

DCLPRIM3("READ-RECORD",read_record_prim,MIN_ARGS(1)|NDCALL,
         "`(READ-RECORD *ports* [*separator*] [*limit*])`",
         -1,KNO_VOID,-1,KNO_VOID,-1,KNO_VOID)
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

/* PPRINT lisp primitives */

static lispval column_symbol, depth_symbol, detail_symbol;
static lispval maxcol_symbol, width_symbol, margin_symbol;
static lispval maxelts_symbol, maxchars_symbol, maxbytes_symbol;
static lispval maxkeys_symbol, listmax_symbol, vecmax_symbol, choicemax_symbol;

#define PPRINT_MARGINBUF_SIZE 256

DCLPRIM("PPRINT",lisp_pprint,MIN_ARGS(1)|NDCALL|KNO_VAR_ARGS,
        "(pprint *object* *port* *width* *margin*)\n"
        "Generates a formatted representation of *object* "
        "on *port* () with a width of *width* columns with a "
        "left margin of *margin* which is either number of columns "
        "or a string.")
static lispval lisp_pprint(int n,lispval *args)
{
  U8_OUTPUT *out=NULL;
  struct U8_OUTPUT tmpout;
  struct PPRINT_CONTEXT ppcxt={0};
  int arg_i=0, used[7]={0};
  int indent=0, close_port=0, stringout=0, col=0, depth=0;
  lispval opts = VOID, port_arg=VOID, obj = args[0]; used[0]=1;
  if (n>7) return kno_err(kno_TooManyArgs,"lisp_pprint",NULL,VOID);
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
    lispval port = kno_getopt(opts,FDSYM_OUTPUT,VOID);
    if (!(VOIDP(port))) out = get_output_port(port);
    if (out)
      port_arg=port;
    else {kno_decref(port);}}
  if (out == NULL) {
    lispval filename = kno_getopt(opts,FDSYM_FILENAME,VOID);
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

DCLPRIM3("LISTDATA",lisp_listdata,MIN_ARGS(1)|NDCALL,
         "`(LISTDATA *object* [*opts*] [*port*])` output "
         "a formatted textual representation of *object* to *port*, "
         "controlled by *opts*.",
         -1,KNO_VOID,-1,KNO_VOID,-1,KNO_VOID)
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

DCLPRIM1("BASE64->PACKET",from_base64_prim,0,
          "`(BASE64->PACKET *string*)` converts the BASE64 encoding "
          "in string into a data packet",
          kno_string_type,KNO_VOID)
static lispval from_base64_prim(lispval string)
{
  const u8_byte *string_data = CSTRING(string);
  unsigned int string_len = STRLEN(string), data_len;
  unsigned char *data=
    u8_read_base64(string_data,string_data+string_len,&data_len);
  if (data)
    return kno_init_packet(NULL,data_len,data);
  else return KNO_ERROR;
}

DCLPRIM3("PACKET->BASE64",to_base64_prim,MIN_ARGS(1),
          "`(PACKET->BASE64 *packet* [*nopad*] [*foruri*])` "
          "converts a packet into a string containing it's "
          "BASE64 representation.",
          kno_packet_type,KNO_VOID,-1,KNO_VOID,-1,KNO_VOID)
static lispval to_base64_prim(lispval packet,lispval nopad,
                              lispval urisafe)
{
  const u8_byte *packet_data = KNO_PACKET_DATA(packet);
  unsigned int packet_len = KNO_PACKET_LENGTH(packet), ascii_len;
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

DCLPRIM3("->BASE64",any_to_base64_prim,MIN_ARGS(1),
          "`(->BASE64 *packet* [*nopad*] [*foruri*])` "
          "converts a string or packet into a string containing it's "
          "BASE64 representation.",
          -1,KNO_VOID,-1,KNO_VOID,-1,KNO_VOID)
static lispval any_to_base64_prim(lispval arg,lispval nopad,
                                  lispval urisafe)
{
  unsigned int data_len, ascii_len;
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

DCLPRIM1("BASE16->PACKET",from_base16_prim,0,
          "`(BASE16->PACKET *string*)` converts the hex encoding "
          "in string into a data packet",
          kno_string_type,KNO_VOID)
static lispval from_base16_prim(lispval string)
{
  const u8_byte *string_data = CSTRING(string);
  unsigned int string_len = STRLEN(string), data_len;
  unsigned char *data = u8_read_base16(string_data,string_len,&data_len);
  if (data)
    return kno_init_packet(NULL,data_len,data);
  else return KNO_ERROR;
}

DCLPRIM1("PACKET->BASE16",to_base16_prim,0,
          "`(PACKET->BASE16 *packet*)` converts "
          "the data packet *packet* into a hexadecimal string.",
          kno_packet_type,KNO_VOID)
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

DCLPRIM3("GZIP",gzip_prim,MIN_ARGS(1),
         "`(GZIP *data* [*file*] [*comment*])` GZIP encodes "
         "the string or packet *arg*. If *file* is provided "
         "the compressed data is written to it; otherwise, "
         "the compressed data is returned as a packet. "
         "When provided *comment* (also a string) is added to "
         "the compressed content",
         -1,VOID,
         kno_string_type,VOID,
         kno_string_type,VOID)
static lispval gzip_prim(lispval arg,lispval filename,lispval comment)
{
  if (!((STRINGP(arg)||PACKETP(arg))))
    return kno_type_error("string or packet","x2zipfile_prim",arg);
  else {
    u8_condition error = NULL;
    const unsigned char *data=
      ((STRINGP(arg))?(CSTRING(arg)):(KNO_PACKET_DATA(arg)));
    unsigned int data_len=
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
      int len;
      unsigned char *string=
        u8_localize(latin1_encoding,&text,end,'\\',0,NULL,&len);
      kno_write_bytes(&out,string,len); kno_write_byte(&out,'\0');
      u8_free(string);}
    if (STRINGP(comment)) {
      int len;
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

/* Port type operations */

/* The port type */

static int unparse_port(struct U8_OUTPUT *out,lispval x)
{
  struct KNO_PORT *p = kno_consptr(kno_port,x,kno_port_type);
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
 if (KNO_MALLOCD_CONSP(c)) u8_free(c);
}

/* Initializing some symbols */

static void init_portprims_symbols()
{
  column_symbol=kno_intern("COLUMN");
  depth_symbol=kno_intern("DEPTH");
  maxcol_symbol=kno_intern("MAXCOL");
  width_symbol=kno_intern("WIDTH");
  margin_symbol=kno_intern("MARGIN");
  maxelts_symbol=kno_intern("MAXELTS");
  maxchars_symbol=kno_intern("MAXCHARS");
  maxbytes_symbol=kno_intern("MAXBYTES");
  maxkeys_symbol=kno_intern("MAXKEYS");
  listmax_symbol=kno_intern("LISTMAX");
  vecmax_symbol=kno_intern("VECMAX");
  choicemax_symbol=kno_intern("CHOICEMAX");
  label_symbol = kno_intern("LABEL");
  output_symbol = kno_intern("OUTPUT");
  detail_symbol = kno_intern("DETAIL");
}

/* The init function */

KNO_EXPORT void kno_init_portprims_c()
{
  u8_register_source_file(_FILEINFO);

  kno_unparsers[kno_port_type]=unparse_port;
  kno_recyclers[kno_port_type]=recycle_port;

  init_portprims_symbols();

  DECL_PRIM(eofp,1,kno_scheme_module);
  DECL_ALIAS("EOF?",eofp,kno_scheme_module);
  DECL_PRIM(portp,1,kno_scheme_module);
  DECL_PRIM(input_portp,1,kno_scheme_module);
  DECL_PRIM(output_portp,1,kno_scheme_module);
  DECL_PRIM(packet2dtype,1,kno_scheme_module);
  DECL_PRIM(lisp2packet,2,kno_scheme_module);

  DECL_PRIM(open_output_string,0,kno_scheme_module);
  DECL_PRIM(open_input_string,1,kno_scheme_module);

  int one_port_arg[1] = { kno_port_type };
  DECL_PRIM_ARGS(portid_prim,1,kno_scheme_module,
                 one_port_arg,NULL);
  DECL_PRIM_ARGS(portdata_prim,1,kno_scheme_module,
                 one_port_arg,NULL);

  DECL_PRIM(write_prim,2,kno_scheme_module);
  DECL_PRIM(display_prim,2,kno_scheme_module);
  DECL_PRIM(putchar_prim,2,kno_scheme_module);
  DECL_PRIM(newline_prim,1,kno_scheme_module);
  DECL_ALIAS("WRITE-CHAR",putchar_prim,kno_scheme_module);

  DECL_PRIM(getchar_prim,1,kno_scheme_module);
  DECL_PRIM(read_prim,1,kno_scheme_module);
  DECL_PRIM(getline_prim,4,kno_scheme_module);

  DECL_PRIM_N(lisp_pprint,kno_scheme_module);
  DECL_PRIM(lisp_listdata,3,kno_scheme_module);

  DECL_PRIM(read_record_prim,3,kno_scheme_module);

  DECL_PRIM(substringout,3,kno_scheme_module);
  DECL_PRIM(uniscape,2,kno_scheme_module);

  DECL_PRIM(from_base64_prim,1,kno_scheme_module);
  DECL_PRIM(to_base64_prim,3,kno_scheme_module);
  DECL_PRIM(any_to_base64_prim,3,kno_scheme_module);

  DECL_PRIM(from_base16_prim,1,kno_scheme_module);
  DECL_PRIM(to_base16_prim,1,kno_scheme_module);

  DECL_PRIM(gzip_prim,3,kno_scheme_module);

  kno_def_evalfn(kno_scheme_module,"PRINTOUT","",printout_evalfn);
  kno_def_evalfn(kno_scheme_module,"LINEOUT","",lineout_evalfn);
  kno_def_evalfn(kno_scheme_module,"STRINGOUT","",stringout_evalfn);

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
