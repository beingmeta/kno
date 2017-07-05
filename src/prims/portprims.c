/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_PROVIDE_FASTEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/frames.h"
#include "framerd/streams.h"
#include "framerd/dtypeio.h"
#include "framerd/ports.h"

#include <libu8/u8streamio.h>
#include <libu8/u8crypto.h>

#include <zlib.h>

fd_exception fd_UnknownEncoding=_("Unknown encoding");

#define fast_eval(x,env) (fd_stack_eval(x,env,_stack,0))

/* Making ports */

static lispval make_port(U8_INPUT *in,U8_OUTPUT *out,u8_string id)
{
  struct FD_PORT *port = u8_alloc(struct FD_PORT);
  FD_INIT_CONS(port,fd_port_type);
  port->fd_inport = in; port->fd_outport = out; port->fd_portid = id;
  return LISP_CONS(port);
}

static u8_output get_output_port(lispval portarg)
{
  if ((VOIDP(portarg))||(FD_TRUEP(portarg)))
    return u8_current_output;
  else if (FD_PORTP(portarg)) {
    struct FD_PORT *p=
      fd_consptr(struct FD_PORT *,portarg,fd_port_type);
    return p->fd_outport;}
  else return NULL;
}

static u8_input get_input_port(lispval portarg)
{
  if (VOIDP(portarg))
    return NULL; /* get_default_output(); */
  else if (FD_PORTP(portarg)) {
    struct FD_PORT *p=
      fd_consptr(struct FD_PORT *,portarg,fd_port_type);
    return p->fd_inport;}
  else return NULL;
}

static lispval portp(lispval arg)
{
  if (FD_PORTP(arg))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval input_portp(lispval arg)
{
  if (FD_PORTP(arg)) {
    struct FD_PORT *p=
      fd_consptr(struct FD_PORT *,arg,fd_port_type);
    if (p->fd_inport)
      return FD_TRUE;
    else return FD_FALSE;}
  else return FD_FALSE;
}

static lispval output_portp(lispval arg)
{
  if (FD_PORTP(arg)) {
    struct FD_PORT *p=
      fd_consptr(struct FD_PORT *,arg,fd_port_type);
    if (p->fd_outport)
      return FD_TRUE;
    else return FD_FALSE;}
  else return FD_FALSE;
}

/* Identifying end of file */

static lispval eofp(lispval x)
{
  if (FD_EOFP(x)) return FD_TRUE; else return FD_FALSE;
}

/* DTYPE streams */

static lispval packet2dtype(lispval packet)
{
  lispval object;
  struct FD_INBUF in;
  FD_INIT_BYTE_INPUT(&in,FD_PACKET_DATA(packet),
                     FD_PACKET_LENGTH(packet));
  object = fd_read_dtype(&in);
  return object;
}

static lispval lisp2packet(lispval object,lispval initsize)
{
  size_t size = FIX2INT(initsize);
  struct FD_OUTBUF out;
  FD_INIT_BYTE_OUTPUT(&out,size);
  int bytes = fd_write_dtype(&out,object);
  if (bytes<0) return FD_ERROR;
  else return fd_init_packet(NULL,bytes,out.buffer);
}

/* Output strings */

static lispval open_output_string()
{
  U8_OUTPUT *out = u8_alloc(struct U8_OUTPUT);
  U8_INIT_OUTPUT(out,256);
  return make_port(NULL,out,u8_strdup("output string"));
}

static lispval open_input_string(lispval arg)
{
  if (STRINGP(arg)) {
    U8_INPUT *in = u8_alloc(struct U8_INPUT);
    U8_INIT_STRING_INPUT(in,FD_STRING_LENGTH(arg),u8_strdup(CSTRING(arg)));
    in->u8_streaminfo = in->u8_streaminfo|U8_STREAM_OWNS_BUF;
    return make_port(in,NULL,u8_strdup("input string"));}
  else return fd_type_error(_("string"),"open_input_string",arg);
}

static lispval portid(lispval port_arg)
{
  if (FD_PORTP(port_arg)) {
    struct FD_PORT *port = (struct FD_PORT *)port_arg;
    if (port->fd_portid) return lispval_string(port->fd_portid);
    else return FD_FALSE;}
  else return fd_type_error(_("port"),"portid",port_arg);
}

static lispval portdata(lispval port_arg)
{
  if (FD_PORTP(port_arg)) {
    struct FD_PORT *port = (struct FD_PORT *)port_arg;
    if (port->fd_outport)
      return fd_substring(port->fd_outport->u8_outbuf,port->fd_outport->u8_write);
    else return fd_substring(port->fd_outport->u8_outbuf,port->fd_outport->u8_outlim);}
  else return fd_type_error(_("port"),"portdata",port_arg);
}

/* Simple STDIO */

static lispval write_prim(lispval x,lispval portarg)
{
  U8_OUTPUT *out = get_output_port(portarg);
  if (out) {
    fd_unparse(out,x);
    u8_flush(out);
    return VOID;}
  else return fd_type_error(_("output port"),"write_prim",portarg);
}

static lispval display_prim(lispval x,lispval portarg)
{
  U8_OUTPUT *out = get_output_port(portarg);
  if (out) {
    if (STRINGP(x))
      u8_puts(out,CSTRING(x));
    else fd_unparse(out,x);
    u8_flush(out);
    return VOID;}
  else return fd_type_error(_("output port"),"display_prim",portarg);
}

static lispval putchar_prim(lispval char_arg,lispval port)
{
  int ch;
  U8_OUTPUT *out = get_output_port(port);
  if (out) {
    if (FD_CHARACTERP(char_arg))
      ch = FD_CHAR2CODE(char_arg);
    else if (FD_UINTP(char_arg))
      ch = FIX2INT(char_arg);
    else return fd_type_error("character","putchar_prim",char_arg);
    u8_putc(out,ch);
    return VOID;}
  else return fd_type_error(_("output port"),"putchar_prim",port);
}

static lispval newline_prim(lispval portarg)
{
  U8_OUTPUT *out = get_output_port(portarg);
  if (out) {
    u8_puts(out,"\n");
    u8_flush(out);
    return VOID;}
  else return fd_type_error(_("output port"),"newline_prim",portarg);
}

static int printout_helper(U8_OUTPUT *out,lispval x)
{
  if (FD_ABORTP(x)) return 0;
  else if (VOIDP(x)) return 1;
  if (out == NULL) out = u8_current_output;
  if (STRINGP(x))
    u8_puts(out,CSTRING(x));
  else fd_unparse(out,x);
  return 1;
}

FD_EXPORT
lispval fd_printout(lispval body,fd_lexenv env)
{
  struct FD_STACK *_stack=fd_stackptr;
  U8_OUTPUT *out = u8_current_output;
  while (PAIRP(body)) {
    lispval value = fast_eval(FD_CAR(body),env);
    if (printout_helper(out,value)) fd_decref(value);
    else return value;
    body = FD_CDR(body);}
  u8_flush(out);
  return VOID;
}

FD_EXPORT
lispval fd_printout_to(U8_OUTPUT *out,lispval body,fd_lexenv env)
{
  struct FD_STACK *_stack=fd_stackptr;
  u8_output prev = u8_current_output;
  u8_set_default_output(out);
  while (PAIRP(body)) {
    lispval value = fast_eval(FD_CAR(body),env);
    if (printout_helper(out,value)) fd_decref(value);
    else {
      u8_flush(out);
      u8_set_default_output(prev);
      return value;}
    body = FD_CDR(body);}
  u8_flush(out);
  u8_set_default_output(prev);
  return VOID;
}

static lispval substringout(lispval arg,lispval start,lispval end)
{
  u8_output output = u8_current_output;
  u8_string string = CSTRING(arg); unsigned int len = STRLEN(arg);
  if (VOIDP(start)) u8_putn(output,string,len);
  else if (!(FD_UINTP(start)))
    return fd_type_error("uint","substringout",start);
  else if (VOIDP(end)) {
    unsigned int byte_start = u8_byteoffset(string,FIX2INT(start),len);
    u8_putn(output,string+byte_start,len-byte_start);}
  else if (!(FD_UINTP(end)))
    return fd_type_error("uint","substringout",end);
  else {
    unsigned int byte_start = u8_byteoffset(string,FIX2INT(start),len);
    unsigned int byte_end = u8_byteoffset(string,FIX2INT(end),len);
    u8_putn(output,string+byte_start,byte_end-byte_start);}
  return VOID;
}

static lispval uniscape(lispval arg,lispval excluding)
{
  u8_string input = ((STRINGP(arg))?(CSTRING(arg)):
                   (fd_lisp2string(arg)));
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

static lispval printout_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  return fd_printout(fd_get_body(expr,1),env);
}
static lispval lineout_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  U8_OUTPUT *out = u8_current_output;
  lispval value = fd_printout(fd_get_body(expr,1),env);
  if (FD_ABORTP(value)) return value;
  u8_putc(out,'\n');
  u8_flush(out);
  return VOID;
}

static lispval message_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval body = fd_get_body(expr,1);
  U8_OUTPUT *out = u8_open_output_string(1024);
  U8_OUTPUT *stream = u8_current_output;
  u8_set_default_output(out);
  while (PAIRP(body)) {
    lispval value = fast_eval(FD_CAR(body),env);
    if (printout_helper(out,value)) fd_decref(value);
    else {
      u8_set_default_output(stream);
      u8_close_output(out);
      return value;}
    body = FD_CDR(body);}
  u8_set_default_output(stream);
  u8_logger(-10,NULL,out->u8_outbuf);
  u8_close_output(out);
  return VOID;
}

static lispval notify_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval body = fd_get_body(expr,1);
  U8_OUTPUT *out = u8_open_output_string(1024);
  U8_OUTPUT *stream = u8_current_output;
  u8_set_default_output(out);
  while (PAIRP(body)) {
    lispval value = fast_eval(FD_CAR(body),env);
    if (printout_helper(out,value)) fd_decref(value);
    else {
      u8_set_default_output(stream);
      u8_close_output(out);
      return value;}
    body = FD_CDR(body);}
  u8_set_default_output(stream);
  u8_logger(LOG_NOTICE,NULL,out->u8_outbuf);
  u8_close_output(out);
  return VOID;
}

static lispval status_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval body = fd_get_body(expr,1);
  U8_OUTPUT *out = u8_open_output_string(1024);
  U8_OUTPUT *stream = u8_current_output;
  u8_set_default_output(out);
  while (PAIRP(body)) {
    lispval value = fast_eval(FD_CAR(body),env);
    if (printout_helper(out,value)) fd_decref(value);
    else {
      u8_set_default_output(stream);
      u8_close_output(out);
      return value;}
    body = FD_CDR(body);}
  u8_set_default_output(stream);
  u8_logger(LOG_INFO,NULL,out->u8_outbuf);
  u8_close_output(out);
  return VOID;
}

static lispval warning_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval body = fd_get_body(expr,1);
  U8_OUTPUT *out = u8_open_output_string(1024);
  U8_OUTPUT *stream = u8_current_output;
  u8_set_default_output(out);
  while (PAIRP(body)) {
    lispval value = fast_eval(FD_CAR(body),env);
    if (printout_helper(out,value)) fd_decref(value);
    else {
      u8_set_default_output(stream);
      u8_close_output(out);
      return value;}
    body = FD_CDR(body);}
  u8_set_default_output(stream);
  u8_logger(LOG_WARN,NULL,out->u8_outbuf);
  u8_close_output(out);
  return VOID;
}

static int get_loglevel(lispval level_arg)
{
  if (FD_INTP(level_arg)) return FIX2INT(level_arg);
  else return -1;
}

static lispval log_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval level_arg = fd_eval(fd_get_arg(expr,1),env);
  lispval body = fd_get_body(expr,2);
  int level = get_loglevel(level_arg);
  U8_OUTPUT *out = u8_open_output_string(1024);
  U8_OUTPUT *stream = u8_current_output;
  u8_condition condition = NULL;
  if (FD_THROWP(level_arg)) return level_arg;
  else if (FD_ABORTP(level_arg)) {
    fd_clear_errors(1);}
  else fd_decref(level_arg);
  if ((PAIRP(body))&&(SYMBOLP(FD_CAR(body)))) {
    condition = SYM_NAME(FD_CAR(body));
    body = FD_CDR(body);}
  u8_set_default_output(out);
  while (PAIRP(body)) {
    lispval value = fast_eval(FD_CAR(body),env);
    if (printout_helper(out,value)) fd_decref(value);
    else {
      u8_set_default_output(stream);
      u8_close_output(out);
      return value;}
    body = FD_CDR(body);}
  u8_set_default_output(stream);
  u8_logger(level,condition,out->u8_outbuf);
  u8_close_output(out);
  return VOID;
}

static lispval logif_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval test_expr = fd_get_arg(expr,1), value = FD_FALSE;
  if (FD_ABORTP(test_expr)) return test_expr;
  else if (PRED_FALSE(STRINGP(test_expr)))
    return fd_reterr(fd_SyntaxError,"logif_evalfn",
                     _("LOGIF condition expression cannot be a string"),expr);
  else value = fast_eval(test_expr,env);
  if (FD_ABORTP(value)) return value;
  else if ( (FALSEP(value)) || (VOIDP(value)) ||
            (EMPTYP(value)) || (NILP(value)) )
    return VOID;
  else {
    lispval body = fd_get_body(expr,2);
    U8_OUTPUT *out = u8_open_output_string(1024);
    U8_OUTPUT *stream = u8_current_output;
    u8_condition condition = NULL;
    if ((PAIRP(body))&&(SYMBOLP(FD_CAR(body)))) {
      condition = SYM_NAME(FD_CAR(body));
      body = FD_CDR(body);}
    fd_decref(value); u8_set_default_output(out);
    while (PAIRP(body)) {
      lispval value = fast_eval(FD_CAR(body),env);
      if (printout_helper(out,value)) fd_decref(value);
      else {
        u8_set_default_output(stream);
        u8_close_output(out);
        return value;}
      body = FD_CDR(body);}
    u8_set_default_output(stream);
    u8_logger(-10,condition,out->u8_outbuf);
    u8_close_output(out);
    return VOID;}
}

static lispval logifplus_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval test_expr = fd_get_arg(expr,1), value = FD_FALSE, loglevel_arg;
  if (FD_ABORTP(test_expr)) return test_expr;
  else if (PRED_FALSE(STRINGP(test_expr)))
    return fd_reterr(fd_SyntaxError,"logif_evalfn",
                     _("LOGIF condition expression cannot be a string"),expr);
  else value = fast_eval(test_expr,env);
  if (FD_ABORTP(value)) return value;
  else if ((FALSEP(value)) || (VOIDP(value)) ||
           (EMPTYP(value)) || (NILP(value)))
    return VOID;
  else loglevel_arg = fd_eval(fd_get_arg(expr,2),env);
  if (FD_ABORTP(loglevel_arg)) return loglevel_arg;
  else if (VOIDP(loglevel_arg))
    return fd_reterr(fd_SyntaxError,"logif_plus_evalfn",
                     _("LOGIF+ loglevel invalid"),expr);
  else if (!(FD_INTP(loglevel_arg)))
    return fd_reterr(fd_TypeError,"logif_plus_evalfn",
                     _("LOGIF+ loglevel invalid"),loglevel_arg);
  else {
    lispval body = fd_get_body(expr,3);
    U8_OUTPUT *out = u8_open_output_string(1024);
    U8_OUTPUT *stream = u8_current_output;
    int priority = FIX2INT(loglevel_arg);
    u8_condition condition = NULL;
     if ((PAIRP(body))&&(SYMBOLP(FD_CAR(body)))) {
      condition = SYM_NAME(FD_CAR(body));
      body = FD_CDR(body);}
    fd_decref(value); u8_set_default_output(out);
    while (PAIRP(body)) {
      lispval value = fast_eval(FD_CAR(body),env);
      if (printout_helper(out,value)) fd_decref(value);
      else {
        u8_set_default_output(stream);
        u8_close_output(out);
        return value;}
      body = FD_CDR(body);}
    u8_set_default_output(stream);
    u8_logger(-priority,condition,out->u8_outbuf);
    u8_close_output(out);
    return VOID;}
}

static lispval stringout_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  struct U8_OUTPUT out; lispval result; u8_byte buf[256];
  U8_INIT_OUTPUT_X(&out,256,buf,0);
  result = fd_printout_to(&out,fd_get_body(expr,1),env);
  if (!(FD_ABORTP(result))) {
    fd_decref(result);
    result = fd_make_string
      (NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);}
  u8_close_output(&out);
  return result;
}

/* Input operations! */

static lispval getchar_prim(lispval port)
{
  U8_INPUT *in = get_input_port(port);
  if (in) {
    int ch = -1;
    if (in) ch = u8_getc(in);
    if (ch<0) return FD_EOF;
    else return FD_CODE2CHAR(ch);}
  else return fd_type_error(_("input port"),"getchar_prim",port);
}

static lispval getline_prim(lispval port,lispval eos_arg,lispval lim_arg,
                           lispval eof_marker)
{
  U8_INPUT *in = get_input_port(port);
  if (VOIDP(eof_marker)) eof_marker = EMPTY;
  else if (FD_TRUEP(eof_marker)) eof_marker = FD_EOF;
  else {}
  if (in) {
    u8_string data, eos;
    int lim, size = 0;
    if (in == NULL)
      return fd_type_error(_("input port"),"getline_prim",port);
    if (VOIDP(eos_arg)) eos="\n";
    else if (STRINGP(eos_arg)) eos = CSTRING(eos_arg);
    else return fd_type_error(_("string"),"getline_prim",eos_arg);
    if (VOIDP(lim_arg)) lim = 0;
    else if (FIXNUMP(lim_arg)) lim = FIX2INT(lim_arg);
    else return fd_type_error(_("fixum"),"getline_prim",eos_arg);
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
        return fd_stream2string(&out);}
      else return fd_init_string(NULL,size,data);
    else if (size<0)
      if (errno == EAGAIN)
        return FD_EOF;
      else return FD_ERROR;
    else return fd_incref(eof_marker);}
  else return fd_type_error(_("input port"),"getline_prim",port);
}

static lispval read_prim(lispval port)
{
  if (STRINGP(port)) {
    struct U8_INPUT in;
    U8_INIT_STRING_INPUT(&in,STRLEN(port),CSTRING(port));
    return fd_parser(&in);}
  else {
    U8_INPUT *in = get_input_port(port);
    if (in) {
      int c = fd_skip_whitespace(in);
      if (c<0) return FD_EOF;
      else return fd_parser(in);}
    else return fd_type_error(_("input port"),"read_prim",port);}
}

/* Reading records */

static off_t find_substring(u8_string string,lispval strings,
                            ssize_t len,ssize_t *lenp);
static ssize_t get_more_data(u8_input in,size_t lim);
static lispval record_reader(lispval port,lispval ends,lispval limit_arg);

static lispval read_record_prim(lispval ports,lispval ends,lispval limit_arg)
{
  lispval results = EMPTY;
  DO_CHOICES(port,ports) {
    lispval result = record_reader(port,ends,limit_arg);
    CHOICE_ADD(results,result);}
  return results;
}

static lispval record_reader(lispval port,lispval ends,lispval limit_arg)
{
  U8_INPUT *in = get_input_port(port);
  size_t lim, matchlen = 0;
  off_t off = -1;

  if (in == NULL)
    return fd_type_error(_("input port"),"record_reader",port);
  if (VOIDP(limit_arg)) lim = -1;
  else if (FIXNUMP(limit_arg))
    lim = FIX2INT(limit_arg);
  else return fd_type_error(_("fixnum"),"record_reader",limit_arg);

  if (VOIDP(ends)) {}
  else {
    DO_CHOICES(end,ends)
      if (!((STRINGP(end))||(TYPEP(end,fd_regex_type))))
        return fd_type_error(_("string"),"record_reader",end);}
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
      lispval result = fd_make_string(NULL,record_len,in->u8_read);
      in->u8_read+=record_len;
      return result;}
    else if ((lim) && ((in->u8_inlim-in->u8_read)>lim))
      return FD_EOF;
    else if (in->u8_fillfn) {
      ssize_t more_data = get_more_data(in,lim);
      if (more_data>0) continue;
      else return FD_EOF;}
    else return FD_EOF;}
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
    else if (TYPEP(s,fd_regex_type)) {
      off_t starts = fd_regex_op(rx_search,s,string,len,0);
      ssize_t matched_len = (starts<0)?(-1):
        (fd_regex_op(rx_matchlen,s,string+starts,len,0));
      if ((starts<0)||(matched_len<=0)) continue;
      else if ((off<0)||((starts<off)&&(matched_len>0))) {
        off = starts;
        matchlen = matched_len;}
      else {}}}
  if (off<0) return off;
  *lenp = matchlen;
  return off;
}

static ssize_t get_more_data(u8_input in,size_t lim)
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

/* Lisp to string */

static lispval lisp2string(lispval x)
{
  U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
  fd_unparse(&out,x);
  return fd_stream2string(&out);
}

static lispval inexact2string(lispval x,lispval precision)
{
  if (FD_FLONUMP(x))
    if ((FD_UINTP(precision)) || (VOIDP(precision))) {
      int prec = ((VOIDP(precision)) ? (2) : (FIX2INT(precision)));
      char buf[128]; char cmd[16];
      sprintf(cmd,"%%.%df",prec);
      sprintf(buf,cmd,FD_FLONUM(x));
      return lispval_string(buf);}
    else return fd_type_error("fixnum","inexact2string",precision);
  else return lisp2string(x);
}

static lispval number2string(lispval x,lispval base)
{
  if (NUMBERP(x)) {
    struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
    fd_output_number(&out,x,fd_getint(base));
    return fd_stream2string(&out);}
  else return fd_err(fd_TypeError,"number2string",NULL,x);
}

static lispval number2locale(lispval x,lispval precision)
{
  if (FD_FLONUMP(x))
    if ((FD_UINTP(precision)) || (VOIDP(precision))) {
      int prec = ((VOIDP(precision)) ? (2) : (FIX2INT(precision)));
      char buf[128]; char cmd[16];
      sprintf(cmd,"%%'.%df",prec);
      sprintf(buf,cmd,FD_FLONUM(x));
      return lispval_string(buf);}
    else return fd_type_error("fixnum","inexact2string",precision);
  else if (FIXNUMP(x)) {
    char buf[128];
    sprintf(buf,"%'lld",FIX2INT(x));
    return lispval_string(buf);}
  else return lisp2string(x);
}

static lispval string2number(lispval x,lispval base)
{
  return fd_string2number(CSTRING(x),fd_getint(base));
}

static lispval just2number(lispval x,lispval base)
{
  if (NUMBERP(x)) return fd_incref(x);
  else if (STRINGP(x)) {
    lispval num = fd_string2number(CSTRING(x),fd_getint(base));
    if (FALSEP(num)) return FD_FALSE;
    else return num;}
  else return fd_type_error(_("string or number"),"->NUMBER",x);
}

/* Printing a backtrace */

static u8_exception print_backtrace_entry
(U8_OUTPUT *out,u8_exception ex,int width)
{
  fd_print_exception(out,ex);
  return ex->u8x_prev;
}

static u8_exception log_backtrace_entry(int loglevel,u8_condition label,
                                        u8_exception ex,int width)
{
  struct U8_OUTPUT tmpout; u8_byte buf[16384];
  U8_INIT_OUTPUT_BUF(&tmpout,16384,buf);
  if ((ex->u8x_context) && (ex->u8x_details))
    u8_log(loglevel,ex->u8x_cond,"(%s) %m",(ex->u8x_context),(ex->u8x_details));
  else if (ex->u8x_context)
    u8_log(loglevel,ex->u8x_cond,"(%s)",(ex->u8x_context));
  else if (ex->u8x_details)
    u8_log(loglevel,ex->u8x_cond,"(%m)",(ex->u8x_details));
  else u8_log(loglevel,ex->u8x_cond,_("No more information"));
  return ex->u8x_prev;
}

FD_EXPORT
void fd_print_backtrace(U8_OUTPUT *out,u8_exception ex,int width)
{
  u8_exception scan = ex;
  while (scan) {
    u8_puts(out,";;=======================================================\n");
    scan = print_backtrace_entry(out,scan,width);}
}

FD_EXPORT void fd_log_backtrace(u8_exception ex,int level,u8_condition label,
                                int width)
{
  u8_exception scan = ex;
  while (scan) {
    scan = log_backtrace_entry(level,label,scan,width);}
}

FD_EXPORT
void fd_summarize_backtrace(U8_OUTPUT *out,u8_exception ex)
{
  u8_exception scan = ex; u8_condition cond = NULL;
  while (scan) {
    lispval irritant = fd_exception_xdata(scan); int show_irritant = 1;
    if (scan!=ex) u8_puts(out," <");
    if (scan->u8x_cond!=cond) {
      cond = scan->u8x_cond; u8_printf(out," (%m)",cond);}
    if (scan->u8x_context)
      u8_printf(out," %s",scan->u8x_context);
    if (scan->u8x_details)
      u8_printf(out," [%s]",scan->u8x_details);
    if (show_irritant)
      u8_printf(out," <%q>",irritant);
    scan = scan->u8x_prev;}
}


/* Table showing primitives */

static lispval lisp_show_table(lispval tables,lispval slotids,lispval portarg)
{
  U8_OUTPUT *out = get_output_port(portarg);
  DO_CHOICES(table,tables)
    if ((FALSEP(slotids)) || (VOIDP(slotids)))
      fd_display_table(out,table,VOID);
    else if (OIDP(table)) {
      U8_OUTPUT *tmp = u8_open_output_string(1024);
      u8_printf(out,"%q\n",table);
      {DO_CHOICES(slotid,slotids) {
        lispval values = fd_frame_get(table,slotid);
        tmp->u8_write = tmp->u8_outbuf; *(tmp->u8_outbuf)='\0';
        u8_printf(tmp,"   %q:   %q\n",slotid,values);
        if (u8_strlen(tmp->u8_outbuf)<80) u8_puts(out,tmp->u8_outbuf);
        else {
          u8_printf(out,"   %q:\n",slotid);
          {DO_CHOICES(value,values) u8_printf(out,"      %q\n",value);}}
        fd_decref(values);}}
      u8_close((u8_stream)tmp);}
    else fd_display_table(out,table,slotids);
  u8_flush(out);
  return VOID;
}

/* PPRINT lisp primitives */

static lispval lisp_pprint(lispval x,lispval portarg,lispval widtharg,lispval marginarg)
{
  struct U8_OUTPUT tmpout;
  U8_OUTPUT *out = get_output_port(portarg);
  int width = ((FD_UINTP(widtharg)) ? (FIX2INT(widtharg)) : (60));
  if ((out == NULL)&&(!(FALSEP(portarg))))
    return fd_type_error(_("port"),"lisp_pprint",portarg);
  U8_INIT_OUTPUT(&tmpout,512);
  if (VOIDP(marginarg))
    fd_pprint(&tmpout,x,NULL,0,0,width);
  else if (STRINGP(marginarg))
    fd_pprint(&tmpout,x,CSTRING(marginarg),0,0,width);
  else if ((FD_UINTP(marginarg))&&(FIX2INT(marginarg)>=0))
    fd_pprint(&tmpout,x,NULL,(FIX2INT(marginarg)),0,width);
  else fd_pprint(&tmpout,x,NULL,0,0,width);
  if (out) {
    u8_puts(out,tmpout.u8_outbuf); u8_free(tmpout.u8_outbuf);
    u8_flush(out);
    return VOID;}
  else return fd_init_string(NULL,tmpout.u8_write-tmpout.u8_outbuf,
                             tmpout.u8_outbuf);
}

/* Base 64 stuff */

static lispval from_base64_prim(lispval string)
{
  const u8_byte *string_data = CSTRING(string);
  unsigned int string_len = STRLEN(string), data_len;
  unsigned char *data=
    u8_read_base64(string_data,string_data+string_len,&data_len);
  if (data)
    return fd_init_packet(NULL,data_len,data);
  else return FD_ERROR;
}

static lispval to_base64_prim(lispval packet,lispval nopad,lispval urisafe)
{
  const u8_byte *packet_data = FD_PACKET_DATA(packet);
  unsigned int packet_len = FD_PACKET_LENGTH(packet), ascii_len;
  char *ascii_string = u8_write_base64(packet_data,packet_len,&ascii_len);
  if (ascii_string) {
    if (FD_TRUEP(nopad)) {
      char *scan = ascii_string+(ascii_len-1);
      while (*scan=='=') {*scan='\0'; scan--; ascii_len--;}}
    if (FD_TRUEP(urisafe)) {
      char *scan = ascii_string, *limit = ascii_string+ascii_len;
      while (scan<limit)  {
        if (*scan=='+') *scan++='-';
        else if (*scan=='/') *scan++='_';
        else scan++;}}
    return fd_init_string(NULL,ascii_len,ascii_string);}
  else return FD_ERROR;
}

static lispval any_to_base64_prim(lispval arg,lispval nopad,lispval urisafe)
{
  unsigned int data_len, ascii_len;
  const u8_byte *data; char *ascii_string;
  if (PACKETP(arg)) {
    data = FD_PACKET_DATA(arg);
    data_len = FD_PACKET_LENGTH(arg);}
  else if ((STRINGP(arg))||(TYPEP(arg,fd_secret_type))) {
    data = CSTRING(arg);
    data_len = STRLEN(arg);}
  else return fd_type_error("packet or string","any_to_base64_prim",arg);
  ascii_string = u8_write_base64(data,data_len,&ascii_len);
  if (ascii_string) {
    if (FD_TRUEP(nopad)) {
      char *scan = ascii_string+(ascii_len-1);
      while (*scan=='=') {*scan='\0'; scan--; ascii_len--;}}
    if (FD_TRUEP(urisafe)) {
      char *scan = ascii_string, *limit = ascii_string+ascii_len;
      while (scan<limit)  {
        if (*scan=='+') *scan++='-';
        else if (*scan=='/') *scan++='_';
        else scan++;}}
    return fd_init_string(NULL,ascii_len,ascii_string);}
  else return FD_ERROR;
}

/* Base 16 stuff */

static lispval from_base16_prim(lispval string)
{
  const u8_byte *string_data = CSTRING(string);
  unsigned int string_len = STRLEN(string), data_len;
  unsigned char *data = u8_read_base16(string_data,string_len,&data_len);
  if (data)
    return fd_init_packet(NULL,data_len,data);
  else return FD_ERROR;
}

static lispval to_base16_prim(lispval packet)
{
  const u8_byte *packet_data = FD_PACKET_DATA(packet);
  unsigned int packet_len = FD_PACKET_LENGTH(packet);
  char *ascii_string = u8_write_base16(packet_data,packet_len);
  if (ascii_string)
    return fd_init_string(NULL,packet_len*2,ascii_string);
  else return FD_ERROR;
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

static lispval gzip_prim(lispval arg,lispval filename,lispval comment)
{
  if (!((STRINGP(arg)||PACKETP(arg))))
    return fd_type_error("string or packet","x2zipfile_prim",arg);
  else {
    fd_exception error = NULL;
    const unsigned char *data=
      ((STRINGP(arg))?(CSTRING(arg)):(FD_PACKET_DATA(arg)));
    unsigned int data_len=
      ((STRINGP(arg))?(STRLEN(arg)):(FD_PACKET_LENGTH(arg)));
    struct FD_OUTBUF out; int flags = 0; /* FDPP_FHCRC */
    time_t now = time(NULL); u8_int4 crc, intval;
    FD_INIT_BYTE_OUTPUT(&out,1024); memset(out.buffer,0,1024);
    fd_write_byte(&out,31); fd_write_byte(&out,139);
    fd_write_byte(&out,8); /* Using default */
    /* Compute flags */
    if ((STRINGP(arg))&&(string_isasciip(CSTRING(arg),STRLEN(arg))))
      flags = flags|FDPP_FASCII;
    if (STRINGP(filename)) flags = flags|FDPP_FNAME;
    if (STRINGP(comment)) flags = flags|FDPP_FCOMMENT;
    fd_write_byte(&out,flags);
    intval = fd_flip_word((unsigned int)now);
    fd_write_4bytes(&out,intval);
    fd_write_byte(&out,2); /* Max compression */
    fd_write_byte(&out,3); /* Assume Unix */
    /* No extra fields */
    if (STRINGP(filename)) {
      u8_string text = CSTRING(filename), end = text+STRLEN(filename);
      int len;
      unsigned char *string=
        u8_localize(latin1_encoding,&text,end,'\\',0,NULL,&len);
      fd_write_bytes(&out,string,len); fd_write_byte(&out,'\0');
      u8_free(string);}
    if (STRINGP(comment)) {
      int len;
      u8_string text = CSTRING(comment), end = text+STRLEN(comment);
      unsigned char *string=
        u8_localize(latin1_encoding,&text,end,'\\',0,NULL,&len);
      fd_write_bytes(&out,string,len); fd_write_byte(&out,'\0');
      u8_free(string);}
    /*
    crc = u8_crc32(0,(void *)out.start,out.ptr-out.start);
    fd_write_byte(&out,((crc)&(0xFF)));
    fd_write_byte(&out,((crc>>8)&(0xFF)));
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
        fd_write_bytes(&out,cbuf+2,csize-6);}
      u8_free(cbuf);}
    if (error) {
      fd_seterr(error,"x2zipfile",NULL,VOID);
      u8_free(out.buffer);
      return FD_ERROR;}
    crc = u8_crc32(0,data,data_len);
    intval = fd_flip_word(crc); fd_write_4bytes(&out,intval);
    intval = fd_flip_word(data_len); fd_write_4bytes(&out,intval);
    return fd_init_packet(NULL,out.bufwrite-out.buffer,out.buffer);}
}

/* Port type operations */

/* The port type */

static int unparse_port(struct U8_OUTPUT *out,lispval x)
{
  struct FD_PORT *p = fd_consptr(fd_port,x,fd_port_type);
  if ((p->fd_inport) && (p->fd_outport) && (p->fd_portid))
    u8_printf(out,"#<I/O Port (%s) #!%x>",p->fd_portid,x);
  else if ((p->fd_inport) && (p->fd_outport))
    u8_printf(out,"#<I/O Port #!%x>",x);
  else if ((p->fd_inport)&&(p->fd_portid))
    u8_printf(out,"#<Input Port (%s) #!%x>",p->fd_portid,x);
  else if (p->fd_inport)
    u8_printf(out,"#<Input Port #!%x>",x);
  else if (p->fd_portid)
    u8_printf(out,"#<Output Port (%s) #!%x>",p->fd_portid,x);
  else u8_printf(out,"#<Output Port #!%x>",x);
  return 1;
}

static void recycle_port(struct FD_RAW_CONS *c)
{
  struct FD_PORT *p = (struct FD_PORT *)c;
  if (p->fd_inport) {
    u8_close_input(p->fd_inport);}
  if (p->fd_outport) {
    u8_close_output(p->fd_outport);}
  if (p->fd_portid) u8_free(p->fd_portid);
 if (FD_MALLOCD_CONSP(c)) u8_free(c);
}

/* The init function */

FD_EXPORT void fd_init_portprims_c()
{
  u8_register_source_file(_FILEINFO);

  fd_unparsers[fd_port_type]=unparse_port;
  fd_recyclers[fd_port_type]=recycle_port;

  fd_idefn(fd_scheme_module,fd_make_cprim1("LISP->STRING",lisp2string,1));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2("INEXACT->STRING",inexact2string,1));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("NUMBER->STRING",number2string,1,
                           -1,VOID,fd_fixnum_type,FD_INT(10)));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2("NUMBER->LOCALE",number2locale,1));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("STRING->NUMBER",string2number,1,
                           fd_string_type,VOID,
                           fd_fixnum_type,FD_INT(-1)));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("->NUMBER",just2number,1,
                           -1,VOID,fd_fixnum_type,FD_INT(-1)));

  fd_idefn(fd_scheme_module,fd_make_cprim1("EOF-OBJECT?",eofp,1));

  fd_idefn(fd_scheme_module,fd_make_cprim1("PORTID",portid,1));

  fd_idefn(fd_scheme_module,
           fd_make_cprim0("OPEN-OUTPUT-STRING",open_output_string));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1("OPEN-INPUT-STRING",open_input_string,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("PORTDATA",portdata,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("PORT?",portp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("INPUT-PORT?",input_portp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("OUTPUT-PORT?",output_portp,1));

  fd_idefn(fd_scheme_module,fd_make_cprim2("WRITE",write_prim,1));
  fd_idefn(fd_scheme_module,fd_make_cprim2("DISPLAY",display_prim,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("NEWLINE",newline_prim,0));
  fd_idefn(fd_scheme_module,fd_make_cprim4("PPRINT",lisp_pprint,1));

  fd_idefn(fd_scheme_module,fd_make_cprim2("PUTCHAR",putchar_prim,1));
  fd_defalias(fd_scheme_module,"WRITE-CHAR","PUTCHAR");
  fd_idefn(fd_scheme_module,fd_make_cprim1("GETCHAR",getchar_prim,0));
  fd_idefn(fd_scheme_module,fd_make_cprim4("GETLINE",getline_prim,0));
  fd_idefn(fd_scheme_module,fd_make_cprim1("READ",read_prim,0));

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("READ-RECORD",read_record_prim,1)));

  fd_def_evalfn(fd_scheme_module,"PRINTOUT","",printout_evalfn);
  fd_def_evalfn(fd_scheme_module,"LINEOUT","",lineout_evalfn);
  fd_def_evalfn(fd_scheme_module,"STRINGOUT","",stringout_evalfn);
  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("SUBSTRINGOUT",substringout,1,
                           fd_string_type,VOID,
                           fd_fixnum_type,VOID,
                           fd_fixnum_type,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("UNISCAPE",uniscape,1,
                           fd_string_type,VOID,
                           fd_string_type,VOID));

  /* Logging functions for specific levels */
  fd_def_evalfn(fd_scheme_module,"NOTIFY","",notify_evalfn);
  fd_def_evalfn(fd_scheme_module,"STATUS","",status_evalfn);
  fd_def_evalfn(fd_scheme_module,"WARNING","",warning_evalfn);

  /* Generic logging function, always outputs */
  fd_def_evalfn(fd_scheme_module,"MESSAGE","",message_evalfn);
  fd_def_evalfn(fd_scheme_module,"%LOGGER","",message_evalfn);

  /* Logging with message level */
  fd_def_evalfn(fd_scheme_module,"LOGMSG","",log_evalfn);
  /* Conditional logging */
  fd_def_evalfn(fd_scheme_module,"LOGIF","",logif_evalfn);
  /* Conditional logging with priority level */
  fd_def_evalfn(fd_scheme_module,"LOGIF+","",logifplus_evalfn);

  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("PACKET->DTYPE",packet2dtype,1,
                           fd_packet_type,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("DTYPE->PACKET",lisp2packet,1,
                           -1,VOID,fd_fixnum_type,FD_INT(128)));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("BASE64->PACKET",from_base64_prim,1,
                           fd_string_type,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("PACKET->BASE64",to_base64_prim,1,
                           fd_packet_type,VOID,-1,FD_FALSE,-1,FD_FALSE));
  fd_idefn(fd_scheme_module,fd_make_cprim3("->BASE64",any_to_base64_prim,1));

  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("BASE16->PACKET",from_base16_prim,1,
                           fd_string_type,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("PACKET->BASE16",to_base16_prim,1,
                           fd_packet_type,VOID));

  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("GZIP",gzip_prim,1,-1,VOID,
                           fd_string_type,VOID,
                           fd_string_type,VOID));

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("%SHOW",lisp_show_table,1)));

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
