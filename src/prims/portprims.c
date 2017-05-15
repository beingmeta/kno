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

static fdtype make_port(U8_INPUT *in,U8_OUTPUT *out,u8_string id)
{
  struct FD_PORT *port = u8_alloc(struct FD_PORT);
  FD_INIT_CONS(port,fd_port_type);
  port->fd_inport = in; port->fd_outport = out; port->fd_portid = id;
  return FDTYPE_CONS(port);
}

static u8_output get_output_port(fdtype portarg)
{
  if ((FD_VOIDP(portarg))||(FD_TRUEP(portarg)))
    return u8_current_output;
  else if (FD_PORTP(portarg)) {
    struct FD_PORT *p=
      fd_consptr(struct FD_PORT *,portarg,fd_port_type);
    return p->fd_outport;}
  else return NULL;
}

static u8_input get_input_port(fdtype portarg)
{
  if (FD_VOIDP(portarg))
    return NULL; /* get_default_output(); */
  else if (FD_PORTP(portarg)) {
    struct FD_PORT *p=
      fd_consptr(struct FD_PORT *,portarg,fd_port_type);
    return p->fd_inport;}
  else return NULL;
}

static fdtype portp(fdtype arg)
{
  if (FD_PORTP(arg))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype input_portp(fdtype arg)
{
  if (FD_PORTP(arg)) {
    struct FD_PORT *p=
      fd_consptr(struct FD_PORT *,arg,fd_port_type);
    if (p->fd_inport)
      return FD_TRUE;
    else return FD_FALSE;}
  else return FD_FALSE;
}

static fdtype output_portp(fdtype arg)
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

static fdtype eofp(fdtype x)
{
  if (FD_EOFP(x)) return FD_TRUE; else return FD_FALSE;
}

/* DTYPE streams */

static fdtype packet2dtype(fdtype packet)
{
  fdtype object;
  struct FD_INBUF in;
  FD_INIT_BYTE_INPUT(&in,FD_PACKET_DATA(packet),
                     FD_PACKET_LENGTH(packet));
  object = fd_read_dtype(&in);
  return object;
}

static fdtype dtype2packet(fdtype object,fdtype initsize)
{
  size_t size = FD_FIX2INT(initsize);
  struct FD_OUTBUF out;
  FD_INIT_BYTE_OUTPUT(&out,size);
  int bytes = fd_write_dtype(&out,object);
  if (bytes<0) return FD_ERROR_VALUE;
  else return fd_init_packet(NULL,bytes,out.buffer);
}

/* Output strings */

static fdtype open_output_string()
{
  U8_OUTPUT *out = u8_alloc(struct U8_OUTPUT);
  U8_INIT_OUTPUT(out,256);
  return make_port(NULL,out,u8_strdup("output string"));
}

static fdtype open_input_string(fdtype arg)
{
  if (FD_STRINGP(arg)) {
    U8_INPUT *in = u8_alloc(struct U8_INPUT);
    U8_INIT_STRING_INPUT(in,FD_STRING_LENGTH(arg),u8_strdup(FD_STRDATA(arg)));
    in->u8_streaminfo = in->u8_streaminfo|U8_STREAM_OWNS_BUF;
    return make_port(in,NULL,u8_strdup("input string"));}
  else return fd_type_error(_("string"),"open_input_string",arg);
}

static fdtype portid(fdtype port_arg)
{
  if (FD_PORTP(port_arg)) {
    struct FD_PORT *port = (struct FD_PORT *)port_arg;
    if (port->fd_portid) return fdtype_string(port->fd_portid);
    else return FD_FALSE;}
  else return fd_type_error(_("port"),"portid",port_arg);
}

static fdtype portdata(fdtype port_arg)
{
  if (FD_PORTP(port_arg)) {
    struct FD_PORT *port = (struct FD_PORT *)port_arg;
    if (port->fd_outport)
      return fd_substring(port->fd_outport->u8_outbuf,port->fd_outport->u8_write);
    else return fd_substring(port->fd_outport->u8_outbuf,port->fd_outport->u8_outlim);}
  else return fd_type_error(_("port"),"portdata",port_arg);
}

/* Simple STDIO */

static fdtype write_prim(fdtype x,fdtype portarg)
{
  U8_OUTPUT *out = get_output_port(portarg);
  if (out) {
    fd_unparse(out,x);
    u8_flush(out);
    return FD_VOID;}
  else return fd_type_error(_("output port"),"write_prim",portarg);
}

static fdtype display_prim(fdtype x,fdtype portarg)
{
  U8_OUTPUT *out = get_output_port(portarg);
  if (out) {
    if (FD_STRINGP(x))
      u8_puts(out,FD_STRDATA(x));
    else fd_unparse(out,x);
    u8_flush(out);
    return FD_VOID;}
  else return fd_type_error(_("output port"),"display_prim",portarg);
}

static fdtype putchar_prim(fdtype char_arg,fdtype port)
{
  int ch;
  U8_OUTPUT *out = get_output_port(port);
  if (out) {
    if (FD_CHARACTERP(char_arg))
      ch = FD_CHAR2CODE(char_arg);
    else if (FD_UINTP(char_arg))
      ch = FD_FIX2INT(char_arg);
    else return fd_type_error("character","putchar_prim",char_arg);
    u8_putc(out,ch);
    return FD_VOID;}
  else return fd_type_error(_("output port"),"putchar_prim",port);
}

static fdtype newline_prim(fdtype portarg)
{
  U8_OUTPUT *out = get_output_port(portarg);
  if (out) {
    u8_puts(out,"\n");
    u8_flush(out);
    return FD_VOID;}
  else return fd_type_error(_("output port"),"newline_prim",portarg);
}

static int printout_helper(U8_OUTPUT *out,fdtype x)
{
  if (FD_ABORTP(x)) return 0;
  else if (FD_VOIDP(x)) return 1;
  if (out == NULL) out = u8_current_output;
  if (FD_STRINGP(x))
    u8_puts(out,FD_STRDATA(x));
  else fd_unparse(out,x);
  return 1;
}

FD_EXPORT
fdtype fd_printout(fdtype body,fd_lispenv env)
{
  struct FD_STACK *_stack=fd_stackptr;
  U8_OUTPUT *out = u8_current_output;
  while (FD_PAIRP(body)) {
    fdtype value = fast_eval(FD_CAR(body),env);
    if (printout_helper(out,value)) fd_decref(value);
    else return value;
    body = FD_CDR(body);}
  u8_flush(out);
  return FD_VOID;
}

FD_EXPORT
fdtype fd_printout_to(U8_OUTPUT *out,fdtype body,fd_lispenv env)
{
  struct FD_STACK *_stack=fd_stackptr;
  u8_output prev = u8_current_output;
  u8_set_default_output(out);
  while (FD_PAIRP(body)) {
    fdtype value = fast_eval(FD_CAR(body),env);
    if (printout_helper(out,value)) fd_decref(value);
    else {
      u8_flush(out);
      u8_set_default_output(prev);
      return value;}
    body = FD_CDR(body);}
  u8_flush(out);
  u8_set_default_output(prev);
  return FD_VOID;
}

static fdtype substringout(fdtype arg,fdtype start,fdtype end)
{
  u8_output output = u8_current_output;
  u8_string string = FD_STRDATA(arg); unsigned int len = FD_STRLEN(arg);
  if (FD_VOIDP(start)) u8_putn(output,string,len);
  else if (!(FD_UINTP(start)))
    return fd_type_error("uint","substringout",start);
  else if (FD_VOIDP(end)) {
    unsigned int byte_start = u8_byteoffset(string,FD_FIX2INT(start),len);
    u8_putn(output,string+byte_start,len-byte_start);}
  else if (!(FD_UINTP(end)))
    return fd_type_error("uint","substringout",end);
  else {
    unsigned int byte_start = u8_byteoffset(string,FD_FIX2INT(start),len);
    unsigned int byte_end = u8_byteoffset(string,FD_FIX2INT(end),len);
    u8_putn(output,string+byte_start,byte_end-byte_start);}
  return FD_VOID;
}

static fdtype uniscape(fdtype arg,fdtype excluding)
{
  u8_string input = ((FD_STRINGP(arg))?(FD_STRDATA(arg)):
                   (fd_dtype2string(arg)));
  u8_string exstring = ((FD_STRINGP(excluding))?
                      (FD_STRDATA(excluding)):
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
  if (!(FD_STRINGP(arg))) u8_free(input);
  return FD_VOID;
}

static fdtype printout_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  return fd_printout(fd_get_body(expr,1),env);
}
static fdtype lineout_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  U8_OUTPUT *out = u8_current_output;
  fdtype value = fd_printout(fd_get_body(expr,1),env);
  if (FD_ABORTP(value)) return value;
  u8_putc(out,'\n');
  u8_flush(out);
  return FD_VOID;
}

static fdtype message_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  fdtype body = fd_get_body(expr,1);
  U8_OUTPUT *out = u8_open_output_string(1024);
  U8_OUTPUT *stream = u8_current_output;
  u8_set_default_output(out);
  while (FD_PAIRP(body)) {
    fdtype value = fast_eval(FD_CAR(body),env);
    if (printout_helper(out,value)) fd_decref(value);
    else {
      u8_set_default_output(stream);
      u8_close_output(out);
      return value;}
    body = FD_CDR(body);}
  u8_set_default_output(stream);
  u8_logger(-10,NULL,out->u8_outbuf);
  u8_close_output(out);
  return FD_VOID;
}

static fdtype notify_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  fdtype body = fd_get_body(expr,1);
  U8_OUTPUT *out = u8_open_output_string(1024);
  U8_OUTPUT *stream = u8_current_output;
  u8_set_default_output(out);
  while (FD_PAIRP(body)) {
    fdtype value = fast_eval(FD_CAR(body),env);
    if (printout_helper(out,value)) fd_decref(value);
    else {
      u8_set_default_output(stream);
      u8_close_output(out);
      return value;}
    body = FD_CDR(body);}
  u8_set_default_output(stream);
  u8_logger(LOG_NOTICE,NULL,out->u8_outbuf);
  u8_close_output(out);
  return FD_VOID;
}

static fdtype status_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  fdtype body = fd_get_body(expr,1);
  U8_OUTPUT *out = u8_open_output_string(1024);
  U8_OUTPUT *stream = u8_current_output;
  u8_set_default_output(out);
  while (FD_PAIRP(body)) {
    fdtype value = fast_eval(FD_CAR(body),env);
    if (printout_helper(out,value)) fd_decref(value);
    else {
      u8_set_default_output(stream);
      u8_close_output(out);
      return value;}
    body = FD_CDR(body);}
  u8_set_default_output(stream);
  u8_logger(LOG_INFO,NULL,out->u8_outbuf);
  u8_close_output(out);
  return FD_VOID;
}

static fdtype warning_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  fdtype body = fd_get_body(expr,1);
  U8_OUTPUT *out = u8_open_output_string(1024);
  U8_OUTPUT *stream = u8_current_output;
  u8_set_default_output(out);
  while (FD_PAIRP(body)) {
    fdtype value = fast_eval(FD_CAR(body),env);
    if (printout_helper(out,value)) fd_decref(value);
    else {
      u8_set_default_output(stream);
      u8_close_output(out);
      return value;}
    body = FD_CDR(body);}
  u8_set_default_output(stream);
  u8_logger(LOG_WARN,NULL,out->u8_outbuf);
  u8_close_output(out);
  return FD_VOID;
}

static int get_loglevel(fdtype level_arg)
{
  if (FD_INTP(level_arg)) return FD_FIX2INT(level_arg);
  else return -1;
}

static fdtype log_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  fdtype level_arg = fd_eval(fd_get_arg(expr,1),env);
  fdtype body = fd_get_body(expr,2);
  int level = get_loglevel(level_arg);
  U8_OUTPUT *out = u8_open_output_string(1024);
  U8_OUTPUT *stream = u8_current_output;
  u8_condition condition = NULL;
  if (FD_THROWP(level_arg)) return level_arg;
  else if (FD_ABORTP(level_arg)) {
    fd_clear_errors(1);}
  else fd_decref(level_arg);
  if ((FD_PAIRP(body))&&(FD_SYMBOLP(FD_CAR(body)))) {
    condition = FD_SYMBOL_NAME(FD_CAR(body));
    body = FD_CDR(body);}
  u8_set_default_output(out);
  while (FD_PAIRP(body)) {
    fdtype value = fast_eval(FD_CAR(body),env);
    if (printout_helper(out,value)) fd_decref(value);
    else {
      u8_set_default_output(stream);
      u8_close_output(out);
      return value;}
    body = FD_CDR(body);}
  u8_set_default_output(stream);
  u8_logger(level,condition,out->u8_outbuf);
  u8_close_output(out);
  return FD_VOID;
}

static fdtype logif_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  fdtype test_expr = fd_get_arg(expr,1), value = FD_FALSE;
  if (FD_ABORTP(test_expr)) return test_expr;
  else if (FD_EXPECT_FALSE(FD_STRINGP(test_expr)))
    return fd_reterr(fd_SyntaxError,"logif_evalfn",
                     _("LOGIF condition expression cannot be a string"),expr);
  else value = fast_eval(test_expr,env);
  if (FD_ABORTP(value)) return value;
  else if ( (FD_FALSEP(value)) || (FD_VOIDP(value)) ||
            (FD_EMPTY_CHOICEP(value)) || (FD_EMPTY_LISTP(value)) )
    return FD_VOID;
  else {
    fdtype body = fd_get_body(expr,2);
    U8_OUTPUT *out = u8_open_output_string(1024);
    U8_OUTPUT *stream = u8_current_output;
    u8_condition condition = NULL;
    if ((FD_PAIRP(body))&&(FD_SYMBOLP(FD_CAR(body)))) {
      condition = FD_SYMBOL_NAME(FD_CAR(body));
      body = FD_CDR(body);}
    fd_decref(value); u8_set_default_output(out);
    while (FD_PAIRP(body)) {
      fdtype value = fast_eval(FD_CAR(body),env);
      if (printout_helper(out,value)) fd_decref(value);
      else {
        u8_set_default_output(stream);
        u8_close_output(out);
        return value;}
      body = FD_CDR(body);}
    u8_set_default_output(stream);
    u8_logger(-10,condition,out->u8_outbuf);
    u8_close_output(out);
    return FD_VOID;}
}

static fdtype logifplus_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  fdtype test_expr = fd_get_arg(expr,1), value = FD_FALSE, loglevel_arg;
  if (FD_ABORTP(test_expr)) return test_expr;
  else if (FD_EXPECT_FALSE(FD_STRINGP(test_expr)))
    return fd_reterr(fd_SyntaxError,"logif_evalfn",
                     _("LOGIF condition expression cannot be a string"),expr);
  else value = fast_eval(test_expr,env);
  if (FD_ABORTP(value)) return value;
  else if ((FD_FALSEP(value)) || (FD_VOIDP(value)) ||
           (FD_EMPTY_CHOICEP(value)) || (FD_EMPTY_LISTP(value)))
    return FD_VOID;
  else loglevel_arg = fd_eval(fd_get_arg(expr,2),env);
  if (FD_ABORTP(loglevel_arg)) return loglevel_arg;
  else if (FD_VOIDP(loglevel_arg))
    return fd_reterr(fd_SyntaxError,"logif_plus_evalfn",
                     _("LOGIF+ loglevel invalid"),expr);
  else if (!(FD_INTP(loglevel_arg)))
    return fd_reterr(fd_TypeError,"logif_plus_evalfn",
                     _("LOGIF+ loglevel invalid"),loglevel_arg);
  else {
    fdtype body = fd_get_body(expr,3);
    U8_OUTPUT *out = u8_open_output_string(1024);
    U8_OUTPUT *stream = u8_current_output;
    int priority = FD_FIX2INT(loglevel_arg);
    u8_condition condition = NULL;
     if ((FD_PAIRP(body))&&(FD_SYMBOLP(FD_CAR(body)))) {
      condition = FD_SYMBOL_NAME(FD_CAR(body));
      body = FD_CDR(body);}
    fd_decref(value); u8_set_default_output(out);
    while (FD_PAIRP(body)) {
      fdtype value = fast_eval(FD_CAR(body),env);
      if (printout_helper(out,value)) fd_decref(value);
      else {
        u8_set_default_output(stream);
        u8_close_output(out);
        return value;}
      body = FD_CDR(body);}
    u8_set_default_output(stream);
    u8_logger(-priority,condition,out->u8_outbuf);
    u8_close_output(out);
    return FD_VOID;}
}

static fdtype stringout_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  struct U8_OUTPUT out; fdtype result; u8_byte buf[256];
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

static fdtype getchar_prim(fdtype port)
{
  U8_INPUT *in = get_input_port(port);
  if (in) {
    int ch = -1;
    if (in) ch = u8_getc(in);
    if (ch<0) return FD_EOF;
    else return FD_CODE2CHAR(ch);}
  else return fd_type_error(_("input port"),"getchar_prim",port);
}

static fdtype getline_prim(fdtype port,fdtype eos_arg,fdtype lim_arg,
                           fdtype eof_marker)
{
  U8_INPUT *in = get_input_port(port);
  if (FD_VOIDP(eof_marker)) eof_marker = FD_EMPTY_CHOICE;
  else if (FD_TRUEP(eof_marker)) eof_marker = FD_EOF;
  else {}
  if (in) {
    u8_string data, eos;
    int lim, size = 0;
    if (in == NULL)
      return fd_type_error(_("input port"),"getline_prim",port);
    if (FD_VOIDP(eos_arg)) eos="\n";
    else if (FD_STRINGP(eos_arg)) eos = FD_STRDATA(eos_arg);
    else return fd_type_error(_("string"),"getline_prim",eos_arg);
    if (FD_VOIDP(lim_arg)) lim = 0;
    else if (FD_FIXNUMP(lim_arg)) lim = FD_FIX2INT(lim_arg);
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
      else return FD_ERROR_VALUE;
    else return fd_incref(eof_marker);}
  else return fd_type_error(_("input port"),"getline_prim",port);
}

static fdtype read_prim(fdtype port)
{
  if (FD_STRINGP(port)) {
    struct U8_INPUT in;
    U8_INIT_STRING_INPUT(&in,FD_STRLEN(port),FD_STRDATA(port));
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

static off_t find_substring(u8_string string,fdtype strings,
                            ssize_t len,ssize_t *lenp);
static ssize_t get_more_data(u8_input in,size_t lim);
static fdtype record_reader(fdtype port,fdtype ends,fdtype limit_arg);

static fdtype read_record_prim(fdtype ports,fdtype ends,fdtype limit_arg)
{
  fdtype results = FD_EMPTY_CHOICE;
  FD_DO_CHOICES(port,ports) {
    fdtype result = record_reader(port,ends,limit_arg);
    FD_ADD_TO_CHOICE(results,result);}
  return results;
}

static fdtype record_reader(fdtype port,fdtype ends,fdtype limit_arg)
{
  U8_INPUT *in = get_input_port(port);
  size_t lim, matchlen = 0;
  off_t off = -1;

  if (in == NULL)
    return fd_type_error(_("input port"),"record_reader",port);
  if (FD_VOIDP(limit_arg)) lim = -1;
  else if (FD_FIXNUMP(limit_arg))
    lim = FD_FIX2INT(limit_arg);
  else return fd_type_error(_("fixnum"),"record_reader",limit_arg);

  if (FD_VOIDP(ends)) {}
  else {
    FD_DO_CHOICES(end,ends)
      if (!((FD_STRINGP(end))||(FD_TYPEP(end,fd_regex_type))))
        return fd_type_error(_("string"),"record_reader",end);}
  while (1) {
    if (FD_VOIDP(ends)) {
      u8_string found = strstr(in->u8_read,"\n");
      if (found) {
        off = found-in->u8_read;
        matchlen = 1;}}
    else off = find_substring(in->u8_read,ends,
                            in->u8_inlim-in->u8_read,
                            &matchlen);
    if (off>=0) {
      size_t record_len = off+matchlen;
      fdtype result = fd_make_string(NULL,record_len,in->u8_read);
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

static off_t find_substring(u8_string string,fdtype strings,
                            ssize_t len_arg,ssize_t *lenp)
{
  ssize_t len = (len_arg<0)?(strlen(string)):(len_arg);
  off_t off = -1; ssize_t matchlen = -1;
  FD_DO_CHOICES(s,strings) {
    if (FD_STRINGP(s)) {
      u8_string next = strstr(string,FD_STRDATA(s));
      if (next) {
        if (off<0) {
          off = next-string; matchlen = FD_STRLEN(s);}
        else if ((next-string)<off) {
          off = next-string;
          if (matchlen<(FD_STRLEN(s))) {
            matchlen = FD_STRLEN(s);}}
        else {}}}
    else if (FD_TYPEP(s,fd_regex_type)) {
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

static fdtype lisp2string(fdtype x)
{
  U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
  fd_unparse(&out,x);
  return fd_stream2string(&out);
}

static fdtype inexact2string(fdtype x,fdtype precision)
{
  if (FD_FLONUMP(x))
    if ((FD_UINTP(precision)) || (FD_VOIDP(precision))) {
      int prec = ((FD_VOIDP(precision)) ? (2) : (FD_FIX2INT(precision)));
      char buf[128]; char cmd[16];
      sprintf(cmd,"%%.%df",prec);
      sprintf(buf,cmd,FD_FLONUM(x));
      return fdtype_string(buf);}
    else return fd_type_error("fixnum","inexact2string",precision);
  else return lisp2string(x);
}

static fdtype number2string(fdtype x,fdtype base)
{
  if (FD_NUMBERP(x)) {
    struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
    fd_output_number(&out,x,fd_getint(base));
    return fd_stream2string(&out);}
  else return fd_err(fd_TypeError,"number2string",NULL,x);
}

static fdtype number2locale(fdtype x,fdtype precision)
{
  if (FD_FLONUMP(x))
    if ((FD_UINTP(precision)) || (FD_VOIDP(precision))) {
      int prec = ((FD_VOIDP(precision)) ? (2) : (FD_FIX2INT(precision)));
      char buf[128]; char cmd[16];
      sprintf(cmd,"%%'.%df",prec);
      sprintf(buf,cmd,FD_FLONUM(x));
      return fdtype_string(buf);}
    else return fd_type_error("fixnum","inexact2string",precision);
  else if (FD_FIXNUMP(x)) {
    char buf[128];
    sprintf(buf,"%'lld",FD_FIX2INT(x));
    return fdtype_string(buf);}
  else return lisp2string(x);
}

static fdtype string2number(fdtype x,fdtype base)
{
  return fd_string2number(FD_STRDATA(x),fd_getint(base));
}

static fdtype just2number(fdtype x,fdtype base)
{
  if (FD_NUMBERP(x)) return fd_incref(x);
  else if (FD_STRINGP(x)) {
    fdtype num = fd_string2number(FD_STRDATA(x),fd_getint(base));
    if (FD_FALSEP(num)) return FD_FALSE;
    else return num;}
  else return fd_type_error(_("string or number"),"->NUMBER",x);
}

/* Printing a backtrace */

static int embeddedp(fdtype focus,fdtype expr)
{
  if (FD_EQ(focus,expr)) return 1;
  else if (FD_PAIRP(expr)) {
    FD_DOLIST(elt,expr)
      if (embeddedp(focus,elt)) return 1;
    return 0;}
  else if (FD_VECTORP(expr)) {
    int i = 0, len = FD_VECTOR_LENGTH(expr);
    while (i<len)
      if (embeddedp(focus,FD_VECTOR_REF(expr,i))) return 1; else i++;
    return 0;}
  else if (FD_CHOICEP(expr)) {
    FD_DO_CHOICES(elt,expr)
      if (embeddedp(focus,elt)) return 1;
    return 0;}
  else if (FD_QCHOICEP(expr)) {
    struct FD_QCHOICE *qc = FD_XQCHOICE(expr);
    FD_DO_CHOICES(elt,qc->qchoiceval)
      if (embeddedp(focus,elt)) return 1;
    return 0;}
  else if (FD_SLOTMAPP(expr)) {
    struct FD_SLOTMAP *sm = FD_XSLOTMAP(expr);
    struct FD_KEYVAL *scan, *limit;
    int slotmap_size;
    fd_read_lock_table(sm);
    slotmap_size = FD_XSLOTMAP_NUSED(sm);
    scan = sm->sm_keyvals; limit = sm->sm_keyvals+slotmap_size;
    while (scan<limit)
      if (embeddedp(focus,scan->kv_key)) {
        fd_unlock_table(sm); return 1;}
      else if (embeddedp(focus,scan->kv_val)) {
        fd_unlock_table(sm); return 1;}
      else scan++;
    fd_unlock_table(sm);
    return 0;}
  else return 0;
}

static fdtype exception_data(u8_exception ex)
{
  if ((ex->u8x_xdata) && (ex->u8x_free_xdata == fd_free_exception_xdata))
    return (fdtype)(ex->u8x_xdata);
  else return FD_VOID;
}

static u8_exception get_innermost_expr(u8_exception ex,fdtype expr)
{
  u8_exception scan = ex; fdtype xdata;
  if (ex == NULL) return ex;
  else xdata = exception_data(scan);
  if ((FD_PAIRP(xdata)) && (embeddedp(xdata,expr))) {
    u8_exception bottom = get_innermost_expr(ex->u8x_prev,xdata);
    if (bottom) return bottom; else return ex;}
  else return NULL;
}

static void print_backtrace_env(U8_OUTPUT *out,u8_exception ex,int width)
{
  fdtype entry = exception_data(ex);
  fdtype keys = fd_getkeys(entry);
  u8_string head = ((ex->u8x_details) ? ((u8_string)(ex->u8x_details)) :
                  (ex->u8x_context) ?  ((u8_string)(ex->u8x_context)) :
                  ((u8_string)""));
  if (FD_ABORTP(keys)) {
    u8_puts(out,head); u8_putc(out,' ');
    fd_unparse(out,entry); u8_putc(out,'\n');}
  else {
    FD_DO_CHOICES(key,keys) {
      fdtype val = fd_get(entry,key,FD_VOID);
      u8_printf(out,";;=%s> %q = %q\n",head,key,val);
      fd_decref(val);}
    fd_decref(keys);}
}

static u8_exception print_backtrace_entry
(U8_OUTPUT *out,u8_exception ex,int width)
{
  if (ex->u8x_context == fd_eval_context) {
    fdtype expr = exception_data(ex);
    if (!(FD_VOIDP(expr))) {
      u8_exception innermost = get_innermost_expr(ex->u8x_prev,expr);
      fdtype focus = ((innermost) ? (exception_data(innermost)) : (FD_VOID));
      u8_puts(out,";;!>> ");
      fd_pprint_focus(out,expr,focus,";;!>> ",0,width,"!",NULL);
      u8_puts(out,"\n");
      if (innermost)return innermost->u8x_prev;
      else return ex->u8x_prev;}}
  else if (ex->u8x_context == fd_apply_context) {
    fdtype entry = exception_data(ex);
    if (!(FD_VOIDP(entry))) {
      int i = 1, lim = FD_VECTOR_LENGTH(entry);
      u8_puts(out,";;*CALL "); fd_unparse(out,FD_VECTOR_REF(entry,0));
      while (i < lim) {
        fdtype arg = FD_VECTOR_REF(entry,i); i++;
        if ((FD_SYMBOLP(arg)) || (FD_PAIRP(arg)))
          u8_puts(out," '");
        else u8_puts(out," ");
        fd_unparse(out,arg);}
      u8_puts(out,"\n");}}
  else if ((ex->u8x_context) && (ex->u8x_context[0]==':')) {
    print_backtrace_env(out,ex,width);}
  else fd_print_exception(out,ex);
  return ex->u8x_prev;
}

static void log_backtrace_env(int loglevel,u8_condition label,
                              u8_exception ex,int width)
{
  fdtype entry = exception_data(ex);
  fdtype keys = fd_getkeys(entry);
  u8_string head = ((ex->u8x_details) ? ((u8_string)(ex->u8x_details)) :
                  (ex->u8x_context) ?  ((u8_string)(ex->u8x_context)) :
                  ((u8_string)""));
  if (FD_ABORTP(keys)) {
    u8_log(loglevel,label,"%s %q\n",head,entry);}
  else {
    struct U8_OUTPUT tmpout; u8_byte buf[16384];
    U8_INIT_OUTPUT_BUF(&tmpout,16384,buf); {
      FD_DO_CHOICES(key,keys) {
        fdtype val = fd_get(entry,key,FD_VOID);
        u8_printf(&tmpout,"> %q = %q\n",key,val);
        fd_decref(val);}
      u8_log(loglevel,label,"%q BINDINGS\n%s",head,tmpout.u8_outbuf);
      u8_close_output(&tmpout);}
    fd_decref(keys);}
}

static u8_exception log_backtrace_entry(int loglevel,u8_condition label,
                                        u8_exception ex,int width)
{
  struct U8_OUTPUT tmpout; u8_byte buf[16384]; 
  U8_INIT_OUTPUT_BUF(&tmpout,16384,buf);
  if (ex->u8x_context == fd_eval_context) {
    fdtype expr = exception_data(ex);
    u8_exception innermost = get_innermost_expr(ex->u8x_prev,expr);
    fdtype focus = ((innermost) ? (exception_data(innermost)) : (FD_VOID));
    u8_puts(&tmpout,"!>> ");
    fd_pprint_focus(&tmpout,expr,focus,"!>> ",0,width,"!>","<!");
    u8_log(loglevel,label,"!>> %s",tmpout.u8_outbuf);
    u8_close_output(&tmpout);
    if (innermost)return innermost->u8x_prev;
    else return ex->u8x_prev;}
  else if (ex->u8x_context == fd_apply_context) {
    fdtype entry = exception_data(ex);
    int i = 1, lim = FD_VECTOR_LENGTH(entry);
    fdtype fn = FD_VECTOR_REF(entry,0);
    u8_puts(&tmpout,"<");
    while (i < lim) {
      fdtype arg = FD_VECTOR_REF(entry,i); i++;
      if ((FD_SYMBOLP(arg)) || (FD_PAIRP(arg)))
        u8_printf(&tmpout," '%q",arg);
      else u8_printf(&tmpout," %q",arg);}
    u8_puts(&tmpout,">");
    u8_log(loglevel,label,"*CALL %q %s",fn,tmpout.u8_outbuf);}
  else if ((ex->u8x_context) && (ex->u8x_context[0]==':')) {
    log_backtrace_env(loglevel,label,ex,width);}
  else if ((ex->u8x_context) && (ex->u8x_details))
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
    fdtype irritant = fd_exception_xdata(scan); int show_irritant = 1;
    if (scan!=ex) u8_puts(out," <");
    if (scan->u8x_cond!=cond) {
      cond = scan->u8x_cond; u8_printf(out," (%m)",cond);}
    if (scan->u8x_context) {
      if (scan->u8x_context == fd_eval_context)
        if ((FD_PAIRP(irritant)) && (!(FD_PAIRP(FD_CAR(irritant))))) {
          u8_printf(out," (%q ...)",FD_CAR(irritant));
          show_irritant = 0;}
        else {}
      else if (scan->u8x_context == fd_apply_context)
        if ((FD_VECTORP(irritant)) && (FD_VECTOR_LENGTH(irritant)>0)) {
          u8_printf(out," %q",FD_VECTOR_REF(irritant,0));
          show_irritant = 0;}
        else {}
      else if ((scan->u8x_context) && (strcmp(scan->u8x_context,":SPROC")==0)) {
        show_irritant = 0;}
      else if ((scan->u8x_context) && (*(scan->u8x_context)==':')) {
        u8_printf(out," %s",scan->u8x_context);
        show_irritant = 0;}
      else u8_printf(out," %s",scan->u8x_context);}
    if (scan->u8x_details)
      u8_printf(out," [%s]",scan->u8x_details);
    if (show_irritant)
      u8_printf(out," <%q>",irritant);
    scan = scan->u8x_prev;}
}


/* Table showing primitives */

static fdtype lisp_show_table(fdtype tables,fdtype slotids,fdtype portarg)
{
  U8_OUTPUT *out = get_output_port(portarg);
  FD_DO_CHOICES(table,tables)
    if ((FD_FALSEP(slotids)) || (FD_VOIDP(slotids)))
      fd_display_table(out,table,FD_VOID);
    else if (FD_OIDP(table)) {
      U8_OUTPUT *tmp = u8_open_output_string(1024);
      u8_printf(out,"%q\n",table);
      {FD_DO_CHOICES(slotid,slotids) {
        fdtype values = fd_frame_get(table,slotid);
        tmp->u8_write = tmp->u8_outbuf; *(tmp->u8_outbuf)='\0';
        u8_printf(tmp,"   %q:   %q\n",slotid,values);
        if (u8_strlen(tmp->u8_outbuf)<80) u8_puts(out,tmp->u8_outbuf);
        else {
          u8_printf(out,"   %q:\n",slotid);
          {FD_DO_CHOICES(value,values) u8_printf(out,"      %q\n",value);}}
        fd_decref(values);}}
      u8_close((u8_stream)tmp);}
    else fd_display_table(out,table,slotids);
  u8_flush(out);
  return FD_VOID;
}

/* PPRINT lisp primitives */

static fdtype lisp_pprint(fdtype x,fdtype portarg,fdtype widtharg,fdtype marginarg)
{
  struct U8_OUTPUT tmpout;
  U8_OUTPUT *out = get_output_port(portarg);
  int width = ((FD_UINTP(widtharg)) ? (FD_FIX2INT(widtharg)) : (60));
  if ((out == NULL)&&(!(FD_FALSEP(portarg))))
    return fd_type_error(_("port"),"lisp_pprint",portarg);
  U8_INIT_OUTPUT(&tmpout,512);
  if (FD_VOIDP(marginarg))
    fd_pprint(&tmpout,x,NULL,0,0,width,1);
  else if (FD_STRINGP(marginarg))
    fd_pprint(&tmpout,x,FD_STRDATA(marginarg),0,0,width,1);
  else if ((FD_UINTP(marginarg))&&(FD_FIX2INT(marginarg)>=0))
    fd_pprint(&tmpout,x,NULL,(FD_FIX2INT(marginarg)),0,width,1);
  else fd_pprint(&tmpout,x,NULL,0,0,width,1);
  if (out) {
    u8_puts(out,tmpout.u8_outbuf); u8_free(tmpout.u8_outbuf);
    u8_flush(out);
    return FD_VOID;}
  else return fd_init_string(NULL,tmpout.u8_write-tmpout.u8_outbuf,tmpout.u8_outbuf);
}

/* Base 64 stuff */

static fdtype from_base64_prim(fdtype string)
{
  const u8_byte *string_data = FD_STRDATA(string);
  unsigned int string_len = FD_STRLEN(string), data_len;
  unsigned char *data=
    u8_read_base64(string_data,string_data+string_len,&data_len);
  if (data)
    return fd_init_packet(NULL,data_len,data);
  else return FD_ERROR_VALUE;
}

static fdtype to_base64_prim(fdtype packet,fdtype nopad,fdtype urisafe)
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
  else return FD_ERROR_VALUE;
}

static fdtype any_to_base64_prim(fdtype arg,fdtype nopad,fdtype urisafe)
{
  unsigned int data_len, ascii_len;
  const u8_byte *data; char *ascii_string;
  if (FD_PACKETP(arg)) {
    data = FD_PACKET_DATA(arg);
    data_len = FD_PACKET_LENGTH(arg);}
  else if ((FD_STRINGP(arg))||(FD_TYPEP(arg,fd_secret_type))) {
    data = FD_STRDATA(arg);
    data_len = FD_STRLEN(arg);}
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
  else return FD_ERROR_VALUE;
}

/* Base 16 stuff */

static fdtype from_base16_prim(fdtype string)
{
  const u8_byte *string_data = FD_STRDATA(string);
  unsigned int string_len = FD_STRLEN(string), data_len;
  unsigned char *data = u8_read_base16(string_data,string_len,&data_len);
  if (data)
    return fd_init_packet(NULL,data_len,data);
  else return FD_ERROR_VALUE;
}

static fdtype to_base16_prim(fdtype packet)
{
  const u8_byte *packet_data = FD_PACKET_DATA(packet);
  unsigned int packet_len = FD_PACKET_LENGTH(packet);
  char *ascii_string = u8_write_base16(packet_data,packet_len);
  if (ascii_string)
    return fd_init_string(NULL,packet_len*2,ascii_string);
  else return FD_ERROR_VALUE;
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

static fdtype gzip_prim(fdtype arg,fdtype filename,fdtype comment)
{
  if (!((FD_STRINGP(arg)||FD_PACKETP(arg))))
    return fd_type_error("string or packet","x2zipfile_prim",arg);
  else {
    fd_exception error = NULL;
    const unsigned char *data=
      ((FD_STRINGP(arg))?(FD_STRDATA(arg)):(FD_PACKET_DATA(arg)));
    unsigned int data_len=
      ((FD_STRINGP(arg))?(FD_STRLEN(arg)):(FD_PACKET_LENGTH(arg)));
    struct FD_OUTBUF out; int flags = 0; /* FDPP_FHCRC */
    time_t now = time(NULL); u8_int4 crc, intval;
    FD_INIT_BYTE_OUTPUT(&out,1024); memset(out.buffer,0,1024);
    fd_write_byte(&out,31); fd_write_byte(&out,139);
    fd_write_byte(&out,8); /* Using default */
    /* Compute flags */
    if ((FD_STRINGP(arg))&&(string_isasciip(FD_STRDATA(arg),FD_STRLEN(arg))))
      flags = flags|FDPP_FASCII;
    if (FD_STRINGP(filename)) flags = flags|FDPP_FNAME;
    if (FD_STRINGP(comment)) flags = flags|FDPP_FCOMMENT;
    fd_write_byte(&out,flags);
    intval = fd_flip_word((unsigned int)now);
    fd_write_4bytes(&out,intval);
    fd_write_byte(&out,2); /* Max compression */
    fd_write_byte(&out,3); /* Assume Unix */
    /* No extra fields */
    if (FD_STRINGP(filename)) {
      u8_string text = FD_STRDATA(filename), end = text+FD_STRLEN(filename);
      int len;
      unsigned char *string=
        u8_localize(latin1_encoding,&text,end,'\\',0,NULL,&len);
      fd_write_bytes(&out,string,len); fd_write_byte(&out,'\0');
      u8_free(string);}
    if (FD_STRINGP(comment)) {
      int len;
      u8_string text = FD_STRDATA(comment), end = text+FD_STRLEN(comment);
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
      fd_seterr(error,"x2zipfile",NULL,FD_VOID);
      u8_free(out.buffer);
      return FD_ERROR_VALUE;}
    crc = u8_crc32(0,data,data_len);
    intval = fd_flip_word(crc); fd_write_4bytes(&out,intval);
    intval = fd_flip_word(data_len); fd_write_4bytes(&out,intval);
    return fd_init_packet(NULL,out.bufwrite-out.buffer,out.buffer);}
}

/* Port type operations */

/* The port type */

static int unparse_port(struct U8_OUTPUT *out,fdtype x)
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
                           -1,FD_VOID,fd_fixnum_type,FD_INT(10)));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2("NUMBER->LOCALE",number2locale,1));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("STRING->NUMBER",string2number,1,
                           fd_string_type,FD_VOID,
                           fd_fixnum_type,FD_INT(-1)));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("->NUMBER",just2number,1,
                           -1,FD_VOID,fd_fixnum_type,FD_INT(-1)));

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

  fd_defspecial(fd_scheme_module,"PRINTOUT",printout_evalfn);
  fd_defspecial(fd_scheme_module,"LINEOUT",lineout_evalfn);
  fd_defspecial(fd_scheme_module,"STRINGOUT",stringout_evalfn);
  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("SUBSTRINGOUT",substringout,1,
                           fd_string_type,FD_VOID,
                           fd_fixnum_type,FD_VOID,
                           fd_fixnum_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("UNISCAPE",uniscape,1,
                           fd_string_type,FD_VOID,
                           fd_string_type,FD_VOID));

  /* Logging functions for specific levels */
  fd_defspecial(fd_scheme_module,"NOTIFY",notify_evalfn);
  fd_defspecial(fd_scheme_module,"STATUS",status_evalfn);
  fd_defspecial(fd_scheme_module,"WARNING",warning_evalfn);

  /* Generic logging function, always outputs */
  fd_defspecial(fd_scheme_module,"MESSAGE",message_evalfn);
  fd_defspecial(fd_scheme_module,"%LOGGER",message_evalfn);

  /* Logging with message level */
  fd_defspecial(fd_scheme_module,"LOGMSG",log_evalfn);
  /* Conditional logging */
  fd_defspecial(fd_scheme_module,"LOGIF",logif_evalfn);
  /* Conditional logging with priority level */
  fd_defspecial(fd_scheme_module,"LOGIF+",logifplus_evalfn);

  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("PACKET->DTYPE",packet2dtype,1,
                           fd_packet_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("DTYPE->PACKET",dtype2packet,1,
                           -1,FD_VOID,fd_fixnum_type,FD_INT(128)));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("BASE64->PACKET",from_base64_prim,1,
                           fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("PACKET->BASE64",to_base64_prim,1,
                           fd_packet_type,FD_VOID,-1,FD_FALSE,-1,FD_FALSE));
  fd_idefn(fd_scheme_module,fd_make_cprim3("->BASE64",any_to_base64_prim,1));

  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("BASE16->PACKET",from_base16_prim,1,
                           fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("PACKET->BASE16",to_base16_prim,1,
                           fd_packet_type,FD_VOID));

  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("GZIP",gzip_prim,1,-1,FD_VOID,
                           fd_string_type,FD_VOID,
                           fd_string_type,FD_VOID));

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("%SHOW",lisp_show_table,1)));

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
