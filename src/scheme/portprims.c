/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2016 beingmeta, inc.
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
#include "framerd/fddb.h"
#include "framerd/pools.h"
#include "framerd/indices.h"
#include "framerd/frames.h"
#include "framerd/dtypestream.h"
#include "framerd/dtypeio.h"
#include "framerd/ports.h"

#include <libu8/u8streamio.h>
#include <libu8/u8crypto.h>

#include <zlib.h>

#define FD_DEFAULT_ZLEVEL 9

fd_exception fd_UnknownEncoding=_("Unknown encoding");

/* The port type */

fd_ptr_type fd_port_type;

static int unparse_port(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_PORT *p=FD_GET_CONS(x,fd_port_type,fd_port);
  if ((p->in) && (p->out) && (p->id))
    u8_printf(out,"#<I/O Port (%s) #!%x>",p->id,x);
  else if ((p->in) && (p->out))
    u8_printf(out,"#<I/O Port #!%x>",x);
  else if ((p->in)&&(p->id))
    u8_printf(out,"#<Input Port (%s) #!%x>",p->id,x);
  else if (p->in)
    u8_printf(out,"#<Input Port #!%x>",x);
  else if (p->id)
    u8_printf(out,"#<Output Port (%s) #!%x>",p->id,x);
  else u8_printf(out,"#<Output Port #!%x>",x);
  return 1;
}

static void recycle_port(struct FD_CONS *c)
{
  struct FD_PORT *p=(struct FD_PORT *)c;
  if (p->in) {
    u8_close_input(p->in);}
  if (p->out) {
    u8_close_output(p->out);}
  if (p->id) u8_free(p->id);
 if (FD_MALLOCD_CONSP(c)) u8_free(c);
}

/* Making ports */

static fdtype make_port(U8_INPUT *in,U8_OUTPUT *out,u8_string id)
{
  struct FD_PORT *port=u8_alloc(struct FD_PORT);
  FD_INIT_CONS(port,fd_port_type);
  port->in=in; port->out=out; port->id=id;
  return FDTYPE_CONS(port);
}

static u8_output get_output_port(fdtype portarg)
{
  if ((FD_VOIDP(portarg))||(FD_TRUEP(portarg)))
    return u8_current_output;
  else if (FD_PORTP(portarg)) {
    struct FD_PORT *p=
      FD_GET_CONS(portarg,fd_port_type,struct FD_PORT *);
    return p->out;}
  else return NULL;
}

static u8_input get_input_port(fdtype portarg)
{
  if (FD_VOIDP(portarg))
    return NULL; /* get_default_output(); */
  else if (FD_PORTP(portarg)) {
    struct FD_PORT *p=
      FD_GET_CONS(portarg,fd_port_type,struct FD_PORT *);
    return p->in;}
  else return NULL;
}

/* Identifying end of file */

static fdtype eofp(fdtype x)
{
  if (FD_EOFP(x)) return FD_TRUE; else return FD_FALSE;
}

/* DTYPE streams */

fd_ptr_type fd_dtstream_type;

static int unparse_dtstream(struct U8_OUTPUT *out,fdtype x)
{
  u8_printf(out,"#<DTStream #!%x>",x);
  return 1;
}

static void recycle_dtstream(struct FD_CONS *c)
{
  struct FD_DTSTREAM *p=(struct FD_DTSTREAM *)c;
  if (p->dt_stream) {
    fd_dtsclose(p->dt_stream,p->owns_socket);}
  if (FD_MALLOCD_CONSP(c)) u8_free(c);
}

static fdtype read_dtype(fdtype stream)
{
  struct FD_DTSTREAM *ds=
    FD_GET_CONS(stream,fd_dtstream_type,struct FD_DTSTREAM *);
  fdtype object=fd_dtsread_dtype(ds->dt_stream);
  if (object == FD_EOD) return FD_EOF;
  else return object;
}

static fdtype write_dtype(fdtype object,fdtype stream)
{
  struct FD_DTSTREAM *ds=
    FD_GET_CONS(stream,fd_dtstream_type,struct FD_DTSTREAM *);
  int bytes=fd_dtswrite_dtype(ds->dt_stream,object);
  if (bytes<0) return FD_ERROR_VALUE;
  else return FD_INT(bytes);
}

static fdtype write_bytes(fdtype object,fdtype stream)
{
  struct FD_DTSTREAM *ds=
    FD_GET_CONS(stream,fd_dtstream_type,struct FD_DTSTREAM *);
  if (FD_STRINGP(object)) {
    fd_dtswrite_bytes(ds->dt_stream,FD_STRDATA(object),FD_STRLEN(object));
    return FD_STRLEN(object);}
  else if (FD_PACKETP(object)) {
    fd_dtswrite_bytes
      (ds->dt_stream,FD_PACKET_DATA(object),FD_PACKET_LENGTH(object));
    return FD_PACKET_LENGTH(object);}
  else {
    int bytes=fd_dtswrite_dtype(ds->dt_stream,object);
    if (bytes<0) return FD_ERROR_VALUE;
    else return FD_INT(bytes);}
}

static fdtype packet2dtype(fdtype packet)
{
  fdtype object;
  struct FD_BYTE_INPUT in;
  FD_INIT_BYTE_INPUT(&in,FD_PACKET_DATA(packet),
                     FD_PACKET_LENGTH(packet));
  object=fd_read_dtype(&in);
  return object;
}

static fdtype dtype2packet(fdtype object,fdtype initsize)
{
  int size=FD_FIX2INT(initsize);
  struct FD_BYTE_OUTPUT out;
  FD_INIT_BYTE_OUTPUT(&out,size);
  int bytes=fd_write_dtype(&out,object);
  if (bytes<0) return FD_ERROR_VALUE;
  else return fd_init_packet(NULL,bytes,out.start);
}

static fdtype read_int(fdtype stream)
{
  struct FD_DTSTREAM *ds=
    FD_GET_CONS(stream,fd_dtstream_type,struct FD_DTSTREAM *);
  unsigned int ival=fd_dtsread_4bytes(ds->dt_stream);
  return FD_INT(ival);
}

static fdtype write_int(fdtype object,fdtype stream)
{
  struct FD_DTSTREAM *ds=
    FD_GET_CONS(stream,fd_dtstream_type,struct FD_DTSTREAM *);
  int ival=fd_getint(object);
  int bytes=fd_dtswrite_4bytes(ds->dt_stream,ival);
  if (bytes<0) return FD_ERROR_VALUE;
  else return FD_INT(bytes);
}

static fdtype zread_dtype(fdtype stream)
{
  struct FD_DTSTREAM *ds=
    FD_GET_CONS(stream,fd_dtstream_type,struct FD_DTSTREAM *);
  fdtype object=fd_zread_dtype(ds->dt_stream);
  if (object == FD_EOD) return FD_EOF;
  else return object;
}

static fdtype zwrite_dtype(fdtype object,fdtype stream)
{
  struct FD_DTSTREAM *ds=
    FD_GET_CONS(stream,fd_dtstream_type,struct FD_DTSTREAM *);
  int bytes=fd_zwrite_dtype(ds->dt_stream,object);
  if (bytes<0) return FD_ERROR_VALUE;
  else return FD_INT(bytes);
}

static fdtype zwrite_dtypes(fdtype object,fdtype stream)
{
  struct FD_DTSTREAM *ds=
    FD_GET_CONS(stream,fd_dtstream_type,struct FD_DTSTREAM *);
  int bytes=fd_zwrite_dtypes(ds->dt_stream,object);
  if (bytes<0) return FD_ERROR_VALUE;
  else return FD_INT(bytes);
}

static fdtype zread_int(fdtype stream)
{
  struct FD_DTSTREAM *ds=
    FD_GET_CONS(stream,fd_dtstream_type,struct FD_DTSTREAM *);
  unsigned int ival=fd_dtsread_zint(ds->dt_stream);
  return FD_INT(ival);
}

static fdtype zwrite_int(fdtype object,fdtype stream)
{
  struct FD_DTSTREAM *ds=
    FD_GET_CONS(stream,fd_dtstream_type,struct FD_DTSTREAM *);
  int ival=fd_getint(object);
  int bytes=fd_dtswrite_zint(ds->dt_stream,ival);
  if (bytes<0) return FD_ERROR_VALUE;
  else return FD_INT(bytes);
}

/* Output strings */

static fdtype open_output_string()
{
  U8_OUTPUT *out=u8_alloc(struct U8_OUTPUT);
  U8_INIT_OUTPUT(out,256);
  return make_port(NULL,out,u8_strdup("output string"));
}

static fdtype open_input_string(fdtype arg)
{
  if (FD_STRINGP(arg)) {
    U8_INPUT *in=u8_alloc(struct U8_INPUT);
    U8_INIT_STRING_INPUT(in,FD_STRING_LENGTH(arg),u8_strdup(FD_STRDATA(arg)));
    in->u8_streaminfo=in->u8_streaminfo|U8_STREAM_OWNS_BUF;
    return make_port(in,NULL,u8_strdup("input string"));}
  else return fd_type_error(_("string"),"open_input_string",arg);
}

static fdtype portid(fdtype port_arg)
{
  if (FD_PORTP(port_arg)) {
    struct FD_PORT *port=(struct FD_PORT *)port_arg;
    if (port->id) return fdtype_string(port->id);
    else return FD_FALSE;}
  else return fd_type_error(_("port"),"portid",port_arg);
}

static fdtype portdata(fdtype port_arg)
{
  if (FD_PORTP(port_arg)) {
    struct FD_PORT *port=(struct FD_PORT *)port_arg;
    if (port->out)
      return fd_substring(port->out->u8_outbuf,port->out->u8_outptr);
    else return fd_substring(port->out->u8_outbuf,port->out->u8_outlim);}
  else return fd_type_error(_("port"),"portdata",port_arg);
}

/* Simple STDIO */

static fdtype write_prim(fdtype x,fdtype portarg)
{
  U8_OUTPUT *out=get_output_port(portarg);
  if (out) {
    u8_printf(out,"%q",x);
    u8_flush(out);
    return FD_VOID;}
  else return fd_type_error(_("output port"),"write_prim",portarg);
}

static fdtype display_prim(fdtype x,fdtype portarg)
{
  U8_OUTPUT *out=get_output_port(portarg);
  if (out) {
    if (FD_STRINGP(x))
      u8_printf(out,"%s",FD_STRDATA(x));
    else u8_printf(out,"%q",x);
    u8_flush(out);
    return FD_VOID;}
  else return fd_type_error(_("output port"),"display_prim",portarg);
}

static fdtype putchar_prim(fdtype char_arg,fdtype port)
{
  int ch;
  U8_OUTPUT *out=get_output_port(port);
  if (out) {
    if (FD_CHARACTERP(char_arg))
      ch=FD_CHAR2CODE(char_arg);
    else if (FD_FIXNUMP(char_arg))
      ch=FD_FIX2INT(char_arg);
    else return fd_type_error("character","putchar_prim",char_arg);
    u8_putc(out,ch);
    return FD_VOID;}
  else return fd_type_error(_("output port"),"putchar_prim",port);
}

static fdtype newline_prim(fdtype portarg)
{
  U8_OUTPUT *out=get_output_port(portarg);
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
  if (out == NULL) out=u8_current_output;
  if (FD_STRINGP(x))
    u8_printf(out,"%s",FD_STRDATA(x));
  else u8_printf(out,"%q",x);
  return 1;
}

FD_EXPORT
fdtype fd_printout(fdtype body,fd_lispenv env)
{
  U8_OUTPUT *out=u8_current_output;
  while (FD_PAIRP(body)) {
    fdtype value=fasteval(FD_CAR(body),env);
    if (printout_helper(out,value)) fd_decref(value);
    else return value;
    body=FD_CDR(body);}
  u8_flush(out);
  return FD_VOID;
}

FD_EXPORT
fdtype fd_printout_to(U8_OUTPUT *out,fdtype body,fd_lispenv env)
{
  u8_output prev=u8_current_output;
  u8_set_default_output(out);
  while (FD_PAIRP(body)) {
    fdtype value=fasteval(FD_CAR(body),env);
    if (printout_helper(out,value)) fd_decref(value);
    else {
      u8_flush(out);
      u8_set_default_output(prev);
      return value;}
    body=FD_CDR(body);}
  u8_flush(out);
  u8_set_default_output(prev);
  return FD_VOID;
}

static fdtype substringout(fdtype arg,fdtype start,fdtype end)
{
  u8_output output=u8_current_output;
  u8_string string=FD_STRDATA(arg); unsigned int len=FD_STRLEN(arg);
  if (FD_VOIDP(start)) u8_putn(output,string,len);
  else if (FD_VOIDP(end)) {
    unsigned int byte_start=u8_byteoffset(string,FD_FIX2INT(start),len);
    u8_putn(output,string+byte_start,len-byte_start);}
  else {
    unsigned int byte_start=u8_byteoffset(string,FD_FIX2INT(start),len);
    unsigned int byte_end=u8_byteoffset(string,FD_FIX2INT(end),len);
    u8_putn(output,string+byte_start,byte_end-byte_start);}
  return FD_VOID;
}

static fdtype uniscape(fdtype arg,fdtype excluding)
{
  u8_string input=((FD_STRINGP(arg))?(FD_STRDATA(arg)):
                   (fd_dtype2string(arg)));
  u8_string exstring=((FD_STRINGP(excluding))?
                      (FD_STRDATA(excluding)):
                      ((u8_string)""));
  u8_output output=u8_current_output;
  u8_string string=input;
  const u8_byte *scan=string;
  int c=u8_sgetc(&scan);
  while (c>0) {
    if ((c>=0x80)||(strchr(exstring,c))) {
      u8_printf(output,"\\u%04x",c);}
    else u8_putc(output,c);
    c=u8_sgetc(&scan);}
  if (!(FD_STRINGP(arg))) u8_free(input);
  return FD_VOID;
}

static fdtype printout_handler(fdtype expr,fd_lispenv env)
{
  return fd_printout(fd_get_body(expr,1),env);
}
static fdtype lineout_handler(fdtype expr,fd_lispenv env)
{
  U8_OUTPUT *out=u8_current_output;
  fdtype value=fd_printout(fd_get_body(expr,1),env);
  if (FD_ABORTP(value)) return value;
  u8_printf(out,"\n");
  u8_flush(out);
  return FD_VOID;
}

static fdtype message_handler(fdtype expr,fd_lispenv env)
{
  fdtype body=fd_get_body(expr,1);
  U8_OUTPUT *out=u8_open_output_string(1024);
  U8_OUTPUT *stream=u8_current_output;
  u8_set_default_output(out);
  while (FD_PAIRP(body)) {
    fdtype value=fasteval(FD_CAR(body),env);
    if (printout_helper(out,value)) fd_decref(value);
    else {
      u8_set_default_output(stream);
      u8_close_output(out);
      return value;}
    body=FD_CDR(body);}
  u8_set_default_output(stream);
  u8_logger(-10,NULL,out->u8_outbuf);
  u8_close_output(out);
  return FD_VOID;
}

static fdtype notify_handler(fdtype expr,fd_lispenv env)
{
  fdtype body=fd_get_body(expr,1);
  U8_OUTPUT *out=u8_open_output_string(1024);
  U8_OUTPUT *stream=u8_current_output;
  u8_set_default_output(out);
  while (FD_PAIRP(body)) {
    fdtype value=fasteval(FD_CAR(body),env);
    if (printout_helper(out,value)) fd_decref(value);
    else {
      u8_set_default_output(stream);
      u8_close_output(out);
      return value;}
    body=FD_CDR(body);}
  u8_set_default_output(stream);
  u8_logger(LOG_NOTICE,NULL,out->u8_outbuf);
  u8_close_output(out);
  return FD_VOID;
}

static fdtype status_handler(fdtype expr,fd_lispenv env)
{
  fdtype body=fd_get_body(expr,1);
  U8_OUTPUT *out=u8_open_output_string(1024);
  U8_OUTPUT *stream=u8_current_output;
  u8_set_default_output(out);
  while (FD_PAIRP(body)) {
    fdtype value=fasteval(FD_CAR(body),env);
    if (printout_helper(out,value)) fd_decref(value);
    else {
      u8_set_default_output(stream);
      u8_close_output(out);
      return value;}
    body=FD_CDR(body);}
  u8_set_default_output(stream);
  u8_logger(LOG_INFO,NULL,out->u8_outbuf);
  u8_close_output(out);
  return FD_VOID;
}

static fdtype warning_handler(fdtype expr,fd_lispenv env)
{
  fdtype body=fd_get_body(expr,1);
  U8_OUTPUT *out=u8_open_output_string(1024);
  U8_OUTPUT *stream=u8_current_output;
  u8_set_default_output(out);
  while (FD_PAIRP(body)) {
    fdtype value=fasteval(FD_CAR(body),env);
    if (printout_helper(out,value)) fd_decref(value);
    else {
      u8_set_default_output(stream);
      u8_close_output(out);
      return value;}
    body=FD_CDR(body);}
  u8_set_default_output(stream);
  u8_logger(LOG_WARN,NULL,out->u8_outbuf);
  u8_close_output(out);
  return FD_VOID;
}

static int get_loglevel(fdtype level_arg)
{
  if (FD_FIXNUMP(level_arg)) return FD_FIX2INT(level_arg);
  else return -1;
}

static fdtype log_handler(fdtype expr,fd_lispenv env)
{
  fdtype level_arg=fd_eval(fd_get_arg(expr,1),env);
  fdtype body=fd_get_body(expr,2);
  int level=get_loglevel(level_arg);
  U8_OUTPUT *out=u8_open_output_string(1024);
  U8_OUTPUT *stream=u8_current_output;
  u8_condition condition=NULL;
  if (FD_THROWP(level_arg)) return level_arg;
  else if (FD_ABORTP(level_arg)) {
    fd_clear_errors(1);}
  else fd_decref(level_arg);
  if ((FD_PAIRP(body))&&(FD_SYMBOLP(FD_CAR(body)))) {
    condition=FD_SYMBOL_NAME(FD_CAR(body));
    body=FD_CDR(body);}
  u8_set_default_output(out);
  while (FD_PAIRP(body)) {
    fdtype value=fasteval(FD_CAR(body),env);
    if (printout_helper(out,value)) fd_decref(value);
    else {
      u8_set_default_output(stream);
      u8_close_output(out);
      return value;}
    body=FD_CDR(body);}
  u8_set_default_output(stream);
  u8_logger(level,condition,out->u8_outbuf);
  u8_close_output(out);
  return FD_VOID;
}

static fdtype logif_handler(fdtype expr,fd_lispenv env)
{
  fdtype test_expr=fd_get_arg(expr,1), value=FD_FALSE;
  if (FD_ABORTP(test_expr)) return test_expr;
  else if (FD_EXPECT_FALSE(FD_STRINGP(test_expr)))
    return fd_reterr(fd_SyntaxError,"logif_handler",_("LOGIF condition expression cannot be a string"),expr);
  else value=fasteval(test_expr,env);
  if (FD_ABORTP(value)) return value;
  else if ((FD_FALSEP(value)) || (FD_VOIDP(value)) || (FD_EMPTY_CHOICEP(value)) || (FD_EMPTY_LISTP(value)))
    return FD_VOID;
  else {
    fdtype body=fd_get_body(expr,2);
    U8_OUTPUT *out=u8_open_output_string(1024);
    U8_OUTPUT *stream=u8_current_output;
    u8_condition condition=NULL;
    if ((FD_PAIRP(body))&&(FD_SYMBOLP(FD_CAR(body)))) {
      condition=FD_SYMBOL_NAME(FD_CAR(body));
      body=FD_CDR(body);}
    fd_decref(value); u8_set_default_output(out);
    while (FD_PAIRP(body)) {
      fdtype value=fasteval(FD_CAR(body),env);
      if (printout_helper(out,value)) fd_decref(value);
      else {
        u8_set_default_output(stream);
        u8_close_output(out);
        return value;}
      body=FD_CDR(body);}
    u8_set_default_output(stream);
    u8_logger(-10,condition,out->u8_outbuf);
    u8_close_output(out);
    return FD_VOID;}
}

static fdtype logifplus_handler(fdtype expr,fd_lispenv env)
{
  fdtype test_expr=fd_get_arg(expr,1), value=FD_FALSE, loglevel_arg;
  if (FD_ABORTP(test_expr)) return test_expr;
  else if (FD_EXPECT_FALSE(FD_STRINGP(test_expr)))
    return fd_reterr(fd_SyntaxError,"logif_handler",_("LOGIF condition expression cannot be a string"),expr);
  else value=fasteval(test_expr,env);
  if (FD_ABORTP(value)) return value;
  else if ((FD_FALSEP(value)) || (FD_VOIDP(value)) ||
           (FD_EMPTY_CHOICEP(value)) || (FD_EMPTY_LISTP(value)))
    return FD_VOID;
  else loglevel_arg=fd_eval(fd_get_arg(expr,2),env);
  if (FD_ABORTP(loglevel_arg)) return loglevel_arg;
  else if (FD_VOIDP(loglevel_arg))
    return fd_reterr(fd_SyntaxError,"logif_plus_handler",
                     _("LOGIF+ loglevel invalid"),expr);
  else if (!(FD_FIXNUMP(loglevel_arg)))
    return fd_reterr(fd_TypeError,"logif_plus_handler",
                     _("LOGIF+ loglevel invalid"),loglevel_arg);
  else {
    fdtype body=fd_get_body(expr,3);
    U8_OUTPUT *out=u8_open_output_string(1024);
    U8_OUTPUT *stream=u8_current_output;
    int priority=FD_FIX2INT(loglevel_arg);
    u8_condition condition=NULL;
     if ((FD_PAIRP(body))&&(FD_SYMBOLP(FD_CAR(body)))) {
      condition=FD_SYMBOL_NAME(FD_CAR(body));
      body=FD_CDR(body);}
    fd_decref(value); u8_set_default_output(out);
    while (FD_PAIRP(body)) {
      fdtype value=fasteval(FD_CAR(body),env);
      if (printout_helper(out,value)) fd_decref(value);
      else {
        u8_set_default_output(stream);
        u8_close_output(out);
        return value;}
      body=FD_CDR(body);}
    u8_set_default_output(stream);
    u8_logger(-priority,condition,out->u8_outbuf);
    u8_close_output(out);
    return FD_VOID;}
}

static fdtype stringout_handler(fdtype expr,fd_lispenv env)
{
  struct U8_OUTPUT out; fdtype result; u8_byte buf[256];
  U8_INIT_OUTPUT_X(&out,256,buf,0);
  result=fd_printout_to(&out,fd_get_body(expr,1),env);
  if (!(FD_ABORTP(result))) {
    fd_decref(result);
    result=fd_make_string
      (NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);}
  u8_close_output(&out);
  return result;
}

/* Input operations! */

static fdtype getchar_prim(fdtype port)
{
  U8_INPUT *in=get_input_port(port);
  if (in) {
    int ch=-1;
    if (in) ch=u8_getc(in);
    if (ch<0) return FD_EOF;
    else return FD_CODE2CHAR(ch);}
  else return fd_type_error(_("input port"),"getchar_prim",port);
}

static fdtype getline_prim(fdtype port,fdtype eos_arg,fdtype lim_arg,
                           fdtype eof_marker)
{
  U8_INPUT *in=get_input_port(port);
  if (FD_VOIDP(eof_marker)) eof_marker=FD_EMPTY_CHOICE;
  else if (FD_TRUEP(eof_marker)) eof_marker=FD_EOF;
  else {}
  if (in) {
    u8_string data, eos; int lim, size=0;
    if (in==NULL)
      return fd_type_error(_("input port"),"getline_prim",port);
    if (FD_VOIDP(eos_arg)) eos="\n";
    else if (FD_STRINGP(eos_arg)) eos=FD_STRDATA(eos_arg);
    else return fd_type_error(_("string"),"getline_prim",eos_arg);
    if (FD_VOIDP(lim_arg)) lim=0;
    else if (FD_FIXNUMP(lim_arg)) lim=FD_FIX2INT(lim_arg);
    else return fd_type_error(_("fixum"),"getline_prim",eos_arg);
    data=u8_gets_x(NULL,lim,in,eos,&size);
    if (data)
      if (strlen(data)<size) {
        /* Handle embedded NUL */
        struct U8_OUTPUT out;
        const u8_byte *scan=data, *limit=scan+size;
        U8_INIT_OUTPUT(&out,size+8);
        while (scan<limit) {
          if (*scan)
            u8_putc(&out,u8_sgetc(&scan));
          else u8_putc(&out,0);}
        u8_free(data);
        return fd_stream2string(&out);}
      else return fd_init_string(NULL,size,data);
    else if (size<0)
      if (errno==EAGAIN)
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
    U8_INPUT *in=get_input_port(port);
    if (in) {
      int c=fd_skip_whitespace(in);
      if (c<0) return FD_EOF;
      else return fd_parser(in);}
    else return fd_type_error(_("input port"),"read_prim",port);}
}

static int find_substring(u8_string string,fdtype strings,int *lenp)
{
  int off=-1, matchlen=-1;
  FD_DO_CHOICES(s,strings) {
    u8_string next=strstr(string,FD_STRDATA(s));
    if (next) {
      if (off<0) {
        off=next-string; matchlen=FD_STRLEN(s);}
      else if ((next-string)<off) {
        off=next-string;
        if (matchlen<(FD_STRLEN(s))) {
          matchlen=FD_STRLEN(s);}}
      else {}}}
  *lenp=matchlen;
  return off;
}
static fdtype record_reader(fdtype port,fdtype ends,fdtype limit_arg)
{
  int lim, off=-1, matchlen=0;
  U8_INPUT *in=get_input_port(port);
  if (in==NULL)
    return fd_type_error(_("input port"),"record_reader",port);
  if (FD_VOIDP(limit_arg)) lim=0;
  else if (FD_FIXNUMP(limit_arg)) lim=FD_FIX2INT(limit_arg);
  else return fd_type_error(_("fixnum"),"record_reader",limit_arg);
  if (FD_VOIDP(ends)) {}
  else {
    FD_DO_CHOICES(end,ends)
      if (!(FD_STRINGP(end)))
        return fd_type_error(_("string"),"record_reader",end);}
  while (1) {
    if (FD_VOIDP(ends)) {
      u8_string found=strstr(in->u8_inptr,"\n");
      if (found) {off=found-in->u8_inptr; matchlen=1;}}
    else off=find_substring(in->u8_inptr,ends,&matchlen);
    if (off>=0) {
      int record_len=off+matchlen;
      u8_byte *buf=u8_malloc(record_len+1);
      int bytes_read=u8_getn(buf,record_len,in);
      return fd_init_string(NULL,bytes_read,buf);}
    else if ((lim) && ((in->u8_inlim-in->u8_inptr)>lim))
      return FD_EOF;
    else if (in->u8_fillfn) {
      int delta=in->u8_fillfn(in);
      if (delta==0) return FD_EOF;}
    else return FD_EOF;}
}

static fdtype read_record_prim(fdtype ports,fdtype ends,fdtype limit_arg)
{
  fdtype results=FD_EMPTY_CHOICE;
  FD_DO_CHOICES(port,ports) {
    fdtype result=record_reader(port,ends,limit_arg);
    FD_ADD_TO_CHOICE(results,result);}
  return results;
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
    if ((FD_FIXNUMP(precision)) || (FD_VOIDP(precision))) {
      int prec=((FD_VOIDP(precision)) ? (2) : (FD_FIX2INT(precision)));
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
    if ((FD_FIXNUMP(precision)) || (FD_VOIDP(precision))) {
      int prec=((FD_VOIDP(precision)) ? (2) : (FD_FIX2INT(precision)));
      char buf[128]; char cmd[16];
      sprintf(cmd,"%%'.%df",prec);
      sprintf(buf,cmd,FD_FLONUM(x));
      return fdtype_string(buf);}
    else return fd_type_error("fixnum","inexact2string",precision);
  else if (FD_FIXNUMP(x)) {
    char buf[128];
    sprintf(buf,"%'d",FD_FIX2INT(x));
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
    fdtype num=fd_string2number(FD_STRDATA(x),fd_getint(base));
    if (FD_FALSEP(num)) return FD_FALSE;
    else return num;}
  else return fd_type_error(_("string or number"),"->NUMBER",x);
}

/* Pretty printing */

static fdtype quote_symbol, comment_symbol;
static fdtype unquote_symbol, quasiquote_symbol, unquote_star_symbol;

static int output_keyval(u8_output out,fdtype key,fdtype val,
                         int col,int maxcol,int first_kv);

#define PPRINT_ATOMICP(x) \
  (!((FD_PAIRP(x)) || (FD_VECTORP(x)) || (FD_SLOTMAPP(x)) || \
     (FD_CHOICEP(x)) || (FD_ACHOICEP(x)) || (FD_QCHOICEP(x))))

FD_EXPORT
int fd_pprint(u8_output out,fdtype x,u8_string prefix,
              int indent,int col,int maxcol,int is_initial)
{
  int startoff=out->u8_outptr-out->u8_outbuf, n_chars;
  if (is_initial==0) u8_putc(out,' ');
  fd_unparse(out,x); n_chars=u8_strlen(out->u8_outbuf+startoff);
  /* If we're not going to descend, and it all fits, just return the
     new column position. */
  if ((n_chars<5)||
      ((PPRINT_ATOMICP(x)) && ((is_initial) || (col+n_chars<maxcol))))
    return col+n_chars;
  /* Otherwise, reset the stream pointer. */
  out->u8_outptr=out->u8_outbuf+startoff; out->u8_outbuf[startoff]='\0';
  /* Newline and indent if you're non-initial and ran out of space. */
  if ((is_initial==0) && (col+n_chars>=maxcol)) {
    int i=indent; u8_putc(out,'\n');
    if (prefix) u8_puts(out,prefix);
    while (i>0) {u8_putc(out,' '); i--;}
    col=indent+((prefix) ? (u8_strlen(prefix)) : (0));
    startoff=out->u8_outptr-out->u8_outbuf;}
  else if (is_initial==0) u8_putc(out,' ');
  /* Handle quote, quasiquote and friends */
  if ((FD_PAIRP(x)) && (FD_SYMBOLP(FD_CAR(x))) &&
      (FD_PAIRP(FD_CDR(x))) && (FD_EMPTY_LISTP(FD_CDR(FD_CDR(x))))) {
    fdtype car=FD_CAR(x);
    if (FD_EQ(car,quote_symbol)) {
      u8_putc(out,'\''); indent++; x=FD_CAR(FD_CDR(x));}
    else if (FD_EQ(car,unquote_symbol)) {
      indent++; u8_putc(out,','); x=FD_CAR(FD_CDR(x));}
    else if (FD_EQ(car,quasiquote_symbol)) {
      indent++; u8_putc(out,'`'); x=FD_CAR(FD_CDR(x));}
    else if (FD_EQ(car,unquote_star_symbol)) {
      indent++; indent++; u8_puts(out,",@"); x=FD_CAR(FD_CDR(x));}
    else if (FD_EQ(car,comment_symbol)) {
      u8_puts(out,"#;"); indent=indent+2; x=FD_CAR(FD_CDR(x));}}
  /* Special compound printers for different types. */
  if (FD_PAIRP(x)) {
    fdtype car=FD_CAR(x), scan=x; int first=1;
    if (FD_SYMBOLP(car)) indent=indent+2;
    u8_putc(out,'('); col++; indent++;
    while (FD_PAIRP(scan)) {
      col=fd_pprint(out,FD_CAR(scan),prefix,indent,col,maxcol,first);
      first=0; scan=FD_CDR(scan);}
    if (FD_EMPTY_LISTP(scan)) {
      u8_putc(out,')');  return col+1;}
    else {
      startoff=out->u8_outptr-out->u8_outbuf;
      u8_printf(out," . %q)",scan);
      n_chars=u8_strlen(out->u8_outbuf+startoff);
      if (col+n_chars>maxcol) {
        int i=indent;
        out->u8_outptr=out->u8_outbuf+startoff;
        out->u8_outbuf[startoff]='\0';
        u8_putc(out,'\n');
        if (prefix) u8_puts(out,prefix);
        while (i>0) {u8_putc(out,' '); i--;}
        u8_printf(out,". ");
        col=indent+2+((prefix) ? (u8_strlen(prefix)) : (0));
        col=fd_pprint(out,scan,prefix,indent+2,col,maxcol,1);
        u8_putc(out,')'); return col+1;}
      else return col+n_chars;}}
  else if (FD_VECTORP(x)) {
    int len=FD_VECTOR_LENGTH(x);
    if (len==0) {u8_printf(out,"#()"); return col+3;}
    else {
      int eltno=0;
      u8_printf(out,"#("); col=col+2;
      while (eltno<len) {
        col=fd_pprint(out,FD_VECTOR_REF(x,eltno),prefix,
                      indent+2,col,maxcol,(eltno==0));
        eltno++;}
      u8_putc(out,')'); return col+1;}}
  else if (FD_QCHOICEP(x)) {
    struct FD_QCHOICE *qc=FD_XQCHOICE(x);
    int first_value=1;
    if (FD_EMPTY_CHOICEP(qc->fd_choiceval)) {
      u8_puts(out,"#{}");
      return col+3;}
    else {
      FD_DO_CHOICES(elt,qc->fd_choiceval)
        if (first_value) {
          u8_puts(out,"#{"); col++; first_value=0;
          col=fd_pprint(out,elt,prefix,indent+2,col,maxcol,1);}
        else col=fd_pprint(out,elt,prefix,indent+2,col,maxcol,0);
      u8_putc(out,'}');
      return col+1;}}
  else if (FD_CHOICEP(x)) {
    int first_value=1;
    FD_DO_CHOICES(elt,x)
      if (first_value) {
        u8_putc(out,'{'); col++; first_value=0;
        col=fd_pprint(out,elt,prefix,indent+1,col,maxcol,1);}
      else col=fd_pprint(out,elt,prefix,indent+1,col,maxcol,0);
    u8_putc(out,'}'); return col+1;}
  else if (FD_SLOTMAPP(x)) {
    struct FD_SLOTMAP *sm=FD_XSLOTMAP(x);
    struct FD_KEYVAL *scan, *limit;
    int slotmap_size, first_kv=1;
    slotmap_size=FD_XSLOTMAP_SIZE(sm);
    if (slotmap_size==0) {
      if (is_initial) {
        u8_printf(out,"#[]"); return col+3;}
      else {u8_printf(out," #[]"); return col+4;}}
    fd_read_lock_struct(sm);
    scan=sm->keyvals; limit=sm->keyvals+slotmap_size;
    u8_puts(out,"#["); col=col+2;
    while (scan<limit) {
      fdtype key=scan->key, val=scan->value;
      int newcol=output_keyval(out,key,val,col,maxcol,first_kv);
      if (newcol>=0) {
        col=newcol; scan++; first_kv=0;
        continue;}
      else {
        int i=indent; u8_putc(out,'\n');
        if (prefix) u8_puts(out,prefix);
        while (i>0) {u8_putc(out,' '); i--;}
        col=indent+((prefix) ? (u8_strlen(prefix)) : (0));}
      col=fd_pprint(out,scan->key,prefix,indent+2,col,maxcol,first_kv);
      u8_putc(out,' '); col++;
      col=fd_pprint(out,scan->value,prefix,indent+4,col,maxcol,0);
      first_kv=0;
      scan++;}
    u8_puts(out,"]");
    fd_rw_unlock_struct(sm);
    return col+1;}
  else {
    int startoff=out->u8_outptr-out->u8_outbuf;
    fd_unparse(out,x); n_chars=u8_strlen(out->u8_outbuf+startoff);
    return indent+n_chars;}
}

FD_EXPORT
int fd_xpprint(u8_output out,fdtype x,u8_string prefix,
               int indent,int col,int maxcol,int is_initial,
               fd_pprintfn fn,void *data)
{
  int startoff=out->u8_outptr-out->u8_outbuf, n_chars;
  if (fn) {
    int newcol=fn(out,x,prefix,indent,col,maxcol,is_initial,data);
    if (newcol>=0) return newcol;}
  if (is_initial==0) u8_putc(out,' ');
  fd_unparse(out,x); n_chars=u8_strlen(out->u8_outbuf+startoff);
  /* If we're not going to descend, and it all fits, just return the
     new column position. */
  if ((PPRINT_ATOMICP(x)) && ((is_initial) || (col+n_chars<maxcol)))
    return col+n_chars;
  /* Otherwise, reset the stream pointer. */
  out->u8_outptr=out->u8_outbuf+startoff; out->u8_outbuf[startoff]='\0';
  /* Newline and indent if you're non-initial and ran out of space. */
  if ((is_initial==0) && (col+n_chars>=maxcol)) {
    int i=indent; u8_putc(out,'\n');
    if (prefix) u8_puts(out,prefix);
    while (i>0) {u8_putc(out,' '); i--;}
    col=indent+((prefix) ? (u8_strlen(prefix)) : (0));
    startoff=out->u8_outptr-out->u8_outbuf;}
  else if (is_initial==0) u8_putc(out,' ');
  /* Handle quote, quasiquote and friends */
  if ((FD_PAIRP(x)) && (FD_SYMBOLP(FD_CAR(x))) &&
      (FD_PAIRP(FD_CDR(x))) && (FD_EMPTY_LISTP(FD_CDR(FD_CDR(x))))) {
    fdtype car=FD_CAR(x);
    if (FD_EQ(car,quote_symbol)) {
      u8_putc(out,'\''); indent++; x=FD_CAR(FD_CDR(x));}
    else if (FD_EQ(car,unquote_symbol)) {
      indent++; u8_putc(out,','); x=FD_CAR(FD_CDR(x));}
    else if (FD_EQ(car,quasiquote_symbol)) {
      indent++; u8_putc(out,'`'); x=FD_CAR(FD_CDR(x));}
    else if (FD_EQ(car,unquote_star_symbol)) {
      indent++; indent++; u8_puts(out,",@"); x=FD_CAR(FD_CDR(x));}}
  /* Special compound printers for different types. */
  if (FD_PAIRP(x)) {
    fdtype car=FD_CAR(x), scan=x; int first=1;
    if (FD_SYMBOLP(car)) indent=indent+2;
    u8_putc(out,'('); col++; indent++;
    while (FD_PAIRP(scan)) {
      col=fd_xpprint(out,FD_CAR(scan),prefix,indent,col,maxcol,first,fn,data);
      first=0; scan=FD_CDR(scan);}
    if (FD_EMPTY_LISTP(scan)) {
      u8_putc(out,')');  return col+1;}
    else {
      startoff=out->u8_outptr-out->u8_outbuf; u8_printf(out," . %q)",scan);
      n_chars=u8_strlen(out->u8_outbuf+startoff);
      if (col+n_chars>maxcol) {
        int i=indent;
        out->u8_outptr=out->u8_outbuf+startoff; out->u8_outbuf[startoff]='\0';
        u8_putc(out,'\n');
        if (prefix) u8_puts(out,prefix);
        while (i>0) {u8_putc(out,' '); i--;}
        u8_printf(out,". ");
        col=indent+2+((prefix) ? (u8_strlen(prefix)) : (0));
        col=fd_xpprint(out,scan,prefix,indent+2,col,maxcol,1,fn,data);
        u8_putc(out,')'); return col+1;}
      else return col+n_chars;}}
  else if (FD_VECTORP(x)) {
    int len=FD_VECTOR_LENGTH(x);
    if (len==0) {u8_printf(out,"#()"); return col+3;}
    else {
      int eltno=0;
      u8_printf(out,"#("); col=col+2;
      while (eltno<len) {
        col=fd_xpprint(out,FD_VECTOR_REF(x,eltno),prefix,indent+2,col,maxcol,(eltno==0),fn,data);
        eltno++;}
      u8_putc(out,')'); return col+1;}}
  else if (FD_QCHOICEP(x)) {
    struct FD_QCHOICE *qc=FD_XQCHOICE(x);
    int first_value=1;
    FD_DO_CHOICES(elt,qc->fd_choiceval)
      if (first_value) {
        u8_puts(out,"#{"); col++; first_value=0;
        col=fd_xpprint(out,elt,prefix,indent+2,col,maxcol,1,fn,data);}
      else col=fd_xpprint(out,elt,prefix,indent+2,col,maxcol,0,fn,data);
    u8_putc(out,'}'); return col+1;}
  else if (FD_CHOICEP(x)) {
    int first_value=1;
    FD_DO_CHOICES(elt,x)
      if (first_value) {
        u8_putc(out,'{'); col++; first_value=0;
        col=fd_xpprint(out,elt,prefix,indent+1,col,maxcol,1,fn,data);}
      else col=fd_xpprint(out,elt,prefix,indent+1,col,maxcol,0,fn,data);
    u8_putc(out,'}'); return col+1;}
  else if (FD_SLOTMAPP(x)) {
    struct FD_SLOTMAP *sm=FD_XSLOTMAP(x);
    struct FD_KEYVAL *scan, *limit;
    int slotmap_size, first_pair=1;
    fd_read_lock_struct(sm);
    slotmap_size=FD_XSLOTMAP_SIZE(sm);
    if (slotmap_size==0) fd_rw_unlock_struct(sm);
    if (slotmap_size==0) {
      if (is_initial) {
        u8_printf(out," #[]"); return 3;}
      else {u8_printf(out," #[]"); return 4;}}
    scan=sm->keyvals; limit=sm->keyvals+slotmap_size;
    u8_puts(out,"#["); col=col+2;
    while (scan<limit) {
      col=fd_xpprint(out,scan->key,prefix,
                     indent+2,col,maxcol,first_pair,fn,data);
      col=fd_xpprint(out,scan->value,
                     prefix,indent+4,col,maxcol,0,fn,data);
      first_pair=0;
      scan++;}
    u8_puts(out,"]");
    fd_rw_unlock_struct(sm);
    return col+1;}
  else {
    int startoff=out->u8_outptr-out->u8_outbuf;
    fd_unparse(out,x); n_chars=u8_strlen(out->u8_outbuf+startoff);
    return indent+n_chars;}
}

static int output_keyval(u8_output out,fdtype key,fdtype val,
                         int col,int maxcol,int first_kv)
{
  int len=0;
  if (FD_STRINGP(key)) len=len+FD_STRLEN(key)+3;
  else if (FD_SYMBOLP(key)) {
    u8_string pname=FD_SYMBOL_NAME(key);
    len=len+strlen(pname)+1;}
  else if (FD_CONSP(key)) return -1;
  else {}
  if (FD_STRINGP(val)) len=len+FD_STRLEN(val)+3;
  else if (FD_SYMBOLP(val)) {
    u8_string pname=FD_SYMBOL_NAME(val);
    len=len+strlen(pname)+1;}
  else {}
  if ((col+len)>maxcol) return -1;
  else {
    struct U8_OUTPUT kvout; u8_byte kvbuf[128];
    U8_INIT_FIXED_OUTPUT(&kvout,128,kvbuf);
    if (first_kv)
      u8_printf(&kvout,"%q %q",key,val);
    else u8_printf(&kvout," %q %q",key,val);
    if ((kvout.u8_streaminfo&U8_STREAM_OVERFLOW)||
        ((col+(kvout.u8_outptr-kvout.u8_outbuf))>maxcol))
      return -1;
    else u8_putn(out,kvout.u8_outbuf,kvout.u8_outptr-kvout.u8_outbuf);
    len=len+kvout.u8_outptr-kvout.u8_outbuf;}
  return len;
}

/* Focused pprinting */

struct FOCUS_STRUCT {fdtype focus; u8_string prefix, suffix;};

static int focus_pprint(u8_output out,fdtype x,u8_string prefix,
                        int indent,int col,int maxcol,int is_initial,void *data)
{
  struct FOCUS_STRUCT *fs=(struct FOCUS_STRUCT *) data;
  if (FD_EQ(x,fs->focus)) {
    int startoff=out->u8_outptr-out->u8_outbuf, n_chars;
    if (is_initial==0) u8_putc(out,' ');
    fd_unparse(out,x); n_chars=u8_strlen(out->u8_outbuf+startoff);
    out->u8_outptr=out->u8_outbuf+startoff; out->u8_outbuf[startoff]='\0';
    if (col+n_chars>=maxcol) {
      int i=indent; u8_putc(out,'\n');
      if (prefix) u8_puts(out,prefix);
      while (i>0) {u8_putc(out,' '); i--;}
      col=indent+((prefix) ? (u8_strlen(prefix)) : (0));}
    else if (is_initial==0) {u8_putc(out,' '); col++;}
    if (fs->prefix) u8_puts(out,fs->prefix);
    col=fd_pprint(out,x,prefix,indent,col,maxcol,1);
    if (fs->suffix) u8_puts(out,fs->suffix);
    return col;}
  else return -1;
}

FD_EXPORT
void fd_pprint_focus(U8_OUTPUT *out,fdtype entry,fdtype focus,u8_string prefix,
                     int indent,int width,u8_string focus_prefix,u8_string focus_suffix)
{
  struct FOCUS_STRUCT fs; fs.focus=focus; fs.prefix=focus_prefix; fs.suffix=focus_suffix;
  fd_xpprint(out,entry,prefix,indent,indent,width,1,focus_pprint,(void *)&fs);
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
    int i=0, len=FD_VECTOR_LENGTH(expr);
    while (i<len)
      if (embeddedp(focus,FD_VECTOR_REF(expr,i))) return 1; else i++;
    return 0;}
  else if (FD_CHOICEP(expr)) {
    FD_DO_CHOICES(elt,expr)
      if (embeddedp(focus,elt)) return 1;
    return 0;}
  else if (FD_QCHOICEP(expr)) {
    struct FD_QCHOICE *qc=FD_XQCHOICE(expr);
    FD_DO_CHOICES(elt,qc->fd_choiceval)
      if (embeddedp(focus,elt)) return 1;
    return 0;}
  else if (FD_SLOTMAPP(expr)) {
    struct FD_SLOTMAP *sm=FD_XSLOTMAP(expr);
    struct FD_KEYVAL *scan, *limit;
    int slotmap_size;
    fd_read_lock_struct(sm);
    slotmap_size=FD_XSLOTMAP_SIZE(sm);
    scan=sm->keyvals; limit=sm->keyvals+slotmap_size;
    while (scan<limit)
      if (embeddedp(focus,scan->key)) {
        fd_rw_unlock_struct(sm); return 1;}
      else if (embeddedp(focus,scan->value)) {
        fd_rw_unlock_struct(sm); return 1;}
      else scan++;
    fd_rw_unlock_struct(sm);
    return 0;}
  else return 0;
}

static fdtype exception_data(u8_exception ex)
{
  if ((ex->u8x_xdata) && (ex->u8x_free_xdata==fd_free_exception_xdata))
    return (fdtype)(ex->u8x_xdata);
  else return FD_VOID;
}

static u8_exception get_innermost_expr(u8_exception ex,fdtype expr)
{
  u8_exception scan=ex; fdtype xdata;
  if (ex==NULL) return ex;
  else xdata=exception_data(scan);
  if ((FD_PAIRP(xdata)) && (embeddedp(xdata,expr))) {
    u8_exception bottom=get_innermost_expr(ex->u8x_prev,xdata);
    if (bottom) return bottom; else return ex;}
  else return NULL;
}

static void print_backtrace_env(U8_OUTPUT *out,u8_exception ex,int width)
{
  fdtype entry=exception_data(ex);
  fdtype keys=fd_getkeys(entry);
  u8_string head=((ex->u8x_details) ? ((u8_string)(ex->u8x_details)) :
                  (ex->u8x_context) ?  ((u8_string)(ex->u8x_context)) :
                  ((u8_string)""));
  if (FD_ABORTP(keys)) {
    u8_printf(out,"%s %q\n",head,entry);}
  else {
    FD_DO_CHOICES(key,keys) {
      fdtype val=fd_get(entry,key,FD_VOID);
      u8_printf(out,";;=%s> %q = %q\n",head,key,val);
      fd_decref(val);}
    fd_decref(keys);}
}

static u8_exception print_backtrace_entry(U8_OUTPUT *out,u8_exception ex,int width)
{
  if (ex->u8x_context==fd_eval_context) {
    fdtype expr=exception_data(ex);
    u8_exception innermost=get_innermost_expr(ex->u8x_prev,expr);
    fdtype focus=((innermost) ? (exception_data(innermost)) : (FD_VOID));
    u8_printf(out,";;!>> ");
    fd_pprint_focus(out,expr,focus,";;!>> ",0,width,"!",NULL);
    u8_printf(out,"\n");
    if (innermost)return innermost->u8x_prev;
    else return ex->u8x_prev;}
  else if (ex->u8x_context==fd_apply_context) {
    fdtype entry=exception_data(ex);
    int i=1, lim=FD_VECTOR_LENGTH(entry);
    u8_printf(out,";;*CALL %q",FD_VECTOR_REF(entry,0));
    while (i < lim) {
      fdtype arg=FD_VECTOR_REF(entry,i); i++;
      if ((FD_SYMBOLP(arg)) || (FD_PAIRP(arg)))
        u8_printf(out," '%q",arg);
      else u8_printf(out," %q",arg);}
    u8_printf(out,"\n");}
  else if ((ex->u8x_context) && (ex->u8x_context[0]==':')) {
    print_backtrace_env(out,ex,width);}
  else fd_print_exception(out,ex);
  return ex->u8x_prev;
}

FD_EXPORT
void fd_print_backtrace(U8_OUTPUT *out,u8_exception ex,int width)
{
  u8_exception scan=ex;
  while (scan) {
    u8_printf(out,";;=======================================================\n");
    scan=print_backtrace_entry(out,scan,width);}
}

FD_EXPORT
void fd_summarize_backtrace(U8_OUTPUT *out,u8_exception ex)
{
  u8_exception scan=ex; u8_condition cond=NULL;
  while (scan) {
    fdtype irritant=fd_exception_xdata(scan); int show_irritant=1;
    if (scan!=ex) u8_printf(out," <");
    if (scan->u8x_cond!=cond) {
      cond=scan->u8x_cond; u8_printf(out," (%m)",cond);}
    if (scan->u8x_context) {
      if (scan->u8x_context==fd_eval_context)
        if ((FD_PAIRP(irritant)) && (!(FD_PAIRP(FD_CAR(irritant))))) {
          u8_printf(out," (%q ...)",FD_CAR(irritant));
          show_irritant=0;}
        else {}
      else if (scan->u8x_context==fd_apply_context)
        if ((FD_VECTORP(irritant)) && (FD_VECTOR_LENGTH(irritant)>0)) {
          u8_printf(out," %q",FD_VECTOR_REF(irritant,0));
          show_irritant=0;}
        else {}
      else if ((scan->u8x_context) && (strcmp(scan->u8x_context,":SPROC")==0)) {
        show_irritant=0;}
      else if ((scan->u8x_context) && (*(scan->u8x_context)==':')) {
        u8_printf(out," %s",scan->u8x_context);
        show_irritant=0;}
      else u8_printf(out," %s",scan->u8x_context);}
    if (scan->u8x_details)
      u8_printf(out," [%s]",scan->u8x_details);
    if (show_irritant)
      u8_printf(out," <%q>",irritant);
    scan=scan->u8x_prev;}
}


/* Table showing primitives */

static fdtype lisp_show_table(fdtype tables,fdtype slotids,fdtype portarg)
{
  U8_OUTPUT *out=get_output_port(portarg);
  FD_DO_CHOICES(table,tables)
    if ((FD_FALSEP(slotids)) || (FD_VOIDP(slotids)))
      fd_display_table(out,table,FD_VOID);
    else if (FD_OIDP(table)) {
      U8_OUTPUT *tmp=u8_open_output_string(1024);
      u8_printf(out,"%q\n");
      {FD_DO_CHOICES(slotid,slotids) {
        fdtype values=fd_frame_get(table,slotid);
        tmp->u8_outptr=tmp->u8_outbuf; *(tmp->u8_outbuf)='\0';
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
  U8_OUTPUT *out=get_output_port(portarg);
  int width=((FD_FIXNUMP(widtharg)) ? (FD_FIX2INT(widtharg)) : (60));
  if ((out==NULL)&&(!(FD_FALSEP(portarg))))
    return fd_type_error(_("port"),"lisp_pprint",portarg);
  U8_INIT_OUTPUT(&tmpout,512);
  if (FD_VOIDP(marginarg))
    fd_pprint(&tmpout,x,NULL,0,0,width,1);
  else if (FD_STRINGP(marginarg))
    fd_pprint(&tmpout,x,FD_STRDATA(marginarg),0,0,width,1);
  else if ((FD_FIXNUMP(marginarg))&&(FD_FIX2INT(marginarg)>=0))
    fd_pprint(&tmpout,x,NULL,(FD_FIX2INT(marginarg)),0,width,1);
  else fd_pprint(&tmpout,x,NULL,0,0,width,1);
  if (out) {
    u8_puts(out,tmpout.u8_outbuf); u8_free(tmpout.u8_outbuf);
    u8_flush(out);
    return FD_VOID;}
  else return fd_init_string(NULL,tmpout.u8_outptr-tmpout.u8_outbuf,tmpout.u8_outbuf);
}

static u8_string lisp_pprintf_handler
  (u8_output out,char *cmd,u8_byte *buf,int bufsiz,va_list *args)
{
  struct U8_OUTPUT tmpout;
  int width=80; fdtype value;
  if (strchr(cmd,'*'))
    width=va_arg(*args,int);
  else width=strtol(cmd,NULL,10);
  value=va_arg(*args,fdtype);
  U8_INIT_OUTPUT(&tmpout,512);
  fd_pprint(&tmpout,value,NULL,0,0,width,1);
  u8_puts(out,tmpout.u8_outbuf);
  u8_free(tmpout.u8_outbuf);
  if (strchr(cmd,'-')) fd_decref(value);
  return NULL;
}

/* Base 64 stuff */

static fdtype from_base64_prim(fdtype string)
{
  const u8_byte *string_data=FD_STRDATA(string);
  unsigned int string_len=FD_STRLEN(string), data_len;
  unsigned char *data=
    u8_read_base64(string_data,string_data+string_len,&data_len);
  if (data)
    return fd_init_packet(NULL,data_len,data);
  else return FD_ERROR_VALUE;
}

static fdtype to_base64_prim(fdtype packet,fdtype nopad,fdtype urisafe)
{
  const u8_byte *packet_data=FD_PACKET_DATA(packet);
  unsigned int packet_len=FD_PACKET_LENGTH(packet), ascii_len;
  char *ascii_string=u8_write_base64(packet_data,packet_len,&ascii_len);
  if (ascii_string) {
    if (FD_TRUEP(nopad)) {
      char *scan=ascii_string+(ascii_len-1);
      while (*scan=='=') {*scan='\0'; scan--; ascii_len--;}}
    if (FD_TRUEP(urisafe)) {
      char *scan=ascii_string, *limit=ascii_string+ascii_len;
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
    data=FD_PACKET_DATA(arg);
    data_len=FD_PACKET_LENGTH(arg);}
  else if ((FD_STRINGP(arg))||(FD_PRIM_TYPEP(arg,fd_secret_type))) {
    data=FD_STRDATA(arg);
    data_len=FD_STRLEN(arg);}
  else return fd_type_error("packet or string","any_to_base64_prim",arg);
  ascii_string=u8_write_base64(data,data_len,&ascii_len);
  if (ascii_string) {
    if (FD_TRUEP(nopad)) {
      char *scan=ascii_string+(ascii_len-1);
      while (*scan=='=') {*scan='\0'; scan--; ascii_len--;}}
    if (FD_TRUEP(urisafe)) {
      char *scan=ascii_string, *limit=ascii_string+ascii_len;
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
  const u8_byte *string_data=FD_STRDATA(string);
  unsigned int string_len=FD_STRLEN(string), data_len;
  unsigned char *data=u8_read_base16(string_data,string_len,&data_len);
  if (data)
    return fd_init_packet(NULL,data_len,data);
  else return FD_ERROR_VALUE;
}

static fdtype to_base16_prim(fdtype packet)
{
  const u8_byte *packet_data=FD_PACKET_DATA(packet);
  unsigned int packet_len=FD_PACKET_LENGTH(packet);
  char *ascii_string=u8_write_base16(packet_data,packet_len);
  if (ascii_string)
    return fd_init_string(NULL,packet_len*2,ascii_string);
  else return FD_ERROR_VALUE;
}

/* Making zipfiles */

static int string_isasciip(const unsigned char *data,int len)
{
  const unsigned char *scan=data, *limit=scan+len;
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
    fd_exception error=NULL;
    const unsigned char *data=
      ((FD_STRINGP(arg))?(FD_STRDATA(arg)):(FD_PACKET_DATA(arg)));
    unsigned int data_len=
      ((FD_STRINGP(arg))?(FD_STRLEN(arg)):(FD_PACKET_LENGTH(arg)));
    struct FD_BYTE_OUTPUT out; int flags=0; /* FDPP_FHCRC */
    time_t now=time(NULL); u8_int4 crc, intval;
    FD_INIT_BYTE_OUTPUT(&out,1024); memset(out.start,0,1024);
    fd_write_byte(&out,31); fd_write_byte(&out,139);
    fd_write_byte(&out,8); /* Using default */
    /* Compute flags */
    if ((FD_STRINGP(arg))&&(string_isasciip(FD_STRDATA(arg),FD_STRLEN(arg))))
      flags=flags|FDPP_FASCII;
    if (FD_STRINGP(filename)) flags=flags|FDPP_FNAME;
    if (FD_STRINGP(comment)) flags=flags|FDPP_FCOMMENT;
    fd_write_byte(&out,flags);
    intval=fd_flip_word((unsigned int)now);
    fd_write_4bytes(&out,intval);
    fd_write_byte(&out,2); /* Max compression */
    fd_write_byte(&out,3); /* Assume Unix */
    /* No extra fields */
    if (FD_STRINGP(filename)) {
      u8_string text=FD_STRDATA(filename), end=text+FD_STRLEN(filename); int len;
      unsigned char *string=u8_localize(latin1_encoding,&text,end,'\\',0,NULL,&len);
      fd_write_bytes(&out,string,len); fd_write_byte(&out,'\0');
      u8_free(string);}
    if (FD_STRINGP(comment)) {
      u8_string text=FD_STRDATA(comment), end=text+FD_STRLEN(comment); int len;
      unsigned char *string=u8_localize(latin1_encoding,&text,end,'\\',0,NULL,&len);
      fd_write_bytes(&out,string,len); fd_write_byte(&out,'\0');
      u8_free(string);}
    /*
    crc=u8_crc32(0,(void *)out.start,out.ptr-out.start);
    fd_write_byte(&out,((crc)&(0xFF)));
    fd_write_byte(&out,((crc>>8)&(0xFF)));
    */
    {
      int zerror;
      unsigned long dsize=data_len, csize, csize_max;
      Bytef *dbuf=(Bytef *)data, *cbuf;
      csize=csize_max=dsize+(dsize/1000)+13;
      cbuf=u8_malloc(csize_max); memset(cbuf,0,csize);
      while ((zerror=compress2(cbuf,&csize,dbuf,dsize,9)) < Z_OK)
        if (zerror == Z_MEM_ERROR) {
          error=_("ZLIB ran out of memory"); break;}
        else if (zerror == Z_BUF_ERROR) {
          /* We don't use realloc because there's not point in copying
             the data and we hope the overhead of free/malloc beats
             realloc when we're doubling the buffer size. */
          u8_free(cbuf);
          cbuf=u8_malloc(csize_max*2);
          if (cbuf==NULL) {
            error=_("OIDPOOL compress ran out of memory"); break;}
          csize=csize_max=csize_max*2;}
        else if (zerror == Z_DATA_ERROR) {
          error=_("ZLIB compress data error"); break;}
        else {
          error=_("Bad ZLIB return code"); break;}
      if (error==NULL) {
        fd_write_bytes(&out,cbuf+2,csize-6);}
      u8_free(cbuf);}
    if (error) {
      fd_seterr(error,"x2zipfile",NULL,FD_VOID);
      u8_free(out.start);
      return FD_ERROR_VALUE;}
    crc=u8_crc32(0,data,data_len);
    intval=fd_flip_word(crc); fd_write_4bytes(&out,intval);
    intval=fd_flip_word(data_len); fd_write_4bytes(&out,intval);
    return fd_init_packet(NULL,out.ptr-out.start,out.start);}
}

/* The init function */

FD_EXPORT void fd_init_portfns_c()
{
  u8_register_source_file(_FILEINFO);

  u8_printf_handlers['Q']=lisp_pprintf_handler;

  fd_port_type=fd_register_cons_type("IOPORT");

  fd_unparsers[fd_port_type]=unparse_port;
  fd_recyclers[fd_port_type]=recycle_port;

  fd_dtstream_type=fd_register_cons_type("DTSTREAM");

  fd_unparsers[fd_dtstream_type]=unparse_dtstream;
  fd_recyclers[fd_dtstream_type]=recycle_dtstream;

  quote_symbol=fd_intern("QUOTE");
  unquote_symbol=fd_intern("UNQUOTE");
  quasiquote_symbol=fd_intern("QUASIQUOTE");
  unquote_star_symbol=fd_intern("UNQUOTE*");
  comment_symbol=fd_intern("COMMENT");

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
           fd_make_cprim0("OPEN-OUTPUT-STRING",open_output_string,0));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1("OPEN-INPUT-STRING",open_input_string,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("PORTDATA",portdata,1));

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

  fd_defspecial(fd_scheme_module,"PRINTOUT",printout_handler);
  fd_defspecial(fd_scheme_module,"LINEOUT",lineout_handler);
  fd_defspecial(fd_scheme_module,"STRINGOUT",stringout_handler);
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
  fd_defspecial(fd_scheme_module,"NOTIFY",notify_handler);
  fd_defspecial(fd_scheme_module,"STATUS",status_handler);
  fd_defspecial(fd_scheme_module,"WARNING",warning_handler);

  /* Generic logging function, always outputs */
  fd_defspecial(fd_scheme_module,"MESSAGE",message_handler);
  fd_defspecial(fd_scheme_module,"%LOGGER",message_handler);

  /* Logging with message level */
  fd_defspecial(fd_scheme_module,"LOGMSG",log_handler);
  /* Conditional logging */
  fd_defspecial(fd_scheme_module,"LOGIF",logif_handler);
  /* Conditional logging with priority level */
  fd_defspecial(fd_scheme_module,"LOGIF+",logifplus_handler);

  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("READ-DTYPE",read_dtype,1,fd_dtstream_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("WRITE-DTYPE",write_dtype,2,
                           -1,FD_VOID,fd_dtstream_type,FD_VOID));

  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("WRITE-BYTES",write_bytes,2,
                           -1,FD_VOID,fd_dtstream_type,FD_VOID));


  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("PACKET->DTYPE",packet2dtype,1,
                           fd_packet_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("DTYPE->PACKET",dtype2packet,1,
                           -1,FD_VOID,fd_fixnum_type,FD_INT(128)));

  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("READ-INT",read_int,1,fd_dtstream_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("WRITE-INT",write_int,2,
                           -1,FD_VOID,fd_dtstream_type,FD_VOID));

  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("ZREAD-DTYPE",
                           zread_dtype,1,fd_dtstream_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("ZWRITE-DTYPE",zwrite_dtype,2,
                           -1,FD_VOID,fd_dtstream_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("ZWRITE-DTYPES",zwrite_dtypes,2,
                           -1,FD_VOID,fd_dtstream_type,FD_VOID));

  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("ZREAD-INT",
                           zread_int,1,fd_dtstream_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("ZWRITE-INT",zwrite_int,2,
                           -1,FD_VOID,fd_dtstream_type,FD_VOID));

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
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
