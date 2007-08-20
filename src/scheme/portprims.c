/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define FD_PROVIDE_FASTEVAL 1

#include "fdb/dtype.h"
#include "fdb/eval.h"
#include "fdb/fddb.h"
#include "fdb/pools.h"
#include "fdb/indices.h"
#include "fdb/frames.h"
#include "fdb/dtypestream.h"
#include "fdb/ports.h"

#include <libu8/u8streamio.h>

fd_exception fd_UnknownEncoding=_("Unknown encoding");

/* The port type */

fd_ptr_type fd_port_type;

static int unparse_port(struct U8_OUTPUT *out,fdtype x)
{
  u8_printf(out,"#<Port #!%x>",x);
  return 1;
}

static void recycle_port(struct FD_CONS *c)
{
  struct FD_PORT *p=(struct FD_PORT *)c;
  if (p->in) {
    u8_close_input(p->in);}
  if (p->out) {
    u8_close_output(p->out);}
  if (FD_MALLOCD_CONSP(c)) u8_free(c);
}

/* Getting default output */

u8_output fd_default_output=NULL;

FD_EXPORT void fd_set_global_output(u8_output out)
{
  fd_default_output=out;
}

#if FD_THREADS_ENABLED
static u8_tld_key default_output_key;
FD_EXPORT U8_OUTPUT *fd_get_default_output()
{
  U8_OUTPUT *f=(U8_OUTPUT *)u8_tld_get(default_output_key);
  if (f) return f;
  else return fd_default_output;
}
FD_EXPORT void fd_set_default_output(U8_OUTPUT *f)
{
  if (f==fd_default_output)
    u8_tld_set(default_output_key,NULL);
  else u8_tld_set(default_output_key,f);
}
#else
static U8_OUTPUT *default_output=NULL;
FD_EXPORT U8_OUTPUT *fd_get_default_output()
{
  if (default_output)
    return default_output;
  else return fd_default_output;
}
FD_EXPORT void fd_set_default_output(U8_OUTPUT *f)
{
  if (f==fd_default_output)
    default_output=NULL;
  else default_output=f;
}
#endif

static int use_u8_message(u8_output f)
{
  u8_message(f->u8_outbuf);
  if (fd_get_default_output()==f)
    fd_set_default_output(NULL);
  if (f->u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(f->u8_outbuf);
  if (f->u8_streaminfo&U8_STREAM_MALLOCD) u8_free(f);
}

FD_INLINE_FCN u8_output get_default_output_port()
{
  u8_output f=fd_get_default_output();
  if (f) return f;
  f=u8_open_output_string(512);
  f->u8_closefn=use_u8_message;
  fd_set_default_output(f);
}

/* Making ports */

static fdtype make_port(U8_INPUT *in,U8_OUTPUT *out)
{
  struct FD_PORT *port=u8_alloc(struct FD_PORT);
  FD_INIT_CONS(port,fd_port_type);
  port->in=in; port->out=out;
  return FDTYPE_CONS(port);
}

static u8_output get_output_port(fdtype portarg)
{
  if (FD_VOIDP(portarg))
    return fd_get_default_output();
  else if (FD_PTR_TYPEP(portarg,fd_port_type)) {
    struct FD_PORT *p=
      FD_GET_CONS(portarg,fd_port_type,struct FD_PORT *);
    return p->out;}
  else return NULL;
}

static u8_input get_input_port(fdtype portarg)
{
  if (FD_VOIDP(portarg))
    return NULL; /* get_default_output(); */
  else if (FD_PTR_TYPEP(portarg,fd_port_type)) {
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
  fd_dtsclose(p->dt_stream,p->owns_socket);
  if (FD_MALLOCD_CONSP(c)) u8_free(c);
}

static fdtype read_dtype(fdtype stream)
{
  struct FD_DTSTREAM *ds=FD_GET_CONS(stream,fd_dtstream_type,struct FD_DTSTREAM *);
  fdtype object=fd_dtsread_dtype(ds->dt_stream);
  if (object == FD_EOD) return FD_EOF;
  else return object;
}

static fdtype write_dtype(fdtype object,fdtype stream)
{
  struct FD_DTSTREAM *ds=FD_GET_CONS(stream,fd_dtstream_type,struct FD_DTSTREAM *);
  int bytes=fd_dtswrite_dtype(ds->dt_stream,object);
  if (bytes<0) return fd_erreify();
  else return FD_INT2DTYPE(bytes);
}

static fdtype read_int(fdtype stream)
{
  struct FD_DTSTREAM *ds=FD_GET_CONS(stream,fd_dtstream_type,struct FD_DTSTREAM *);
  unsigned int ival=fd_dtsread_4bytes(ds->dt_stream);
  return FD_INT2DTYPE(ival);
}

static fdtype write_int(fdtype object,fdtype stream)
{
  struct FD_DTSTREAM *ds=FD_GET_CONS(stream,fd_dtstream_type,struct FD_DTSTREAM *);
  int ival=fd_getint(object);
  int bytes=fd_dtswrite_4bytes(ds->dt_stream,ival);
  if (bytes<0) return fd_erreify();
  else return FD_INT2DTYPE(bytes);
}

static fdtype zread_dtype(fdtype stream)
{
  struct FD_DTSTREAM *ds=FD_GET_CONS(stream,fd_dtstream_type,struct FD_DTSTREAM *);
  fdtype object=fd_zread_dtype(ds->dt_stream);
  if (object == FD_EOD) return FD_EOF;
  else return object;
}

static fdtype zwrite_dtype(fdtype object,fdtype stream)
{
  struct FD_DTSTREAM *ds=FD_GET_CONS(stream,fd_dtstream_type,struct FD_DTSTREAM *);
  int bytes=fd_zwrite_dtype(ds->dt_stream,object);
  if (bytes<0) return fd_erreify();
  else return FD_INT2DTYPE(bytes);
}

static fdtype zwrite_dtypes(fdtype object,fdtype stream)
{
  struct FD_DTSTREAM *ds=FD_GET_CONS(stream,fd_dtstream_type,struct FD_DTSTREAM *);
  int bytes=fd_zwrite_dtypes(ds->dt_stream,object);
  if (bytes<0) return fd_erreify();
  else return FD_INT2DTYPE(bytes);
}

static fdtype zread_int(fdtype stream)
{
  struct FD_DTSTREAM *ds=FD_GET_CONS(stream,fd_dtstream_type,struct FD_DTSTREAM *);
  unsigned int ival=fd_dtsread_zint(ds->dt_stream);
  return FD_INT2DTYPE(ival);
}

static fdtype zwrite_int(fdtype object,fdtype stream)
{
  struct FD_DTSTREAM *ds=FD_GET_CONS(stream,fd_dtstream_type,struct FD_DTSTREAM *);
  int ival=fd_getint(object);
  int bytes=fd_dtswrite_zint(ds->dt_stream,ival);
  if (bytes<0) return fd_erreify();
  else return FD_INT2DTYPE(bytes);
}

/* Output strings */

static fdtype open_output_string()
{
  U8_OUTPUT *out=u8_alloc(struct U8_OUTPUT);
  U8_INIT_OUTPUT(out,256);
  return make_port(NULL,out);
}

static fdtype open_input_string(fdtype arg)
{
  if (FD_STRINGP(arg)) {
    U8_INPUT *in=u8_alloc(struct U8_INPUT);
    U8_INIT_STRING_INPUT(in,FD_STRING_LENGTH(arg),u8_strdup(FD_STRDATA(arg)));
    in->u8_streaminfo=in->u8_streaminfo|U8_STREAM_OWNS_BUF;
    return make_port(in,NULL);}
  else return fd_type_error(_("string"),"open_input_string",arg);
}

static fdtype portdata(fdtype port_arg)
{
  if (FD_PTR_TYPEP(port_arg,fd_port_type)) {
    struct FD_PORT *port=(struct FD_PORT *)port_arg;
    if (port->out)
      return fd_extract_string(NULL,port->out->u8_outbuf,port->out->u8_outptr);
    else return fd_extract_string(NULL,port->out->u8_outbuf,port->out->u8_outlim);}
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
  if (out == NULL) out=fd_get_default_output();
  if (FD_STRINGP(x))
    u8_printf(out,"%s",FD_STRDATA(x));
  else u8_printf(out,"%q",x);
  return 1;
}

FD_EXPORT
fdtype fd_printout(fdtype body,fd_lispenv env)
{
  U8_OUTPUT *out=fd_get_default_output();
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
  u8_output prev=fd_get_default_output();
  fd_set_default_output(out);
  while (FD_PAIRP(body)) {
    fdtype value=fasteval(FD_CAR(body),env);
    if (printout_helper(out,value)) fd_decref(value);
    else {
      u8_flush(out);
      fd_set_default_output(prev);
      return value;}
    body=FD_CDR(body);}
  u8_flush(out);
  fd_set_default_output(prev);
  return FD_VOID;
}

static fdtype printout_handler(fdtype expr,fd_lispenv env)
{
  return fd_printout(fd_get_body(expr,1),env);
}
static fdtype lineout_handler(fdtype expr,fd_lispenv env)
{
  U8_OUTPUT *out=fd_get_default_output();
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
  U8_OUTPUT *stream=fd_get_default_output();
  fd_set_default_output(out);
  while (FD_PAIRP(body)) {
    fdtype value=fasteval(FD_CAR(body),env);
    if (printout_helper(out,value)) fd_decref(value);
    else {
      fd_set_default_output(stream);
      u8_close_output(out);
      return value;}
    body=FD_CDR(body);}
  fd_set_default_output(stream);
  u8_message_string(out->u8_outbuf);
  u8_close_output(out);
  return FD_VOID;
}

static fdtype notify_handler(fdtype expr,fd_lispenv env)
{
  fdtype body=fd_get_body(expr,1);
  U8_OUTPUT *out=u8_open_output_string(1024);
  U8_OUTPUT *stream=fd_get_default_output();
  fd_set_default_output(out);
  while (FD_PAIRP(body)) {
    fdtype value=fasteval(FD_CAR(body),env);
    if (printout_helper(out,value)) fd_decref(value);
    else {
      fd_set_default_output(stream);
      u8_close_output(out);
      return value;}
    body=FD_CDR(body);}
  fd_set_default_output(stream);
  u8_notice_string(out->u8_outbuf);
  u8_close_output(out);
  return FD_VOID;
}

static fdtype status_handler(fdtype expr,fd_lispenv env)
{
  fdtype body=fd_get_body(expr,1);
  U8_OUTPUT *out=u8_open_output_string(1024);
  U8_OUTPUT *stream=fd_get_default_output();
  fd_set_default_output(out);
  while (FD_PAIRP(body)) {
    fdtype value=fasteval(FD_CAR(body),env);
    if (printout_helper(out,value)) fd_decref(value);
    else {
      fd_set_default_output(stream);
      u8_close_output(out);
      return value;}
    body=FD_CDR(body);}
  fd_set_default_output(stream);
  u8_status_string(out->u8_outbuf);
  u8_close_output(out);
  return FD_VOID;
}

static fdtype warning_handler(fdtype expr,fd_lispenv env)
{
  fdtype body=fd_get_body(expr,1);
  U8_OUTPUT *out=u8_open_output_string(1024);
  U8_OUTPUT *stream=fd_get_default_output();
  fd_set_default_output(out);
  while (FD_PAIRP(body)) {
    fdtype value=fasteval(FD_CAR(body),env);
    if (printout_helper(out,value)) fd_decref(value);
    else {
      fd_set_default_output(stream);
      u8_close_output(out);
      return value;}
    body=FD_CDR(body);}
  fd_set_default_output(stream);
  u8_warning_string(out->u8_outbuf);
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
    U8_OUTPUT *stream=fd_get_default_output();
    fd_decref(value); fd_set_default_output(out);
    while (FD_PAIRP(body)) {
      fdtype value=fasteval(FD_CAR(body),env);
      if (printout_helper(out,value)) fd_decref(value);
      else {
	fd_set_default_output(stream);
	u8_close_output(out);
	return value;}
      body=FD_CDR(body);}
    fd_set_default_output(stream);
    u8_message_string(out->u8_outbuf);
    u8_close_output(out);
    return FD_VOID;}
}

static fdtype stringout_handler(fdtype expr,fd_lispenv env)
{
  struct U8_OUTPUT out; fdtype result;
  U8_INIT_OUTPUT(&out,256);
  result=fd_printout_to(&out,fd_get_body(expr,1),env);
  if (FD_ABORTP(result)) {
    u8_close_output(&out); return result;}
  else return fd_init_string
	 (NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
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

static fdtype getline_prim(fdtype port,fdtype eos_arg,fdtype lim_arg)
{
  U8_INPUT *in=get_input_port(port);
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
      return fd_init_string(NULL,size,data);
    else if (size<0)
      return fd_erreify();
    else return FD_EMPTY_CHOICE;}
  else return fd_type_error(_("input port"),"getline_prim",port);
}

static fdtype read_prim(fdtype port)
{
  U8_INPUT *in=get_input_port(port);
  if (in) {
    int c=fd_skip_whitespace(in);
    if (c<0) return FD_EOF;
    else return fd_parser(in);}
  else return fd_type_error(_("input port"),"read_prim",port);
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
  int lim, off=-1, matchlen;
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
      if (found) off=found-in->u8_inptr;}
    else off=find_substring(in->u8_inptr,ends,&matchlen);
    if (off>=0) {
      int record_len=off+matchlen;
      u8_byte *buf=u8_malloc(record_len+1);
      int bytes_read=u8_getn(buf,record_len,in);
      return fd_init_string(NULL,record_len,buf);}
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
  return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
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
    return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);}
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
  fdtype num=fd_string2number(FD_STRDATA(x),fd_getint(base));
  if (FD_VOIDP(num)) return FD_FALSE;
  else return num;
}

static fdtype just2number(fdtype x,fdtype base)
{
  if (FD_NUMBERP(x)) return fd_incref(x);
  else if (FD_STRINGP(x)) {
    fdtype num=fd_string2number(FD_STRDATA(x),fd_getint(base));
    if (FD_VOIDP(num)) return FD_FALSE;
    else return num;}
  else return fd_type_error(_("string or number"),"->NUMBER",x);
}

/* Pretty printing */

static fdtype quote_symbol, unquote_symbol, quasiquote_symbol, unquote_star_symbol, comment_symbol;

#define PPRINT_ATOMICP(x) \
  (!((FD_PAIRP(x)) || (FD_VECTORP(x)) || (FD_SLOTMAPP(x)) || \
     (FD_CHOICEP(x)) || (FD_ACHOICEP(x)) || (FD_QCHOICEP(x))))

FD_EXPORT
int fd_pprint(u8_output out,fdtype x,u8_string prefix,
	      int indent,int col,int maxcol,int initial)
{
  int startoff=out->u8_outptr-out->u8_outbuf, n_chars;
  if (initial==0) u8_putc(out,' ');
  fd_unparse(out,x); n_chars=u8_strlen(out->u8_outbuf+startoff);
  /* If we're not going to descend, and it all fits, just return the
     new column position. */
  if ((PPRINT_ATOMICP(x)) && ((initial) || (col+n_chars<maxcol)))
    return col+n_chars;
  /* Otherwise, reset the stream pointer. */
  out->u8_outptr=out->u8_outbuf+startoff; out->u8_outbuf[startoff]='\0';
  /* Newline and indent if you're non-initial and ran out of space. */
  if ((initial==0) && (col+n_chars>=maxcol)) {
    int i=indent; u8_putc(out,'\n');
    if (prefix) u8_puts(out,prefix);
    while (i>0) {u8_putc(out,' '); i--;}
    col=indent+((prefix) ? (u8_strlen(prefix)) : (0));
    startoff=out->u8_outptr-out->u8_outbuf;}
  else if (initial==0) u8_putc(out,' ');
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
    FD_DO_CHOICES(elt,qc->choice) 
      if (first_value) {
	u8_puts(out,"#{"); col++; first_value=0;
	col=fd_pprint(out,elt,prefix,indent+2,col,maxcol,1);}
      else col=fd_pprint(out,elt,prefix,indent+2,col,maxcol,0);
    u8_putc(out,'}'); return col+1;}
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
    int slotmap_size, first_pair=1; 
    slotmap_size=FD_XSLOTMAP_SIZE(sm);
    if (slotmap_size==0) {
      if (initial) {
	u8_printf(out," #[]"); return 3;}
      else {u8_printf(out," #[]"); return 4;}}
    fd_lock_struct(sm);
    scan=sm->keyvals; limit=sm->keyvals+slotmap_size;
    u8_puts(out,"#["); col=col+2;
    while (scan<limit) {
      col=fd_pprint(out,scan->key,prefix,indent+2,col,maxcol,first_pair);
      u8_putc(out,' '); col++;
      col=fd_pprint(out,scan->value,prefix,indent+4,col,maxcol,0);
      first_pair=0;
      scan++;}
    u8_puts(out,"]");
    fd_unlock_struct(sm);
    return col+1;}
  else {
    int startoff=out->u8_outptr-out->u8_outbuf;
    fd_unparse(out,x); n_chars=u8_strlen(out->u8_outbuf+startoff);
    return indent+n_chars;}
}

FD_EXPORT
int fd_xpprint(u8_output out,fdtype x,u8_string prefix,
	       int indent,int col,int maxcol,int initial,
	       fd_pprintfn fn,void *data)
{
  int startoff=out->u8_outptr-out->u8_outbuf, n_chars;
  if (fn) {
    int newcol=fn(out,x,prefix,indent,col,maxcol,initial,data);
    if (newcol>=0) return newcol;}
  if (initial==0) u8_putc(out,' ');
  fd_unparse(out,x); n_chars=u8_strlen(out->u8_outbuf+startoff);
  /* If we're not going to descend, and it all fits, just return the
     new column position. */
  if ((PPRINT_ATOMICP(x)) && ((initial) || (col+n_chars<maxcol)))
    return col+n_chars;
  /* Otherwise, reset the stream pointer. */
  out->u8_outptr=out->u8_outbuf+startoff; out->u8_outbuf[startoff]='\0';
  /* Newline and indent if you're non-initial and ran out of space. */
  if ((initial==0) && (col+n_chars>=maxcol)) {
    int i=indent; u8_putc(out,'\n'); 
    if (prefix) u8_puts(out,prefix);
    while (i>0) {u8_putc(out,' '); i--;}
    col=indent+((prefix) ? (u8_strlen(prefix)) : (0));
    startoff=out->u8_outptr-out->u8_outbuf;}
  else if (initial==0) u8_putc(out,' ');
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
    FD_DO_CHOICES(elt,qc->choice) 
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
    fd_lock_struct(sm);
    slotmap_size=FD_XSLOTMAP_SIZE(sm);
    if (slotmap_size==0) {
      if (initial) {
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
    fd_unlock_struct(sm);
    return col+1;}
  else {
    int startoff=out->u8_outptr-out->u8_outbuf;
    fd_unparse(out,x); n_chars=u8_strlen(out->u8_outbuf+startoff);
    return indent+n_chars;}
}

/* Focused pprinting */

struct FOCUS_STRUCT {fdtype focus; u8_string prefix, suffix;};

static int focus_pprint(u8_output out,fdtype x,u8_string prefix,
			int indent,int col,int maxcol,int initial,void *data)
{
  struct FOCUS_STRUCT *fs=(struct FOCUS_STRUCT *) data;
  if (FD_EQ(x,fs->focus)) {
    int startoff=out->u8_outptr-out->u8_outbuf, n_chars;
    if (initial==0) u8_putc(out,' ');
    fd_unparse(out,x); n_chars=u8_strlen(out->u8_outbuf+startoff);
    out->u8_outptr=out->u8_outbuf+startoff; out->u8_outbuf[startoff]='\0';
    if (col+n_chars>=maxcol) {
      int i=indent; u8_putc(out,'\n');
      if (prefix) u8_puts(out,prefix);
      while (i>0) {u8_putc(out,' '); i--;}
      col=indent+((prefix) ? (u8_strlen(prefix)) : (0));}
    else if (initial==0) {u8_putc(out,' '); col++;}
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

static void output_env(U8_OUTPUT *out,int width,fdtype entry,fdtype head)
{
  fdtype keys=fd_getkeys(entry);
  int startoff=out->u8_outptr-out->u8_outbuf, indent, col;
  if (FD_SYMBOLP(head)) u8_printf(out,";; %q: ",head);
  else u8_printf(out,";; ");
  col=indent=(out->u8_outptr-out->u8_outbuf)-startoff;
  if (FD_ABORTP(keys)) {
    fd_decref(keys);
    u8_printf(out,"%q\n",entry);}
  else {
    FD_DO_CHOICES(key,keys) {
      fdtype val=fd_get(entry,key,FD_VOID);
      int start=out->u8_outptr-out->u8_outbuf, n_chars;
      u8_printf(out,"%q=%q; ",key,val);
      n_chars=u8_strlen(out->u8_outbuf+start);
      if (col+n_chars>=width) {
	out->u8_outptr=out->u8_outbuf+start; out->u8_outbuf[start]='\0';
	if (FD_SYMBOLP(head)) u8_printf(out,"\n;; %q: %q=%q; ",head,key,val);
	else u8_printf(out,"\n;; %q=%q; ",key,val);
	col=u8_strlen(out->u8_outbuf+start+1);}
      else col=col+n_chars;
      fd_decref(val);}
    fd_decref(keys);
    u8_printf(out,"\n");}
}

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
    FD_DO_CHOICES(elt,qc->choice)
      if (embeddedp(focus,elt)) return 1;
    return 0;}
  else if (FD_SLOTMAPP(expr)) {
    struct FD_SLOTMAP *sm=FD_XSLOTMAP(expr);
    struct FD_KEYVAL *scan, *limit;
    int slotmap_size;
    fd_lock_struct(sm);
    slotmap_size=FD_XSLOTMAP_SIZE(sm);
    scan=sm->keyvals; limit=sm->keyvals+slotmap_size;
    while (scan<limit)
      if (embeddedp(focus,scan->key)) {
	fd_unlock_struct(sm); return 1;}
      else if (embeddedp(focus,scan->value)) {
	fd_unlock_struct(sm); return 1;}
      else scan++;
    fd_unlock_struct(sm);
    return 0;}
  else return 0;
}

static void print_backtrace
  (U8_OUTPUT *out,int width,fdtype bt,fdtype head,int n)
{
  fdtype entry, next;
  if (!(FD_PAIRP(bt))) return;
  else {entry=FD_CAR(bt); next=FD_CDR(bt);}
  if (FD_VECTORP(entry)) {
    int i=1, lim=FD_VECTOR_LENGTH(entry);
    u8_printf(out,";; (%q",FD_VECTOR_REF(entry,0));
    while (i < lim) {
      fdtype arg=FD_VECTOR_REF(entry,i); i++;
      if (FD_STRINGP(arg))
	u8_printf(out," \"%s\"",FD_STRDATA(arg));
      else if ((FD_SYMBOLP(arg)) || (FD_PAIRP(arg)))
	u8_printf(out," '%q",arg);
      else u8_printf(out," %q",arg);}
    head=FD_VECTOR_REF(entry,0);
    u8_printf(out,")\n");}
  else if (FD_PAIRP(entry)) {
    fdtype scan=next, focus=entry;
    while ((FD_PAIRP(scan)) &&
	   (FD_PAIRP(FD_CAR(scan))) &&
	   (embeddedp(FD_CAR(scan),entry))) {
      focus=FD_CAR(scan); scan=FD_CDR(scan);}
    u8_printf(out,";;!>> ");
    fd_pprint_focus(out,entry,focus,";;!>> ",0,width,"!",NULL);
    u8_printf(out,"\n");
    next=scan; head=FD_CAR(focus);}
  else if (FD_TABLEP(entry))
    output_env(out,width,entry,head);
  else if (FD_STRINGP(entry))
    u8_printf(out,";; %s\n",FD_STRDATA(entry));
  else {}
  print_backtrace(out,width,next,head,n+1);
}

FD_EXPORT
void fd_print_backtrace(U8_OUTPUT *out,int width,fdtype bt)
{
  print_backtrace(out,width,bt,FD_VOID,0);
}

FD_EXPORT
void fd_print_error(U8_OUTPUT *out,FD_EXCEPTION_OBJECT *e)
{
  u8_printf(out,";; (ERROR %m)",e->data.cond);
  if (e->data.details) u8_printf(out," %m",e->data.details);
  if (e->data.cxt) u8_printf(out," (%s)",e->data.cxt);
  u8_printf(out,"\n");
  if (!(FD_VOIDP(e->data.irritant)))
    u8_printf(out,";; %q\n",e->data.irritant);
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

static fdtype lisp_pprint(fdtype x,fdtype portarg,fdtype widtharg)
{
  struct U8_OUTPUT tmpout;
  U8_OUTPUT *out=get_output_port(portarg);
  int width=((FD_FIXNUMP(widtharg)) ? (FD_FIX2INT(widtharg)) : (60));
  U8_INIT_OUTPUT(&tmpout,512);
  fd_pprint(&tmpout,x,NULL,0,0,width,1);
  u8_puts(out,tmpout.u8_outbuf); u8_free(tmpout.u8_outbuf);
  u8_flush(out);
 return FD_VOID;
}

/* The init function */

FD_EXPORT void fd_init_portfns_c()
{
  fd_register_source_file(versionid);

  fd_port_type=fd_register_cons_type("IOPORT");

  fd_unparsers[fd_port_type]=unparse_port;
  fd_recyclers[fd_port_type]=recycle_port;

  fd_dtstream_type=fd_register_cons_type("DTSTREAM");

  fd_unparsers[fd_dtstream_type]=unparse_dtstream;
  fd_recyclers[fd_dtstream_type]=recycle_dtstream;

#if FD_THREADS_ENABLED
  u8_new_threadkey(&default_output_key,NULL);
#endif

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
			   -1,FD_VOID,fd_fixnum_type,FD_INT2DTYPE(10)));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim2("NUMBER->LOCALE",number2locale,1));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim2x("STRING->NUMBER",string2number,1,
			   fd_string_type,FD_VOID,
			   fd_fixnum_type,FD_INT2DTYPE(10)));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim2x("->NUMBER",just2number,1,
			   -1,FD_VOID,fd_fixnum_type,FD_INT2DTYPE(10)));

  fd_idefn(fd_scheme_module,fd_make_cprim1("EOF-OBJECT?",eofp,1));

  fd_idefn(fd_scheme_module,
	   fd_make_cprim0("OPEN-OUTPUT-STRING",open_output_string,0));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim1("OPEN-INPUT-STRING",open_input_string,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("PORTDATA",portdata,1));

  fd_idefn(fd_scheme_module,fd_make_cprim2("WRITE",write_prim,1));
  fd_idefn(fd_scheme_module,fd_make_cprim2("DISPLAY",display_prim,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("NEWLINE",newline_prim,0));
  fd_idefn(fd_scheme_module,fd_make_cprim3("PPRINT",lisp_pprint,1));

  fd_idefn(fd_scheme_module,fd_make_cprim2("PUTCHAR",putchar_prim,1));
  fd_defalias(fd_scheme_module,"WRITE-CHAR","PUTCHAR");
  fd_idefn(fd_scheme_module,fd_make_cprim1("GETCHAR",getchar_prim,0));
  fd_idefn(fd_scheme_module,fd_make_cprim3("GETLINE",getline_prim,0));
  fd_idefn(fd_scheme_module,fd_make_cprim1("READ",read_prim,0));

  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim3("READ-RECORD",read_record_prim,1)));

  fd_defspecial(fd_scheme_module,"PRINTOUT",printout_handler);
  fd_defspecial(fd_scheme_module,"LINEOUT",lineout_handler);
  fd_defspecial(fd_scheme_module,"STRINGOUT",stringout_handler);
  fd_defspecial(fd_scheme_module,"MESSAGE",message_handler);
  fd_defspecial(fd_scheme_module,"NOTIFY",notify_handler);
  fd_defspecial(fd_scheme_module,"STATUS",status_handler);
  fd_defspecial(fd_scheme_module,"WARNING",warning_handler);
  fd_defspecial(fd_scheme_module,"LOGIF",logif_handler);
  fd_defspecial(fd_scheme_module,"%LOGGER",message_handler);

  fd_idefn(fd_scheme_module,
	   fd_make_cprim1x("READ-DTYPE",read_dtype,1,fd_dtstream_type,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim2x("WRITE-DTYPE",write_dtype,2,
			   -1,FD_VOID,fd_dtstream_type,FD_VOID));

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
	   fd_make_ndprim(fd_make_cprim3("%SHOW",lisp_show_table,1)));

}


/* The CVS log for this file
   $Log: portfns.c,v $
   Revision 1.52  2006/02/11 18:07:22  haase
   Better error catching for port functions and internal renames

   Revision 1.51  2006/02/10 17:20:09  haase
   Fix indent

   Revision 1.50  2006/01/31 13:47:24  haase
   Changed fd_str[n]dup into u8_str[n]dup

   Revision 1.49  2006/01/27 16:40:53  haase
   Made number->string use fd_output_number

   Revision 1.48  2006/01/26 23:34:08  haase
   Defined READ-RECORD primitive

   Revision 1.47  2006/01/26 17:42:24  haase
   Removed duplicate free of U8 port objects after closing

   Revision 1.46  2006/01/26 14:44:32  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.45  2006/01/07 23:46:32  haase
   Moved thread API into libu8

   Revision 1.44  2006/01/07 14:00:12  haase
   Made Scheme ports free their XFILE structures when being freed themselves

   Revision 1.43  2005/12/22 14:36:49  haase
   Close ports even for malloc'd LISP ports

   Revision 1.42  2005/12/20 20:45:53  haase
   Fixed number->string to handle bignums and other numbers

   Revision 1.41  2005/12/17 21:52:35  haase
   Added PUTCHAR/WRITE-CHAR

   Revision 1.40  2005/12/17 05:56:33  haase
   STRING->NUMBER fixes

   Revision 1.39  2005/10/29 19:43:27  haase
   Made portfns export fd_printout and fd_printout_to

   Revision 1.38  2005/08/15 03:28:56  haase
   Added file information to functions and display it in regular and HTML backtraces

   Revision 1.37  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.36  2005/07/13 21:39:31  haase
   XSLOTMAP/XSCHEMAP renaming

   Revision 1.35  2005/06/17 02:52:43  haase
   Added initial xmldata functions

   Revision 1.34  2005/06/07 16:13:41  haase
   Made PPRINT first go to a string and then output that

   Revision 1.33  2005/06/04 01:25:52  haase
   Fixed READ to return the EOF object and made it a non-error

   Revision 1.32  2005/05/29 21:23:53  haase
   Made LINEOUT pass errors out

   Revision 1.31  2005/05/18 22:47:05  haase
   Added string->number

   Revision 1.30  2005/05/18 19:43:50  haase
   remove redundant time info from scheme MESSAGE primitive

   Revision 1.29  2005/05/18 19:25:20  haase
   Fixes to header ordering to make off_t defaults be pervasive

   Revision 1.28  2005/05/10 18:43:35  haase
   Added context argument to fd_type_error

   Revision 1.27  2005/04/30 16:12:44  haase
   Added EOF-OBJECT?

   Revision 1.26  2005/04/30 11:02:56  haase
   Fix thread bug (slotmaps sometimes not unlocked) in pretty printing

   Revision 1.25  2005/04/26 02:10:27  haase
   Added a function for returning errors and merged code in printout functions

   Revision 1.24  2005/04/25 23:52:47  haase
   Fixed bug in backtrace printing of null backtraces

   Revision 1.23  2005/04/24 22:49:05  haase
   Added %show primitive

   Revision 1.22  2005/04/24 01:58:41  haase
   Added pretty printing

   Revision 1.21  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.20  2005/04/15 13:57:45  haase
   Removed comment prefix from string sent to u8_message

   Revision 1.19  2005/04/14 00:24:57  haase
   Added number->string

   Revision 1.18  2005/03/26 20:27:38  haase
   Added lisp->string and inexact->string

   Revision 1.17  2005/03/26 20:20:32  haase
   Made flush-output do an fsync for XFILEs

   Revision 1.16  2005/03/26 20:05:27  haase
   Added FLUSH-OUTPUT primitive

   Revision 1.15  2005/03/26 04:44:28  haase
   Added MESSAGE primitive

   Revision 1.14  2005/03/25 19:24:49  haase
   Added use of u8stdio

   Revision 1.13  2005/03/25 17:48:37  haase
   More fixes for fileio separation

   Revision 1.12  2005/03/25 13:25:11  haase
   Seperated out file io functions from generic port functions

   Revision 1.11  2005/03/05 21:07:39  haase
   Numerous i18n updates

   Revision 1.10  2005/03/05 18:19:18  haase
   More i18n modifications

   Revision 1.9  2005/03/05 05:58:27  haase
   Various message changes for better initialization

   Revision 1.8  2005/03/04 04:08:33  haase
   Fixes for minor libu8 changes

   Revision 1.7  2005/02/25 16:17:58  haase
   Added file access predicates

   Revision 1.6  2005/02/15 22:39:04  haase
   More extensions to port handling functions, including GETLINE procedure

   Revision 1.5  2005/02/15 17:05:07  haase
   Upgraded ports implementation to use xfiles and added more primitives

   Revision 1.4  2005/02/15 03:03:40  haase
   Updated to use the new libu8

   Revision 1.3  2005/02/14 01:30:58  haase
   Added filestring primitive

   Revision 1.2  2005/02/12 15:16:11  haase
   Made FILEOUT return error if fopen fails

   Revision 1.1  2005/02/12 01:34:52  haase
   Added simple portfns and default output ports


*/
