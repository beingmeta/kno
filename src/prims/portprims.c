/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
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
#include "framerd/pprint.h"

#include <libu8/u8streamio.h>
#include <libu8/u8crypto.h>

#include <zlib.h>

u8_condition fd_UnknownEncoding=_("Unknown encoding");

#define fast_eval(x,env) (fd_stack_eval(x,env,_stack,0))

/* Making ports */

static lispval make_port(U8_INPUT *in,U8_OUTPUT *out,u8_string id)
{
  struct FD_PORT *port = u8_alloc(struct FD_PORT);
  FD_INIT_CONS(port,fd_port_type);
  port->port_input = in;
  port->port_output = out;
  port->port_id = id;
  port->port_lisprefs = FD_EMPTY;
  return LISP_CONS(port);
}

static u8_output get_output_port(lispval portarg)
{
  if ((VOIDP(portarg))||(FD_TRUEP(portarg)))
    return u8_current_output;
  else if (FD_PORTP(portarg)) {
    struct FD_PORT *p=
      fd_consptr(struct FD_PORT *,portarg,fd_port_type);
    return p->port_output;}
  else return NULL;
}

static u8_input get_input_port(lispval portarg)
{
  if (VOIDP(portarg))
    return NULL; /* get_default_output(); */
  else if (FD_PORTP(portarg)) {
    struct FD_PORT *p=
      fd_consptr(struct FD_PORT *,portarg,fd_port_type);
    return p->port_input;}
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
    if (p->port_input)
      return FD_TRUE;
    else return FD_FALSE;}
  else return FD_FALSE;
}

static lispval output_portp(lispval arg)
{
  if (FD_PORTP(arg)) {
    struct FD_PORT *p=
      fd_consptr(struct FD_PORT *,arg,fd_port_type);
    if (p->port_output)
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
  struct FD_INBUF in = { 0 };
  FD_INIT_BYTE_INPUT(&in,FD_PACKET_DATA(packet),
                     FD_PACKET_LENGTH(packet));
  object = fd_read_dtype(&in);
  return object;
}

static lispval lisp2packet(lispval object,lispval initsize)
{
  size_t size = FIX2INT(initsize);
  struct FD_OUTBUF out = { 0 };
  FD_INIT_BYTE_OUTPUT(&out,size);
  int bytes = fd_write_dtype(&out,object);
  if (bytes<0)
    return FD_ERROR;
  else if ( (BUFIO_ALLOC(&out)) == FD_HEAP_BUFFER )
    return fd_init_packet(NULL,bytes,out.buffer);
  else {
    lispval packet = fd_make_packet
      (NULL,out.bufwrite-out.buffer,out.buffer);
    fd_close_outbuf(&out);
    return packet;}
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
    if (port->port_id)
      return lispval_string(port->port_id);
    else return FD_FALSE;}
  else return fd_type_error(_("port"),"portid",port_arg);
}

static lispval portdata(lispval port_arg)
{
  if (FD_PORTP(port_arg)) {
    struct FD_PORT *port = (struct FD_PORT *)port_arg;
    if (port->port_output)
      return fd_substring(port->port_output->u8_outbuf,port->port_output->u8_write);
    else return fd_substring(port->port_output->u8_outbuf,port->port_output->u8_outlim);}
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
static ssize_t get_more_data(u8_input in,ssize_t lim);
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
  ssize_t lim, matchlen = 0, maxbuf=in->u8_bufsz;
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
    else if ((lim>0) && ((in->u8_inlim-in->u8_read)>lim))
      return FD_EOF;
    else if (in->u8_fillfn) {
      if ((in->u8_inlim-in->u8_read)>=(maxbuf-16))
        maxbuf=maxbuf*2;
      ssize_t more_data = get_more_data(in,maxbuf);
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

static lispval lisp_pprint(int n,lispval *args)
{
  U8_OUTPUT *out=NULL;
  struct U8_OUTPUT tmpout;
  struct PPRINT_CONTEXT ppcxt={0};
  int arg_i=0, used[7]={0};
  int indent=0, close_port=0, stringout=0, col=0, depth=0;
  lispval opts = VOID, port_arg=VOID, obj = args[0]; used[0]=1;
  if (n>7) return fd_err(fd_TooManyArgs,"lisp_pprint",NULL,VOID);
  while (arg_i<n)
    if (used[arg_i]) arg_i++;
    else {
      lispval arg = args[arg_i];
      if ( (out == NULL) &&
           (out = get_output_port(arg)) )
        used[arg_i]=1;
      else if (FD_FIXNUMP(arg)) {
        ppcxt.pp_maxcol = FD_FIX2INT(arg);
        used[arg_i]=1;}
      else if (FD_STRINGP(arg)) {
        ppcxt.pp_margin = CSTRING(arg);
        ppcxt.pp_margin_len = FD_STRLEN(arg);
        used[arg_i]= 1;}
      else if (FD_TABLEP(arg)) {
        opts=arg;
        used[arg_i]=1;}
      else if (FD_FALSEP(arg)) {
        stringout=1; used[arg_i]=1;}
      else u8_log(LOG_WARN,"BadPPrintArg","%q",arg);
      arg_i++;}
  if ( (FD_PAIRP(opts)) || (FD_TABLEP(opts)) ) {
    lispval maxelts = fd_getopt(opts,maxelts_symbol,FD_VOID);
    lispval colval = fd_getopt(opts,column_symbol,FD_VOID);
    lispval depthval = fd_getopt(opts,depth_symbol,FD_VOID);
    lispval maxchars = fd_getopt(opts,maxchars_symbol,FD_VOID);
    lispval maxbytes = fd_getopt(opts,maxbytes_symbol,FD_VOID);
    lispval maxkeys = fd_getopt(opts,maxkeys_symbol,FD_VOID);
    lispval list_max = fd_getopt(opts,listmax_symbol,FD_VOID);
    lispval vec_max = fd_getopt(opts,vecmax_symbol,FD_VOID);
    lispval choice_max = fd_getopt(opts,choicemax_symbol,FD_VOID);
    if (FD_FIXNUMP(colval))   col   = FD_FIX2INT(colval);
    if (FD_FIXNUMP(depthval)) depth = FD_FIX2INT(depthval);

    if (FD_FIXNUMP(maxelts)) ppcxt.pp_maxelts=FD_FIX2INT(maxelts);
    if (FD_FIXNUMP(maxchars)) ppcxt.pp_maxchars=FD_FIX2INT(maxchars);
    if (FD_FIXNUMP(maxbytes)) ppcxt.pp_maxbytes=FD_FIX2INT(maxbytes);
    if (FD_FIXNUMP(maxkeys)) ppcxt.pp_maxkeys=FD_FIX2INT(maxkeys);
    if (FD_FIXNUMP(list_max)) ppcxt.pp_list_max=FD_FIX2INT(list_max);
    if (FD_FIXNUMP(vec_max)) ppcxt.pp_vector_max=FD_FIX2INT(vec_max);
    if (FD_FIXNUMP(choice_max)) ppcxt.pp_choice_max=FD_FIX2INT(choice_max);
    fd_decref(maxbytes); fd_decref(maxchars);
    fd_decref(maxelts); fd_decref(maxkeys);
    fd_decref(list_max); fd_decref(vec_max);
    fd_decref(colval); fd_decref(depthval);
    fd_decref(choice_max);
    if (ppcxt.pp_margin == NULL) {
      lispval margin_opt = fd_getopt(opts,margin_symbol,FD_VOID);
      if (FD_STRINGP(margin_opt)) {
        ppcxt.pp_margin=CSTRING(margin_opt);
        ppcxt.pp_margin_len=FD_STRLEN(margin_opt);}
      else if ( (FD_UINTP(margin_opt)) ) {
        long long margin_width = FD_FIX2INT(margin_opt);
        u8_byte *margin = alloca(margin_width+1);
        u8_byte *scan=margin, *limit=scan+margin_width;
        while (scan<limit) *scan++=' ';
        *scan='\0';
        ppcxt.pp_margin=margin;
        /* Since it's all ASCII spaces, margin_len (bytes) =
           margin_width (chars) */
        ppcxt.pp_margin_len=margin_width;}
      else if ( (FD_VOIDP(margin_opt)) || (FD_DEFAULTP(margin_opt)) ) {}
      else u8_log(LOG_WARN,"BadPPrintMargin","%q",margin_opt);
      fd_decref(margin_opt);}
    if (ppcxt.pp_maxcol>0) {
      lispval maxcol = fd_getopt(opts,maxcol_symbol,FD_VOID);
      if (FD_VOIDP(maxcol))
        maxcol = fd_getopt(opts,width_symbol,FD_VOID);
      if (FD_FIXNUMP(maxcol))
        ppcxt.pp_maxcol=FD_FIX2INT(maxcol);
      else if ( (FD_VOIDP(maxcol)) || (FD_DEFAULTP(maxcol)) )
        ppcxt.pp_maxcol=0;
      else ppcxt.pp_maxcol=-1;
      fd_decref(maxcol);}}
  if (out == NULL) {
    lispval port = fd_getopt(opts,FDSYM_OUTPUT,VOID);
    if (!(VOIDP(port))) out = get_output_port(port);
    if (out)
      port_arg=port;
    else {fd_decref(port);}}
  if (out == NULL) {
    lispval filename = fd_getopt(opts,FDSYM_FILENAME,VOID);
    if (!(VOIDP(filename))) {
      out = (u8_output) u8_open_output_file(CSTRING(filename),NULL,-1,-1);
      if (out == NULL) {
        fd_decref(filename);
        return FD_ERROR_VALUE;}
      else {
        fd_decref(filename);
        close_port=1;}}}
  if (stringout) {
    U8_INIT_OUTPUT(&tmpout,1000);
    out=&tmpout;}
  else out = get_output_port(VOID);
  col = fd_pprinter(out,obj,indent,col,depth,NULL,NULL,&ppcxt);
  if (stringout)
    return fd_init_string(NULL,tmpout.u8_write-tmpout.u8_outbuf,
                          tmpout.u8_outbuf);
  else {
    u8_flush(out);
    if (close_port) u8_close_output(out);
    fd_decref(port_arg);
    return FD_INT(col);}
}

/* LIST object */

static int get_stringopt(lispval opts,lispval optname,u8_string *strval)
{
  lispval v = fd_getopt(opts,optname,FD_VOID);
  if (FD_VOIDP(v)) {
    return 0;}
  else if (FD_STRINGP(v)) {
    *strval = FD_CSTRING(v);
    fd_decref(v);
    return 1;}
  else {
    if (FD_SYMBOLP(optname))
      fd_seterr("BadStringOpt","lisp_list_object",FD_SYMBOL_NAME(optname),v);
    else if (FD_STRINGP(optname))
      fd_seterr("BadStringOpt","lisp_list_object",FD_CSTRING(optname),v);
    else fd_seterr("BadStringOpt","lisp_list_object",NULL,v);
    fd_decref(v);
    return -1;}
}

static int get_fixopt(lispval opts,lispval optname,long long *intval)
{
  lispval v = fd_getopt(opts,optname,FD_VOID);
  if (FD_VOIDP(v)) return 0;
  else if (FD_FIXNUMP(v)) {
    *intval = FD_FIX2INT(v);
    return 1;}
  else if (FD_FALSEP(v)) {
    *intval = 0;
    return 1;}
  else {
    if (FD_SYMBOLP(optname))
      fd_seterr("BadFixOpt","lisp_list_object",FD_SYMBOL_NAME(optname),v);
    else if (FD_STRINGP(optname))
      fd_seterr("BadStringOpt","lisp_list_object",FD_CSTRING(optname),v);
    else fd_seterr("BadFixOpt","lisp_list_object",NULL,v);
    fd_decref(v);
    return -1;}
}

static lispval label_symbol, width_symbol, depth_symbol, output_symbol;

static lispval lisp_listdata(lispval object,lispval opts,lispval stream)
{
  u8_string label=NULL, pathref=NULL, indent="";
  long long width = 100, detail = -1;
  if (FD_FIXNUMP(opts)) {
    detail = FD_FIX2INT(opts);
    opts = FD_FALSE;}
  else if (FD_TRUEP(opts)) {
    detail = 0;
    opts = FD_FALSE;}
  else if (get_stringopt(opts,label_symbol,&label)<0)
    return FD_ERROR;
  else if (get_stringopt(opts,margin_symbol,&indent)<0)
    return FD_ERROR;
  else if (get_fixopt(opts,width_symbol,&width)<0)
    return FD_ERROR;
  else if (get_fixopt(opts,depth_symbol,&detail)<0)
    return FD_ERROR;
  else if (get_fixopt(opts,detail_symbol,&detail)<0)
    return FD_ERROR;
  else NO_ELSE;
  if (FD_VOIDP(stream))
    stream = fd_getopt(opts,output_symbol,FD_VOID);
  else fd_incref(stream);
  U8_OUTPUT *out = get_output_port(stream);
  int rv = fd_list_object(out,object,label,pathref,indent,NULL,width,detail);
  u8_flush(out);
  fd_decref(stream);
  if (rv<0)
    return FD_ERROR;
  else return FD_VOID;
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
    u8_condition error = NULL;
    const unsigned char *data=
      ((STRINGP(arg))?(CSTRING(arg)):(FD_PACKET_DATA(arg)));
    unsigned int data_len=
      ((STRINGP(arg))?(STRLEN(arg)):(FD_PACKET_LENGTH(arg)));
    struct FD_OUTBUF out = { 0 };
    int flags = 0; /* FDPP_FHCRC */
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
      fd_close_outbuf(&out);
      return FD_ERROR;}
    crc = u8_crc32(0,data,data_len);
    intval = fd_flip_word(crc); fd_write_4bytes(&out,intval);
    intval = fd_flip_word(data_len); fd_write_4bytes(&out,intval);
    if ( (BUFIO_ALLOC(&out)) == FD_HEAP_BUFFER )
      return fd_init_packet(NULL,out.bufwrite-out.buffer,out.buffer);
    else {
      lispval packet = fd_make_packet(NULL,out.bufwrite-out.buffer,out.buffer);
      fd_close_outbuf(&out);
      return packet;}}
}

/* Port type operations */

/* The port type */

static int unparse_port(struct U8_OUTPUT *out,lispval x)
{
  struct FD_PORT *p = fd_consptr(fd_port,x,fd_port_type);
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

static void recycle_port(struct FD_RAW_CONS *c)
{
  struct FD_PORT *p = (struct FD_PORT *)c;
  if (p->port_input) {
    u8_close_input(p->port_input);}
  if (p->port_output) {
    u8_close_output(p->port_output);}
  if (p->port_id) u8_free(p->port_id);
  if (p->port_lisprefs != FD_NULL) fd_decref(p->port_lisprefs);
 if (FD_MALLOCD_CONSP(c)) u8_free(c);
}

/* Initializing some symbols */

static void init_portprims_symbols()
{
  column_symbol=fd_intern("COLUMN");
  depth_symbol=fd_intern("DEPTH");
  maxcol_symbol=fd_intern("MAXCOL");
  width_symbol=fd_intern("WIDTH");
  margin_symbol=fd_intern("MARGIN");
  maxelts_symbol=fd_intern("MAXELTS");
  maxchars_symbol=fd_intern("MAXCHARS");
  maxbytes_symbol=fd_intern("MAXBYTES");
  maxkeys_symbol=fd_intern("MAXKEYS");
  listmax_symbol=fd_intern("LISTMAX");
  vecmax_symbol=fd_intern("VECMAX");
  choicemax_symbol=fd_intern("CHOICEMAX");
  label_symbol = fd_intern("LABEL");
  output_symbol = fd_intern("OUTPUT");
  detail_symbol = fd_intern("DETAIL");
}

/* The init function */

FD_EXPORT void fd_init_portprims_c()
{
  u8_register_source_file(_FILEINFO);

  fd_unparsers[fd_port_type]=unparse_port;
  fd_recyclers[fd_port_type]=recycle_port;

  init_portprims_symbols();

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

  fd_idefnN(fd_scheme_module,"PPRINT",lisp_pprint,FD_NEEDS_1_ARG|FD_NDCALL,
            "(pprint *object* *port* *width* *margin*)\n"
            "Generates a formatted representation of *object* "
            "on *port* () with a width of *width* columns with a "
            "left margin of *margin* which is either number of columns "
            "or a string.");
  fd_idefn3(fd_scheme_module,"LISTDATA",lisp_listdata,FD_NEEDS_1_ARG|FD_NDCALL,
            "(LISTDATA *object* *opts* [*port*])",
            -1,FD_VOID,-1,FD_VOID,-1,FD_VOID);

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
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
