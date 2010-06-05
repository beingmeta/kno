/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2010 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define U8_INLINE_IO 1

#include "fdb/dtype.h"
#include "fdb/eval.h"
#include "fdb/tables.h"
#include "fdb/ports.h"
#include "fdb/fdweb.h"

#include <libu8/xfiles.h>

#include <ctype.h>
#define FD_JSON_ANYKEY    2  /* Allow compound table keys */
#define FD_JSON_IDKEY     4  /* Allow raw identifiers as table keys */
#define FD_JSON_SYMBOLIZE 8  /* Convert table keys to symbols (and vice versa) */
#define FD_JSON_COLONIZE 16
#define FD_JSON_TICKS    32  /* Show time as unix time */
#define FD_JSON_TICKLETS 96  /* Show time as nanosecond-precision unix time */
#define FD_JSON_WARNINGS 128 /* Show time as nanosecond-precision unix time */
#define FD_JSON_DEFAULTS (FD_JSON_COLONIZE|FD_JSON_SYMBOLIZE)

#define FD_JSON_MAXFLAGS 256

static fd_exception JSON_Error="JSON Parsing Error";

static int readc(U8_INPUT *in)
{
  int c=u8_getc(in);
  if (c=='\\') {
    u8_ungetc(in,c);
    return fd_read_escape(in);}
  else return c;
}

static int skip_whitespace(U8_INPUT *in)
{
  int c=u8_getc(in);
  while ((c>0) && (u8_isspace(c))) c=u8_getc(in);
  if (c>0) u8_ungetc(in,c);
  return c;
}

static fdtype json_parse(U8_INPUT *in,int flags);
static fdtype json_vector(U8_INPUT *in,int flags);
static fdtype json_table(U8_INPUT *in,int flags);
static fdtype json_string(U8_INPUT *in,int flags);
static fdtype json_atom(U8_INPUT *in,int flags);

static fdtype json_parse(U8_INPUT *in,int flags)
{
  int c=skip_whitespace(in); fdtype v;
  if (c=='[') return json_vector(in,flags);
  else if (c=='{') return json_table(in,flags);
  else if (c=='"') return json_string(in,flags);
  else return json_atom(in,0);
}

static fdtype parse_error(u8_output out,fdtype result,int report)
{
  fd_clear_errors(report);
  fd_decref(result);
  return fd_init_string(NULL,out->u8_outptr-out->u8_outbuf,out->u8_outbuf);
}

static fdtype json_atom(U8_INPUT *in,int flags)
{
  fdtype result;
  struct U8_OUTPUT out; u8_byte _buf[256]; int c=readc(in);
  U8_INIT_OUTPUT_BUF(&out,256,_buf);
  while ((u8_isalnum(c)) || (c=='-') || (c=='_') || (c=='+') || (c=='.')) {
    u8_putc(&out,c); c=readc(in);}
  if (c>=0) u8_ungetc(in,c);
  if ((strcmp(out.u8_outbuf,"true")==0)) result=FD_TRUE;
  else if ((strcmp(out.u8_outbuf,"false")==0)) result=FD_FALSE;
  else if  ((strcmp(out.u8_outbuf,"null")==0)) result=FD_EMPTY_CHOICE;
  else result=fd_parse(out.u8_outbuf);
  if (FD_ABORTP(result)) return parse_error(&out,result,flags&FD_JSON_WARNINGS);
  if (out.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(out.u8_outbuf);
  return result;
}

static fdtype json_string(U8_INPUT *in,int flags)
{
  struct U8_OUTPUT out; int c=readc(in); /* Skip '"' */
  int init_escape=0;
  U8_INIT_OUTPUT(&out,32);
  c=u8_getc(in);
  if ((c=='\\')&&(flags&FD_JSON_COLONIZE)) {
    c=u8_getc(in);
    if (c==':') init_escape=1;
    u8_putc(&out,c);
    c=u8_getc(in);}
  while (c>=0) {
    if (c=='"') break;
    else if (c=='\\') {
      c=fd_read_escape(in);
      u8_putc(&out,c); c=u8_getc(in);
      continue;}
    u8_putc(&out,c);
    c=u8_getc(in);}
  if (init_escape)
    return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
  else if ((flags&FD_JSON_COLONIZE)&&(out.u8_outbuf[0]==':')) {
    fdtype result=fd_parse(out.u8_outbuf+1);
    if (FD_ABORTP(result)) return parse_error(&out,result,flags&FD_JSON_WARNINGS);
    u8_free(out.u8_outbuf);
    return result;}
  else return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
}

static fdtype json_intern(U8_INPUT *in,int flags)
{
  struct U8_OUTPUT out; int c=readc(in); /* Skip '"' */
  c=u8_getc(in);
  U8_INIT_OUTPUT(&out,64);
  while (c>=0) {
    if (c=='"') break;
    else if (c=='\\') {
      c=fd_read_escape(in);
      u8_putc(&out,c); c=u8_getc(in);
      continue;}
    u8_putc(&out,c);
    c=u8_getc(in);}
  if (out.u8_outbuf[0]==':') {
    fdtype result=fd_parse(out.u8_outbuf+1);
    if (FD_ABORTP(result)) return parse_error(&out,result,flags&FD_JSON_WARNINGS);
    u8_free(out.u8_outbuf);
    return result;}
  else {
    fdtype result=fd_parse(out.u8_outbuf);
    if (FD_ABORTP(result)) return parse_error(&out,result,flags&FD_JSON_WARNINGS);
    u8_free(out.u8_outbuf);
    return result;}
}

static fdtype json_key(U8_INPUT *in,int flags)
{
  int c=skip_whitespace(in);
  if (c=='"')
    if (flags&FD_JSON_SYMBOLIZE) 
      return json_intern(in,flags);
    else return json_string(in,flags);
  else if (((c=='{')||(c=='['))&&(!(flags&FD_JSON_ANYKEY))) {
    fd_seterr("Invalid JSON key","json_key",NULL,FD_VOID);
    return FD_PARSE_ERROR;}
  else if ((!(flags&FD_JSON_IDKEY))&&(!((u8_isdigit(c))||(c=='+')||(c=='-')))) {
    fd_seterr("Invalid JSON key","json_key",NULL,FD_VOID);
    return FD_PARSE_ERROR;}
  else {
    fdtype result=json_atom(in,flags);
    if ((FD_FIXNUMP(result))||(FD_BIGINTP(result))||(FD_FLONUMP(result)))
      return result;
    else if ((FD_SYMBOLP(result))&&(flags&FD_JSON_IDKEY))
      return result;
    else if (flags&FD_JSON_ANYKEY)
      return result;
    else {
      fd_decref(result);
      fd_seterr("Invalid JSON key","json_key",NULL,FD_VOID);
      return FD_PARSE_ERROR;}}
}

static fdtype json_vector(U8_INPUT *in,int flags)
{
  int n_elts=0, max_elts=16, c, i;
  unsigned int good_pos=in->u8_inptr-in->u8_inbuf;
  fdtype *elts; 
  if (u8_getc(in)!='[') return FD_ERROR_VALUE;
  else elts=u8_alloc_n(16,fdtype);
  c=skip_whitespace(in);
  while (c>=0) {
    good_pos=in->u8_inptr-in->u8_inbuf;
    if (c==']') {
      u8_getc(in); /* Absorb ] */
      return fd_init_vector(NULL,n_elts,elts);}
    else if (c==',') {
      c=u8_getc(in); c=skip_whitespace(in);}
    else {
      fdtype elt;
      if (n_elts==max_elts)  {
	int new_max=max_elts*2;
	fdtype *newelts=u8_realloc(elts,sizeof(fdtype)*new_max);
	if (newelts) {elts=newelts; max_elts=new_max;}
	else {
	  u8_seterr(fd_MallocFailed,"json_vector",NULL);
	  break;}}
      elts[n_elts++]=elt=json_parse(in,flags);
      if (FD_ABORTP(elt)) break;
      c=skip_whitespace(in);}}
  i=0; while (i<n_elts) {fd_decref(elts[i]); i++;}
  return fd_err(JSON_Error,"json_vector",
		u8_strdup(in->u8_inbuf+good_pos),
		FD_VOID);
}

static fdtype json_table(U8_INPUT *in,int flags)
{
  int n_elts=0, max_elts=16, c, i;
   unsigned int good_pos=in->u8_inptr-in->u8_inbuf;
   struct FD_KEYVAL *kv;
  if (u8_getc(in)!='{') return FD_ERROR_VALUE;
  else kv=u8_alloc_n(16,struct FD_KEYVAL);
  c=skip_whitespace(in);
  while (c>=0) {
    good_pos=in->u8_inptr-in->u8_inbuf;
    if (c=='}') {
      u8_getc(in); /* Absorb ] */
      return fd_init_slotmap(NULL,n_elts,kv);}
    else if (c==',') {
      c=u8_getc(in); c=skip_whitespace(in);}
    else {
      if (n_elts==max_elts)  {
	int new_max=max_elts*2;
	struct FD_KEYVAL *newelts=u8_realloc(kv,sizeof(struct FD_KEYVAL)*new_max);
	if (newelts) {kv=newelts; max_elts=new_max;}
	else {
	  u8_seterr(fd_MallocFailed,"json_table",NULL);
	  break;}}
      kv[n_elts].key=json_key(in,flags);
      if (FD_ABORTP(kv[n_elts].key)) break;
      c=skip_whitespace(in);
      if (c==':') c=u8_getc(in);
      else return FD_EOD;
      kv[n_elts].value=json_parse(in,flags);
      if (FD_ABORTP(kv[n_elts].value)) break;
      n_elts++; c=skip_whitespace(in);}}
  i=0; while (i<n_elts) {
    fd_decref(kv[i].key); fd_decref(kv[i].value); i++;}
  return fd_err(JSON_Error,"json_table",
		u8_strdup(in->u8_inbuf+good_pos),
		FD_VOID);
}

static fdtype jsonparseprim(fdtype in,fdtype flags_arg)
{
  int flags=0;
  if (FD_FALSEP(flags_arg)) {}
  else if (FD_TRUEP(flags_arg)) flags=FD_JSON_COLONIZE;
  else if (FD_FIXNUMP(flags_arg)) 
    flags=FD_FIX2INT(flags_arg);
  else return fd_type_error("int","jsonparseprim",flags_arg);
  if (FD_PRIM_TYPEP(in,fd_port_type)) {
    struct FD_PORT *p=FD_GET_CONS(in,fd_port_type,struct FD_PORT *);
    U8_INPUT *in=p->in;
    return json_parse(in,flags);}
  else if (FD_STRINGP(in)) {
    struct U8_INPUT inport;
    U8_INIT_STRING_INPUT(&inport,FD_STRLEN(in),FD_STRDATA(in));
    return json_parse(&inport,flags);}
  else if (FD_PACKETP(in)) {
    struct U8_INPUT inport;
    U8_INIT_STRING_INPUT(&inport,FD_PACKET_LENGTH(in),FD_PACKET_DATA(in));
    return json_parse(&inport,flags);}
  else return fd_type_error("string or stream","jsonparse",in);
}

/* JSON output */

static void json_unparse(u8_output out,fdtype x,int flags,fdtype oidfn,fdtype slotfn,fdtype miscfn);

static void json_escape(u8_output out,u8_string s)
{
  u8_string start=s, scan=start; int c;
  while (c=(*scan))
    if ((c<128)&&((c=='"')||(c=='\\')||(iscntrl(c)))) {
      if (scan>start) u8_putn(out,start,scan-start);
      u8_putc(out,'\\');
      switch (c) {
      case '\n': u8_putc(out,'n'); break;
      case '\b': u8_putc(out,'b'); break;
      case '\f': u8_putc(out,'f'); break;
      case '\r': u8_putc(out,'r'); break;
      case '\t': u8_putc(out,'t'); break;
      default: u8_putc(out,c); break;}
      scan++; start=scan;}
    else scan++;
  if (scan>start) u8_putn(out,start,scan-start);
}

static void json_lower(u8_output out,u8_string s)
{
  u8_byte *scan=s;
  int c=u8_sgetc(&scan);
  while (c>=0) {
    int lc=u8_tolower(c);
    if (lc>=128) u8_putc(out,lc);
    else switch (lc) {
      case '\n': u8_puts(out,"\\n"); break;
      case '\r': u8_puts(out,"\\r"); break;
      case '\t': u8_puts(out,"\\t"); break;
      case '\\': u8_puts(out,"\\\\"); break;
      case '"': u8_puts(out,"\\\""); break;
      default: u8_putc(out,lc);}
    c=u8_sgetc(&scan);}
}

static int json_slotval(u8_output out,fdtype key,fdtype value,int flags,fdtype slotfn,fdtype oidfn,fdtype miscfn)
{
  if (FD_VOIDP(value)) return 0;
  else {
    fdtype slotname=((FD_VOIDP(slotfn))?(FD_VOID):(fd_apply(slotname,1,&key)));
    if (FD_VOIDP(slotname))
      if ((FD_SYMBOLP(key))&&(flags&FD_JSON_SYMBOLIZE)) {
	u8_string pname=FD_SYMBOL_NAME(key);
	if (!(flags&FD_JSON_IDKEY)) u8_putc(out,'"');
	json_lower(out,pname);
	if (!(flags&FD_JSON_IDKEY)) u8_putc(out,'"');}
      else json_unparse(out,key,flags,oidfn,slotfn,miscfn);
    else if (FD_STRINGP(slotname)) {
      u8_putc(out,'"');
      json_escape(out,FD_STRDATA(slotname)); fd_decref(slotname);
      u8_putc(out,'"');}
    else if (FD_SYMBOLP(slotname)) {
      json_escape(out,FD_SYMBOL_NAME(slotname));}
    else return 0;
    u8_puts(out,": ");
    json_unparse(out,value,flags,oidfn,slotfn,miscfn);
    return 1;}
}

static void json_unparse(u8_output out,fdtype x,int flags,fdtype slotfn,fdtype oidfn,fdtype miscfn)
{
  if (FD_FIXNUMP(x)) u8_printf(out,"%d",FD_FIX2INT(x));
  else if (FD_OIDP(x)) {
    fdtype oidval=((FD_VOIDP(oidfn))?(FD_VOID):(fd_dapply(oidfn,1,&x)));
    if (FD_VOIDP(oidval)) {
      FD_OID addr=FD_OID_ADDR(x);
      if (flags)
	u8_printf(out,"\":@%x/%x\"",FD_OID_HI(addr),FD_OID_LO(addr));
      else u8_printf(out,"\"@%x/%x\"",FD_OID_HI(addr),FD_OID_LO(addr));}
    else json_unparse(out,oidval,flags,slotfn,oidfn,miscfn);
    fd_decref(oidval);}
  else if ((FD_SYMBOLP(x))&&(FD_VOIDP(miscfn))) {
    u8_string pname=FD_SYMBOL_NAME(x);
    u8_puts(out,"\":");
    json_escape(out,pname);
    u8_puts(out,"\"");}
  else if (FD_STRINGP(x)) {
    u8_string pstring=FD_STRDATA(x);
    u8_puts(out,"\"");
    if ((flags)&&(pstring[0]==':')) u8_putc(out,'\\');
    json_escape(out,pstring);
    u8_puts(out,"\"");}
  else if ((FD_BIGINTP(x))||(FD_FLONUMP(x)))
    fd_unparse(out,x);
  else if (FD_VECTORP(x)) {
    int i=0; int lim=FD_VECTOR_LENGTH(x);
    while (i<lim) {
      if (i>0) u8_putc(out,','); else u8_putc(out,'[');
      json_unparse(out,FD_VECTOR_REF(x,i),flags,slotfn,oidfn,miscfn);
      i++;}
    u8_putc(out,']');}
  else if ((FD_CHOICEP(x))||(FD_ACHOICEP(x))) {
    int elt_count=0; FD_DO_CHOICES(e,x) {
      if (elt_count>0) u8_putc(out,',');
      else u8_puts(out,"[");
      json_unparse(out,e,flags,slotfn,oidfn,miscfn);
      elt_count++;}
    u8_putc(out,']');}
  else if (FD_PRIM_TYPEP(x,fd_timestamp_type)) {
    struct FD_TIMESTAMP *tm=
      FD_GET_CONS(x,fd_timestamp_type,struct FD_TIMESTAMP *);
    if (flags&FD_JSON_COLONIZE) 
      u8_printf(out,"\":#T%iSXGt\"",&(tm->xtime));
    else if (tm->xtime.u8_tick<0)  u8_puts(out,"-1"); /* Invalid time */
    else if (flags&FD_JSON_TICKLETS) {
      double dtick=((unsigned long long)tm->xtime.u8_tick)+
	(tm->xtime.u8_nsecs)*0.000000001;
      u8_printf(out,"%f",dtick);}
    else if (flags&FD_JSON_TICKS)
      u8_printf(out,"%lld",(unsigned long long)tm->xtime.u8_tick);
    else u8_printf(out,"\"%iSXGt\"",&(tm->xtime));}
  else if (FD_TABLEP(x)) {
    fdtype keys=fd_getkeys(x);
    int elt_count=0; FD_DO_CHOICES(key,keys) {
      fdtype value=fd_get(x,key,FD_VOID);
      if (!(FD_VOIDP(value))) {
	if (elt_count>0) u8_putc(out,',');
	else u8_puts(out,"{");
	if (json_slotval(out,key,value,flags,slotfn,oidfn,miscfn)) elt_count++;
	fd_decref(value);}}
    u8_puts(out,"}");}
  else if (FD_EMPTY_CHOICEP(x)) u8_puts(out,"[]");
  else if (FD_TRUEP(x)) u8_puts(out,"true");
  else if (FD_FALSEP(x)) u8_puts(out,"false");
  else {
    u8_byte buf[256]; struct U8_OUTPUT tmpout;
    fdtype tval=((FD_VOIDP(miscfn))?(FD_VOID):(fd_dapply(miscfn,1,&x)));
    U8_INIT_OUTPUT_BUF(&tmpout,256,buf);
    if (FD_VOIDP(tval)) fd_unparse(&tmpout,x);
    else fd_unparse(&tmpout,tval);
    fd_decref(tval);
    if (flags) u8_puts(out,"\":"); else u8_putc(out,'"');
    json_escape(out,tmpout.u8_outbuf);
    u8_close_output(&tmpout);}
}

static fdtype jsonoutput(fdtype x,fdtype flags_arg,fdtype slotfn,fdtype oidfn,fdtype miscfn)
{
  u8_output out=fd_get_default_output();
  int flags=
    ((FD_FALSEP(flags_arg))?(0):
     (FD_TRUEP(flags_arg)) ? (FD_JSON_DEFAULTS) :
     (FD_FIXNUMP(flags_arg))?(FD_FIX2INT(flags_arg)):(-1));
  if ((flags<0)||(flags>=FD_JSON_MAXFLAGS))
    return fd_type_error("fixnum/flags","jsonoutput",flags_arg);
  json_unparse(out,x,flags,slotfn,oidfn,miscfn);
  if (out==fd_default_output) u8_flush(out);
  return FD_VOID;
}

static fdtype jsonstring(fdtype x,fdtype flags_arg,fdtype slotfn,fdtype oidfn,fdtype miscfn)
{
  struct U8_OUTPUT tmpout; 
  int flags=
    ((FD_FALSEP(flags_arg))?(0):
     (FD_TRUEP(flags_arg)) ? (FD_JSON_DEFAULTS) :
     (FD_FIXNUMP(flags_arg))?(FD_FIX2INT(flags_arg)):(-1));
  if ((flags<0)||(flags>=FD_JSON_MAXFLAGS))
    return fd_type_error("fixnum/flags","jsonoutput",flags_arg);
  U8_INIT_OUTPUT(&tmpout,128);
  json_unparse(&tmpout,x,flags,slotfn,oidfn,miscfn);
  return fd_init_string(NULL,tmpout.u8_outptr-tmpout.u8_outbuf,tmpout.u8_outbuf);
}

/* Module initialization */

FD_EXPORT void fd_init_json_c()
{
  fdtype module=fd_new_module("FDWEB",(FD_MODULE_SAFE));
  fdtype unsafe_module=fd_new_module("FDWEB",0);

  fd_idefn(module,fd_make_cprim2x
	   ("JSONPARSE",jsonparseprim,1,-1,FD_VOID,-1,FD_INT2DTYPE(FD_JSON_DEFAULTS)));
  fd_idefn(unsafe_module,fd_make_cprim2x
	   ("JSONPARSE",jsonparseprim,1,-1,FD_VOID,-1,FD_INT2DTYPE(FD_JSON_DEFAULTS)));
  fd_idefn(module,fd_make_cprim5x("->JSON",jsonstring,1,
				  -1,FD_VOID,-1,FD_TRUE,
				  -1,FD_VOID,-1,FD_VOID,-1,FD_VOID));
  fd_idefn(unsafe_module,fd_make_cprim5x("->JSON",jsonstring,1,
					 -1,FD_VOID,-1,FD_INT2DTYPE(FD_JSON_DEFAULTS),
					 -1,FD_VOID,-1,FD_VOID,-1,FD_VOID));
  fd_idefn(module,fd_make_cprim5x("JSONOUTPUT",jsonoutput,1,
				  -1,FD_VOID,-1,FD_INT2DTYPE(FD_JSON_DEFAULTS),
				  -1,FD_VOID,-1,FD_VOID,-1,FD_VOID));
  fd_idefn(unsafe_module,fd_make_cprim5x("JSONOUTPUT",jsonoutput,1,
					 -1,FD_VOID,-1,FD_INT2DTYPE(FD_JSON_DEFAULTS),
					 -1,FD_VOID,-1,FD_VOID,-1,FD_VOID));

  fd_register_source_file(versionid);
}

