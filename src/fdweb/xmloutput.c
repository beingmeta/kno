/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2007 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define U8_INLINE_IO 1
#define FD_PROVIDE_FASTEVAL 1

#include "fdb/dtype.h"
#include "fdb/fddb.h"
#include "fdb/pools.h"
#include "fdb/frames.h"
#include "fdb/tables.h"
#include "fdb/eval.h"
#include "fdb/ports.h"
#include "fdb/fdweb.h"
#include "fdb/support.h"

#include "fdb/support.h"

#ifndef BACKTRACE_INDENT_DEPTH
#define BACKTRACE_INDENT_DEPTH 12
#endif

static int backtrace_indent_depth=BACKTRACE_INDENT_DEPTH;

FD_EXPORT void fd_pprint_focus
  (U8_OUTPUT *out,fdtype entry,fdtype focus,u8_string prefix,
   int indent,int width,u8_string focus_prefix,u8_string focus_suffix);

#include <libu8/xfiles.h>

FD_EXPORT void fd_uri_output(u8_output out,u8_string uri,char *escape);

#define strd u8_strdup

/*
  (XMLOUT .. .. ..) does escaping
  (XMLTAG 'tag p v p v ...)
  (DIV () ...)
  (SPAN () ...)
  (special* () ...)
  (special ....)

  (URI string)
*/

static fdtype xmloidfn_symbol, obj_name, id_symbol, quote_symbol;
static fdtype href_symbol, class_symbol, raw_name_symbol, browseinfo_symbol;

/* Utility output functions */

static void emit_xmlname(u8_output out,u8_string name)
{
  u8_puts(out,name);
}

static void attrib_entify(u8_output out,u8_string value)
{
  u8_byte *scan=value; int c;
  while ((c=u8_sgetc(&scan))>=0)
    if (c=='\'') u8_puts(out,"&#39;");
    else if (c=='<') u8_puts(out,"&#60;");
    else if (c=='&') u8_puts(out,"&#38;");
    else if (c=='>') u8_puts(out,"&#62;");
    else u8_putc(out,c);
} 

FD_INLINE_FCN void entify(u8_output out,u8_string value)
{
  u8_byte *scan=value; int c;
  while ((c=u8_sgetc(&scan))>=0)
    if (c=='<') u8_puts(out,"&#60;");
    else if (c=='>') u8_puts(out,"&#62;");
    else if (c=='&') u8_puts(out,"&#38;");
    else u8_putc(out,c);
} 

FD_INLINE_FCN void entify_lower(u8_output out,u8_string value)
{
  u8_byte *scan=value; int c;
  while ((c=u8_sgetc(&scan))>=0)
    if (c=='<') u8_puts(out,"&#60;");
    else if (c=='>') u8_puts(out,"&#62;");
    else if (c=='&') u8_puts(out,"&#38;");
    else u8_putc(out,u8_tolower(c));
} 

FD_EXPORT
void fd_entify(u8_output out,u8_string value)
{
  entify(out,value);
}

FD_EXPORT
void fd_attrib_entify(u8_output out,u8_string value)
{
  attrib_entify(out,value);
}

static void emit_xmlattrib
  (u8_output out,u8_output tmp,u8_string name,fdtype value)
{
  int c; u8_byte *scan=name;
  while ((c=u8_sgetc(&scan))>0) u8_putc(out,u8_tolower(c));
  u8_puts(out,"='");
  if (FD_STRINGP(value))
    attrib_entify(out,FD_STRDATA(value));
  else if (FD_SYMBOLP(value)) {
    u8_putc(out,':');
    attrib_entify(out,FD_SYMBOL_NAME(value));}
  else if (FD_OIDP(value))
    u8_printf(out,":@%x/%x",
	      FD_OID_HI(FD_OID_ADDR(value)),
	      FD_OID_LO(FD_OID_ADDR(value)));
  else if (FD_FIXNUMP(value))
    u8_printf(out,"%d",FD_FIX2INT(value));
  else if (FD_FLONUMP(value))
    u8_printf(out,"%f",FD_FLONUM(value));
  else if (tmp) {
    tmp->u8_outptr=tmp->u8_outbuf;
    tmp->u8_streaminfo=tmp->u8_streaminfo|U8_STREAM_TACITURN;
    fd_unparse(tmp,value);
    tmp->u8_streaminfo=tmp->u8_streaminfo&(~U8_STREAM_TACITURN);
    u8_puts(out,":");
    attrib_entify(out,tmp->u8_outbuf);}
  else {
    U8_OUTPUT tmp; u8_byte buf[128];
    U8_INIT_OUTPUT_BUF(&tmp,128,buf);
    tmp.u8_streaminfo=tmp.u8_streaminfo|U8_STREAM_TACITURN;
    fd_unparse(&tmp,value);
    u8_puts(out,":");
    attrib_entify(out,tmp.u8_outbuf);
    if (tmp.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(tmp.u8_outbuf);}
  u8_puts(out,"'");
}

static fdtype xmlify(fdtype value)
{
  if (FD_STRINGP(value)) return value;
  else if (FD_OIDP(value)) {
    U8_OUTPUT tmp; U8_INIT_OUTPUT(&tmp,32);
    u8_printf(&tmp,":%40%x%2f%x",
	      FD_OID_HI(FD_OID_ADDR(value)),
	      FD_OID_LO(FD_OID_ADDR(value)));
    return fd_init_string(NULL,tmp.u8_outptr-tmp.u8_outbuf,tmp.u8_outbuf);}
  else {
    U8_OUTPUT tmp; U8_INIT_OUTPUT(&tmp,32);
    tmp.u8_streaminfo=tmp.u8_streaminfo|U8_STREAM_TACITURN;
    u8_putc(&tmp,':'); fd_unparse(&tmp,value); 
    return fd_init_string(NULL,tmp.u8_outptr-tmp.u8_outbuf,tmp.u8_outbuf);}
}

static fdtype oid2id(fdtype oid,fdtype prefix)
{
  U8_OUTPUT tmp; U8_INIT_OUTPUT(&tmp,32);
  if (FD_VOIDP(prefix)) 
    u8_printf(&tmp,":@%x/%x",
	      FD_OID_HI(FD_OID_ADDR(oid)),
	      FD_OID_LO(FD_OID_ADDR(oid)));
  else if (FD_SYMBOLP(prefix))
    u8_printf(&tmp,"%s_%x_%x",
	      FD_SYMBOL_NAME(prefix),
	      FD_OID_HI(FD_OID_ADDR(oid)),
	      FD_OID_LO(FD_OID_ADDR(oid)));
  else if (FD_STRINGP(prefix))
    u8_printf(&tmp,"%s_%x_%x",
	      FD_STRDATA(prefix),
	      FD_OID_HI(FD_OID_ADDR(oid)),
	      FD_OID_LO(FD_OID_ADDR(oid)));
  else {
    u8_free(tmp.u8_outbuf);
    return fd_type_error("string","oid2id",prefix);}
  return fd_init_string(NULL,tmp.u8_outptr-tmp.u8_outbuf,tmp.u8_outbuf);
}

FD_INLINE_FCN fdtype oidunxmlify(fdtype string)
{
  u8_string s=FD_STRDATA(string), addr_start=strchr(s,'_');
  FD_OID addr; unsigned int hi, lo;
  if (addr_start) sscanf(addr_start,"_%x_%x",&hi,&lo);
  else return FD_EMPTY_CHOICE;
  FD_SET_OID_HI(addr,hi); FD_SET_OID_LO(addr,lo);
  return fd_make_oid(addr);
}

static void emit_xmlcontent(u8_output out,u8_string content)
{
  entify(out,content);
}

static int open_markup(u8_output out,u8_output tmp,u8_string eltname,
		       fdtype attribs,fd_lispenv env,int empty)
{
  u8_putc(out,'<');
  emit_xmlname(out,eltname);
  while (FD_PAIRP(attribs)) {
    fdtype elt=FD_CAR(attribs);
    /* Kludge to handle case where the attribute name is quoted. */
    if ((FD_PAIRP(elt)) && (FD_CAR(elt)==quote_symbol) &&
	(FD_PAIRP(FD_CDR(elt))) && (FD_SYMBOLP(FD_CADR(elt))))
      elt=FD_CADR(elt);
    u8_putc(out,' ');
    if (FD_STRINGP(elt)) {
      attrib_entify(out,FD_STRDATA(elt));
      attribs=FD_CDR(attribs);}
    else if (FD_SYMBOLP(elt))
      if (FD_PAIRP(FD_CDR(attribs))) {
	if (env) {
	  fdtype val=fd_eval((FD_CADR(attribs)),env);
	  if (FD_ABORTP(val)) {
	    if (empty) u8_puts(out,"/>"); else u8_puts(out,">");
	    return fd_interr(val);}
	  else if (FD_VOIDP(val)) {}
	  else emit_xmlattrib(out,tmp,FD_SYMBOL_NAME(elt),val);
	  fd_decref(val);}
	else emit_xmlattrib(out,tmp,FD_SYMBOL_NAME(elt),FD_CADR(attribs));
	attribs=FD_CDR(FD_CDR(attribs));}
      else {
	if (empty) u8_puts(out,"/>"); else u8_puts(out,">");
	return fd_reterr(fd_SyntaxError,"open_markup",NULL,fd_incref(attribs));}
    else if ((FD_PAIRP(elt)) && (FD_PAIRP(FD_CDR(elt)))) {
      fdtype attrib_name=FD_CAR(elt);
      fdtype attrib_expr=FD_CAR(FD_CDR(elt));
      if ((FD_SYMBOLP(attrib_name)) || (FD_STRINGP(attrib_name))) {}
      else if (FD_PAIRP(attrib_name)) {
	fdtype name=fd_eval(attrib_name,env);
	if (FD_ABORTP(name)) {
	  if (empty) u8_puts(out,"/>"); else u8_puts(out,">");
	  return fd_interr(name);}
	else if (FD_SYMBOLP(name)) attrib_name=name;
	else if (FD_STRINGP(name))
	  attrib_name=fd_intern(FD_STRDATA(attrib_name));
	else {
	  fd_decref(name);
	  if (empty) u8_puts(out,"/>"); else u8_puts(out,">");
	  return fd_reterr(fd_TypeError,"open_markup",
			   u8_strdup("attribname"),fd_incref(name));}
	fd_decref(name);}
      else {
	if (empty) u8_puts(out,"/>"); else u8_puts(out,">");
	return fd_reterr(fd_SyntaxError,"open_markup",
			 NULL,fd_incref(attribs));}
      if (FD_NEED_EVALP(attrib_expr)) {
	fdtype val=((env) ? (fd_eval(attrib_expr,env)) :
		    (fd_incref(attrib_expr)));
	if (FD_VOIDP(val)) {}
	else if (FD_ABORTP(val)) {
	  if (empty) u8_puts(out,"/>"); else u8_puts(out,">");
	  return fd_interr(val);}
	else if (FD_STRINGP(attrib_name))
	  if (FD_FALSEP(val)) {}
	  else attrib_entify(out,FD_STRDATA(attrib_name));
	else emit_xmlattrib(out,tmp,FD_SYMBOL_NAME(attrib_name),val);
	fd_decref(val);}
      else if (FD_FALSEP(attrib_expr)) {}
      else if (FD_SYMBOLP(attrib_name))
	emit_xmlattrib(out,tmp,FD_SYMBOL_NAME(attrib_name),attrib_expr);
      else if (FD_STRINGP(attrib_name))
	emit_xmlattrib(out,tmp,FD_STRING_DATA(attrib_name),attrib_expr);
      attribs=FD_CDR(attribs);}
    else {
	fd_seterr(fd_SyntaxError,"open_markup",NULL,fd_incref(attribs));
	return -1;}}
  if (empty) u8_puts(out,"/>"); else u8_puts(out,">");
  return 1;
}

static int close_markup(u8_output out,u8_string tagname)
{
  u8_puts(out,"</");
  emit_xmlname(out,tagname);
  u8_puts(out,">");
  return 1;
}

FD_EXPORT int fd_open_markup
  (u8_output out,u8_string eltname,fdtype attribs,int empty)
{
  return open_markup(out,NULL,eltname,attribs,NULL,empty);
}

static u8_string get_tagname(fdtype tag,u8_byte *buf,int len)
{
  U8_OUTPUT out; U8_INIT_OUTPUT_BUF(&out,len,buf);
  if (FD_SYMBOLP(tag)) {
    u8_byte *scan=FD_SYMBOL_NAME(tag); int c;
    while ((c=u8_sgetc(&scan))>=0)
      if ((c=='*') && (*scan=='\0')) break;
      else if (u8_isupper(c))
	u8_putc(&out,u8_tolower(c));
      else u8_putc(&out,c);}
  else if (FD_STRINGP(tag)) {
    u8_puts(&out,FD_STRDATA(tag));}
  else return NULL;
  return out.u8_outbuf;
}

/* XMLOUTPUT primitives */

static int xmlout_helper(U8_OUTPUT *out,U8_OUTPUT *tmp,fdtype x,
			 fdtype xmloidfn,fd_lispenv env)
{
  if (FD_ABORTP(x)) return 0;
  else if (FD_VOIDP(x)) return 1;
  if (FD_STRINGP(x))
    emit_xmlcontent(out,FD_STRDATA(x));
  else if ((FD_APPLICABLEP(xmloidfn)) && (FD_OIDP(x))) {
    fdtype result=fd_apply(xmloidfn,1,&x);
    fd_decref(result);}
  else if (FD_OIDP(x)) 
    fd_xmloid(out,x);
  else if ((FD_SLOTMAPP(x)) &&
	   (fd_test(x,raw_name_symbol,FD_VOID)))
    fd_xmleval(out,x,env);
  else {
    U8_OUTPUT _out; u8_byte buf[128];
    if (tmp==NULL) {
      U8_INIT_OUTPUT_BUF(&_out,64,buf); tmp=&_out;}
    tmp->u8_outptr=tmp->u8_outbuf;
    fd_unparse(tmp,x);
    /* if (FD_OIDP(x)) output_oid(tmp,x); else {} */
    emit_xmlcontent(out,tmp->u8_outbuf);}
  return 1;
}

static fdtype xmlout(fdtype expr,fd_lispenv env)
{
  fdtype body=fd_get_body(expr,1);
  U8_OUTPUT *out=fd_get_default_output(), tmpout;
  fdtype xmloidfn=fd_symeval(xmloidfn_symbol,env);
  u8_byte buf[128];
  U8_INIT_OUTPUT_BUF(&tmpout,128,buf);
  while (FD_PAIRP(body)) {
    fdtype value=fasteval(FD_CAR(body),env);
    if (FD_ABORTP(value)) {
      fd_decref(xmloidfn);
      return value;}
    else if (xmlout_helper(out,&tmpout,value,xmloidfn,env))
      fd_decref(value);
    else return value;
    body=FD_CDR(body);}
  u8_flush(out);
  if (tmpout.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(tmpout.u8_outbuf);
  fd_decref(xmloidfn);
  return FD_VOID;
}

FD_EXPORT int fd_dtype2xml(u8_output out,fdtype x,fd_lispenv env)
{
  int retval=-1;
  fdtype xmloidfn=fd_symeval(xmloidfn_symbol,env);
  if (out==NULL) out=fd_get_default_output();
  retval=xmlout_helper(out,NULL,x,xmloidfn,env);
  fd_decref(xmloidfn);
  return retval;
}

static fdtype raw_xhtml_handler(fdtype expr,fd_lispenv env)
{
  fdtype body=fd_get_body(expr,1);
  U8_OUTPUT *out=fd_get_default_output(), tmpout;
  fdtype xmloidfn=fd_symeval(xmloidfn_symbol,env);
  u8_byte buf[128];
  U8_INIT_OUTPUT_BUF(&tmpout,128,buf);
  while (FD_PAIRP(body)) {
    fdtype value=fasteval(FD_CAR(body),env);
    if (FD_ABORTP(value)) {
      fd_decref(xmloidfn);
      return value;}
    else if (FD_STRINGP(value))
      u8_putn(out,FD_STRDATA(value),FD_STRLEN(value));
    else u8_printf(out,"%q",value);
    fd_decref(value);
    body=FD_CDR(body);}
  u8_flush(out);
  if (tmpout.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(tmpout.u8_outbuf);
  fd_decref(xmloidfn);
  return FD_VOID;
}

static fdtype nbsp_prim()
{
  U8_OUTPUT *out=fd_get_default_output();
  u8_puts(out,"&nbsp;");
  return FD_VOID;
}

static fdtype xmlemptyelt(int n,fdtype *args)
{
  fdtype eltname=args[0];
  u8_byte tagbuf[128], *tagname; int i=1;
  U8_OUTPUT *out=fd_get_default_output();
  tagname=get_tagname(eltname,tagbuf,128);
  if (tagname) {
    u8_putc(out,'<');
    emit_xmlname(out,tagname);
    if (tagname!=tagbuf) u8_free(tagname);}
  else return fd_err(fd_TypeError,"xmlemptyelt",_("invalid XML element name"),eltname);
  while (i<n) {
    fdtype elt=args[i];
    u8_putc(out,' ');
    if (FD_STRINGP(elt)) {
      entify(out,FD_STRDATA(elt)); i++;}
    else if (FD_SYMBOLP(elt))
      if (i+1<n) {
	fdtype val=args[i+1];
	if (!(FD_EMPTY_CHOICEP(val)))
	  emit_xmlattrib(out,NULL,FD_SYMBOL_NAME(elt),val);
	i=i+2;}
      else return fd_err(fd_SyntaxError,"xmlemptyelt",_("odd number of arguments"),elt);
    else return fd_err(fd_SyntaxError,"xmlemptyelt",_("invalid XML attribute name"),elt);}
  u8_puts(out,"/>");
  return FD_VOID;
}

static fdtype xmlentry(fdtype expr,fd_lispenv env)
{
  U8_OUTPUT *out=fd_get_default_output();
  fdtype head=fd_get_arg(expr,1), args=FD_CDR(FD_CDR(expr));
  u8_byte tagbuf[128], *tagname;
  if ((FD_PAIRP(head)))  head=fd_eval(head,env);
  else head=fd_incref(head);
  tagname=get_tagname(head,tagbuf,128);
  if (tagname==NULL) {
    fd_decref(head);
    return fd_err(fd_SyntaxError,"xmlentry",NULL,expr);}
  else if (open_markup(out,NULL,tagname,args,env,1)<0) {
    fd_decref(head);
    u8_flush(out);
    return FD_ERROR_VALUE;}
  else {
    fd_decref(head);
    u8_flush(out);
    return FD_VOID;}
}

static fdtype doxmlblock(fdtype expr,fd_lispenv env,int newline)
{
  fdtype tagspec=fd_get_arg(expr,1), attribs, body;
  fdtype xmloidfn=fd_symeval(xmloidfn_symbol,env);
  u8_byte tagbuf[128], buf[128], *tagname; int eval_attribs=0;
  U8_OUTPUT *out, tmpout;
  if (FD_SYMBOLP(tagspec)) {
    attribs=fd_get_arg(expr,2); body=fd_get_body(expr,3);
    eval_attribs=1;}
  else if (FD_STRINGP(tagspec)) {
    attribs=fd_get_arg(expr,2); body=fd_get_body(expr,3);
    fd_incref(tagspec); eval_attribs=1;}
  else {
    body=fd_get_body(expr,2);
    tagspec=fd_eval(tagspec,env);
    if (FD_ABORTP(tagspec)) {
      fd_decref(xmloidfn);
      return tagspec;}
    else if (FD_SYMBOLP(tagspec)) attribs=FD_EMPTY_LIST;
    else if (FD_STRINGP(tagspec)) attribs=FD_EMPTY_LIST;
    else if (FD_PAIRP(tagspec)) {
      fdtype name=FD_CAR(tagspec); attribs=fd_incref(FD_CDR(tagspec));
      if (FD_SYMBOLP(name)) {}
      else if (FD_STRINGP(name)) fd_incref(name);
      else {
	fd_decref(xmloidfn);
	return fd_err(fd_SyntaxError,"xmlblock",NULL,tagspec);}
      fd_decref(tagspec); tagspec=name;}
    else {
      fd_decref(xmloidfn);
      return fd_err(fd_SyntaxError,"xmlblock",NULL,tagspec);}}
  tagname=get_tagname(tagspec,tagbuf,128);
  if (tagname==NULL) {
    fd_decref(xmloidfn);
    return fd_err(fd_SyntaxError,"xmlblock",NULL,expr);}
  out=fd_get_default_output();
  U8_INIT_OUTPUT_BUF(&tmpout,128,buf);
  if (open_markup(out,&tmpout,tagname,attribs,env,0)<0) {
    fd_decref(xmloidfn);
    return FD_ERROR_VALUE;}
  if (newline) u8_putc(out,'\n');
  while (FD_PAIRP(body)) {
    fdtype value=fasteval(FD_CAR(body),env);
    if (FD_ABORTP(value)) {
      fd_decref(xmloidfn);
      close_markup(out,tagname);
      return value;}
    else if (xmlout_helper(out,&tmpout,value,xmloidfn,env))
      fd_decref(value);
    else {
      fd_decref(xmloidfn);
      return value;}
    body=FD_CDR(body);}
  if (newline) u8_putc(out,'\n');
  if (close_markup(out,tagname)<0) {
    fd_decref(xmloidfn);
    return FD_ERROR_VALUE;}
  if (tagname!=tagbuf) u8_free(tagname);
  u8_flush(out);
  fd_decref(xmloidfn);
  if (tmpout.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(tmpout.u8_outbuf);
  return FD_VOID;
}

static fdtype xmlblock(fdtype expr,fd_lispenv env)
{
  return doxmlblock(expr,env,0);
}
static fdtype xmlblockn(fdtype expr,fd_lispenv env)
{
  return doxmlblock(expr,env,1);
}

static fdtype handle_markup(fdtype expr,fd_lispenv env,int star,int block)
{
  if ((FD_PAIRP(expr)) && (FD_SYMBOLP(FD_CAR(expr)))) {
    fdtype attribs=fd_get_arg(expr,1), body=fd_get_body(expr,2);
    fdtype xmloidfn=fd_symeval(xmloidfn_symbol,env);
    U8_OUTPUT *out=fd_get_default_output(), tmpout;
    u8_byte *tagname, tagbuf[128], buf[128];
    U8_INIT_OUTPUT_BUF(&tmpout,128,buf);
    if (star) {
      attribs=fd_get_arg(expr,1); body=fd_get_body(expr,2);}
    else {attribs=FD_EMPTY_LIST; body=fd_get_body(expr,1);}
    tagname=get_tagname(FD_CAR(expr),tagbuf,128);
    if (tagname==NULL) {
      fd_decref(xmloidfn);
      return fd_err(fd_SyntaxError,"handle_markup",NULL,expr);}
    if (block) u8_printf(out,"\n");
    if (open_markup(out,&tmpout,tagname,attribs,env,0)<0) {
      fd_decref(xmloidfn);
      return FD_ERROR_VALUE;}
    if (block) u8_printf(out,"\n");
    while (FD_PAIRP(body)) {
      fdtype value=fasteval(FD_CAR(body),env);
      if (FD_ABORTP(value)) {
	close_markup(out,tagname);
	if (block) u8_printf(out,"\n");
	fd_decref(xmloidfn);
	return value;}
      else if (xmlout_helper(out,&tmpout,value,xmloidfn,env))
	fd_decref(value);
      else {
	fd_decref(xmloidfn);
	return value;}
      body=FD_CDR(body);}
    if (block) u8_printf(out,"\n");
    if (close_markup(out,tagname)<0) {
      fd_decref(xmloidfn);
      return FD_ERROR_VALUE;}
    if (block) u8_printf(out,"\n");
    if (tagname!=tagbuf) u8_free(tagname);
    u8_flush(out);
    if (tmpout.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(tmpout.u8_outbuf);
    fd_decref(xmloidfn);
    return FD_VOID;}
  else return fd_err(fd_SyntaxError,"XML markup",NULL,fd_incref(expr));
}

static fdtype markup_handler(fdtype expr,fd_lispenv env)
{
  return handle_markup(expr,env,0,0);
}

static fdtype markupblock_handler(fdtype expr,fd_lispenv env)
{
  return handle_markup(expr,env,0,1);
}

static fdtype markupstarblock_handler(fdtype expr,fd_lispenv env)
{
  return handle_markup(expr,env,1,1);
}

static fdtype markupstar_handler(fdtype expr,fd_lispenv env)
{
  return handle_markup(expr,env,1,0);
}

static fdtype emptymarkup_handler(fdtype expr,fd_lispenv env)
{
  U8_OUTPUT *out=fd_get_default_output();
  fdtype head=FD_CAR(expr), args=FD_CDR(expr);
  u8_byte tagbuf[128], *tagname=get_tagname(head,tagbuf,128);
  if (tagname==NULL)
    return fd_err(fd_SyntaxError,"emptymarkup_handler",NULL,expr);
  else if (open_markup(out,NULL,tagname,args,env,1)<0)
    return FD_ERROR_VALUE;
  else {
    u8_flush(out);
    return FD_VOID;}
}

/* XHTML error report */

#define DEFAULT_DOCTYPE \
  "<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.1//EN\"\
               \"http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd\">"
#define DEFAULT_XMLPI \
  "<?xml version='1.0' charset='utf-8' ?>"

static u8_string error_stylesheet=NULL;

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

static fdtype get_focus_expr(u8_exception ex)
{
  u8_exception scan=ex; fdtype xdata, focus=FD_VOID;
  if (ex==NULL) return FD_VOID;
  else {
    xdata=exception_data(scan);
    scan=ex->u8x_prev;}
  if (FD_PAIRP(xdata)) {
    while (scan) {
      fdtype sdata=exception_data(scan);
      if (FD_PAIRP(sdata))
	if (embeddedp(sdata,xdata)) {
	  focus=sdata; scan=scan->u8x_prev;}
	else return focus;
      else if (FD_TABLEP(sdata)) {
	scan=scan->u8x_prev;}
      else break;}
    return focus;}
  else return FD_VOID;
}

static u8_exception get_next_frame(u8_exception ex)
{
  u8_exception scan=ex; fdtype xdata;
  if (ex==NULL) return NULL;
  else {
    xdata=exception_data(scan);
    scan=ex->u8x_prev;}
  if (FD_PAIRP(xdata)) {
    while (scan) {
      fdtype sdata=exception_data(scan);
      if (FD_PAIRP(sdata))
	if (embeddedp(sdata,xdata)) {
	  scan=scan->u8x_prev;}
	else return scan;
      else return scan;}
    return scan;}
  else return scan;
}

static u8_exception output_backtrace_entry(u8_output s,u8_exception ex)
{
  if (ex==NULL) return NULL;
  else if (ex->u8x_prev==NULL)
    u8_printf(s,"<div class='last_entry'>\n");
  if (ex->u8x_context==fd_eval_context) {
    fdtype expr=exception_data(ex);
    struct U8_OUTPUT tmp; u8_string focus_start;
    fdtype focus=get_focus_expr(ex);
    U8_INIT_OUTPUT(&tmp,1024);
    u8_printf(s,"<div class='expr'>");
    fd_pprint_focus(&tmp,expr,focus,NULL,0,80,"#@?#","#@?#");
    if ((focus_start=(strstr(tmp.u8_outbuf,"#@?#")))) {
      u8_byte *focus_end=strstr(focus_start+4,"#@?#");
      *focus_start='\0'; fd_entify(s,tmp.u8_outbuf);
      *focus_end='\0'; u8_printf(s,"<span class='focus'>");
      fd_entify(s,focus_start+4);
      u8_printf(s,"</span>");
      fd_entify(s,focus_end+4);}
    else u8_puts(s,tmp.u8_outbuf);
    u8_free(tmp.u8_outbuf);
    u8_printf(s,"</div>\n");}
  else if (ex->u8x_context==fd_apply_context) {
    fdtype entry=exception_data(ex);
    int i=1, len=FD_VECTOR_LENGTH(entry);
    u8_printf(s,"<div class='rail'>(<span class='name'>%lk</span>",FD_VECTOR_REF(entry,0));
    /* u8_printf(s,"<span class='head'>CALL</span>"); */
    while (i<len) {
      fdtype elt=FD_VECTOR_REF(entry,i); i++;
      if ((FD_PAIRP(elt)) || (FD_SYMBOLP(elt)))
	u8_printf(s,"<span class='arg'>&apos;%lk</span>",elt);
      else u8_printf(s,"<span class='arg'>%lk</span>",elt);}
    u8_printf(s,")</div>\n");}
  else if ((ex->u8x_context) && (ex->u8x_context[0]==':')) {
    fdtype entry=exception_data(ex);
    fdtype keys=fd_getkeys(entry);
    u8_string head=((ex->u8x_details) ? ((u8_string)(ex->u8x_details)) :
		    (ex->u8x_context) ?  ((u8_string)(ex->u8x_context)) :
		    ((u8_string)"BINDINGS"));
    u8_printf(s,"<div class='bindings'>");
    u8_printf(s,"<span class='head'>%s</span>",head);
    /* u8_printf(s,"<span class='head'>%lk</span>",ex->u8x_context); */
    if (FD_ABORTP(keys)) {
      fd_decref(keys);
      u8_printf(s,"%lk</div>\n",entry);}
    else {
      FD_DO_CHOICES(key,keys) {
	fdtype val=fd_get(entry,key,FD_VOID);
	u8_printf(s,"<span class='binding'>%lk=%lk;</span> ",key,val);
	fd_decref(val);}
      fd_decref(keys);
      u8_printf(s,"</div>\n");}}
  else {
    fdtype irritant=exception_data(ex);
    u8_printf(s,"<div class='error'>\n");
    /* u8_printf(s,"<span class='head'>ERR</span>"); */
    if (ex->u8x_context) u8_printf(s,"In <span class='cxt'>%k</span>, ",ex->u8x_context);
    u8_printf(s," <span class='ex'>%k</span>",ex->u8x_cond);
    if (ex->u8x_details) u8_printf(s," <span class='details'>(%k)</span>",ex->u8x_details);
    if (!(FD_VOIDP(irritant))) 
      u8_printf(s,": <span class='irritant'>%lk</span>",irritant);
    u8_printf(s,"</div>\n");}
  if (ex->u8x_prev==NULL) u8_printf(s,"</div>\n");
  return get_next_frame(ex);
}

static int output_backtrace_entries(u8_output s,u8_exception ex)
{
  int depth=0;
  if (ex==NULL) return 0;
  depth=output_backtrace_entries(s,get_next_frame(ex));
  if (depth<backtrace_indent_depth)
    u8_printf(s,"<div class='backtrace_indent'/>\n");
  output_backtrace_entry(s,ex);
  if (depth<backtrace_indent_depth) return depth+1; else return depth;
}

static void output_backtrace(u8_output s,u8_exception ex)
{
  int i=0, depth;
  u8_printf(s,"<div class='backtrace'>\n");
  output_backtrace_entry(s,ex);
  if (ex->u8x_prev) output_backtrace_entry(s,ex->u8x_prev);
  depth=output_backtrace_entries(s,ex);
  while (i<depth) {u8_printf(s,"</div>\n"); i++;}
  u8_printf(s,"</div>\n");
}

#if 0
static void output_backtrace(u8_output s,u8_exception ex)
{
  u8_exception scan=ex; int n=0;
  u8_printf(s,"<div class='backtrace'>\n");
  while ((scan) && (n<BACKTRACE_INDENT_DEPTH)) {
    u8_printf(s,"<div class='backtrace_indent'>\n");
    scan=output_backtrace_entry(s,scan); n++;}
  while (scan) scan=output_backtrace_entry(s,scan);
  while (n>0) {u8_printf(s,"</div>\n"); n--;}
  u8_printf(s,"</div>\n");
}
#endif

FD_EXPORT
void fd_xhtmlerrorpage(u8_output s,u8_exception ex)
{
  u8_exception e=u8_exception_root(ex);
  fdtype irritant=fd_exception_xdata(e);
  s->u8_outptr=s->u8_outbuf;
  u8_printf(s,"%s\n%s\n",DEFAULT_DOCTYPE,DEFAULT_XMLPI);
  u8_printf(s,"<html>\n<head>\n<title>");
  if (e->u8x_cond)
    u8_printf(s,"%k",e->u8x_cond);
  else u8_printf(s,"Unknown Exception");
  if (e->u8x_context) u8_printf(s," @%k",e->u8x_context);
  if (!(FD_VOIDP(irritant))) u8_printf(s," %lk",irritant);
  if (e->u8x_details) u8_printf(s," (%k)",e->u8x_details);
  u8_printf(s,"</title>\n");
  u8_printf(s,"<link rel='stylesheet' type='text/css' href='%s'/>\n",
	    error_stylesheet);
  u8_printf(s,"</head>\n<body id='ERRORPAGE'>\n<div class='server_sorry'>");
  u8_printf(s,"There was an unexpected error processing your request\n");
  u8_printf(s,"<div class='error'>\n");
  if (e->u8x_context) u8_printf(s,"In <span class='cxt'>%k</span>, ",e->u8x_context);
  u8_printf(s," <span class='ex'>%k</span>",e->u8x_cond);
  if (e->u8x_details) u8_printf(s," <span class='details'>(%k)</span>",e->u8x_details);
  if (!(FD_VOIDP(irritant))) 
    u8_printf(s,": <span class='irritant'>%lk</span>",irritant);
  u8_printf(s,"</div></div>\n");
  output_backtrace(s,ex);
  u8_printf(s,"</body>\n</html>\n");  
}

/* Getting oid display data */

static fdtype global_browseinfo=FD_EMPTY_CHOICE;
static u8_string default_browse_uri=NULL;
static u8_string default_browse_class=NULL;

#if FD_THREADS_ENABLED
static u8_mutex browseinfo_lock;
#endif

static fdtype get_browseinfo(fdtype arg)
{
  fd_pool p=fd_oid2pool(arg);
  fdtype pool=fd_pool2lisp(p), browseinfo=fd_thread_get(browseinfo_symbol), dflt=FD_VOID;
  FD_DO_CHOICES(info,browseinfo) {
    if ((FD_VECTORP(info)) && (FD_VECTOR_LENGTH(info)>0))
      if (FD_EQ(FD_VECTOR_REF(info,0),pool)) {
	fd_incref(info); fd_decref(browseinfo);
	return info;}
      else if (FD_TRUEP(FD_VECTOR_REF(info,0)))
	dflt=info;
      else {}
    else dflt=info;}
  if (FD_VOIDP(dflt)) {
    u8_lock_mutex(&browseinfo_lock);
    {FD_DO_CHOICES(info,global_browseinfo) {
	if ((FD_VECTORP(info)) && (FD_VECTOR_LENGTH(info)>0))
	  if (FD_EQ(FD_VECTOR_REF(info,0),pool)) {
	    fd_incref(info);
	    u8_unlock_mutex(&browseinfo_lock);
	    return info;}
	  else if (FD_TRUEP(FD_VECTOR_REF(info,0)))
	    dflt=info;}
      fd_incref(dflt);
      u8_unlock_mutex(&browseinfo_lock);
      if (FD_VOIDP(dflt)) return FD_EMPTY_CHOICE;
      else return dflt;}}
  else {
    fd_incref(dflt); fd_decref(browseinfo);
    return dflt;}
}

static int unpack_browseinfo(fdtype info,u8_string *baseuri,u8_string *classname,fdtype *displayer)
{
  if ((FD_EMPTY_CHOICEP(info)) || (FD_VOIDP(info))) {
    if (*baseuri==NULL)
      if (default_browse_uri)
	*baseuri=default_browse_uri;
      else *baseuri="browse.fdcgi?";
    if (*classname==NULL)
      if (default_browse_class)
	*classname=default_browse_class;
      else *classname="oid";}
  else if (FD_STRINGP(info)) {
    *baseuri=FD_STRDATA(info);
    if (*classname==NULL)
      if (default_browse_class)
	*classname=default_browse_class;
      else *classname="oid";
    if (displayer) *displayer=FD_VOID;}
  else if ((FD_VECTORP(info)) && (FD_VECTOR_LENGTH(info)>1)) {
    if (*classname==NULL)
      *classname=((default_browse_class) ? (default_browse_class) : ((u8_string)"oid"));
    if (*baseuri==NULL)
      *baseuri=((default_browse_uri) ? (default_browse_uri) : ((u8_string)"browse.fdcgi?"));
    switch (FD_VECTOR_LENGTH(info)) {
    case 2:
      if (FD_STRINGP(FD_VECTOR_REF(info,1)))
	*baseuri=FD_STRDATA(FD_VECTOR_REF(info,1));
      else u8_log(LOG_WARN,fd_TypeError,"Bad browse info %q",info);
    case 3:
      if (FD_STRINGP(FD_VECTOR_REF(info,2)))
	*classname=FD_STRDATA(FD_VECTOR_REF(info,2));
      else u8_log(LOG_WARN,fd_TypeError,"Bad browse info %q",info);
    case 4:
      if (displayer) *displayer=FD_VECTOR_REF(info,3);}}
  else {
    u8_log(LOG_WARN,fd_TypeError,"Bad browse info %q",info);
    *baseuri="browse.fdcgi?";}
  return 0;
}

static fdtype browseinfo_config_get(fdtype var,void *ignored)
{
  fdtype result;
  u8_lock_mutex(&browseinfo_lock);
  result=global_browseinfo; fd_incref(result);
  u8_unlock_mutex(&browseinfo_lock);
  return result;
}

static int browseinfo_config_set(fdtype var,fdtype val,void *ignored)
{
  fdtype new_browseinfo=FD_EMPTY_CHOICE, old_browseinfo;
  u8_lock_mutex(&browseinfo_lock);
  old_browseinfo=global_browseinfo;
  if ((FD_STRINGP(val)) || ((FD_VECTORP(val)) && (FD_VECTOR_LENGTH(val)>1))) {
    fdtype target=FD_VECTOR_REF(val,0);
    FD_DO_CHOICES(info,old_browseinfo) {
      if ((FD_VECTORP(info)) && (FD_EQ(target,FD_VECTOR_REF(info,0)))) {}
      else if ((FD_STRINGP(info)) && (FD_TRUEP(target))) {}
      else {
	fd_incref(info); FD_ADD_TO_CHOICE(new_browseinfo,info);}}
    fd_incref(val); FD_ADD_TO_CHOICE(new_browseinfo,val);
    global_browseinfo=fd_simplify_choice(new_browseinfo);
    u8_unlock_mutex(&browseinfo_lock);
    return 1;}
  else {
    u8_unlock_mutex(&browseinfo_lock);
    fd_seterr(fd_TypeError,"browse info",NULL,val);
    return -1;}
}

/* Doing anchor output */

static fdtype doanchor(fdtype expr,fd_lispenv env)
{
  U8_OUTPUT *out=fd_get_default_output(), tmpout;
  fdtype target=fd_eval(fd_get_arg(expr,1),env), xmloidfn;
  fdtype body=fd_get_body(expr,2);
  u8_byte buf[128]; U8_INIT_OUTPUT_BUF(&tmpout,128,buf);
  if (FD_VOIDP(target))
    return fd_err(fd_SyntaxError,"doanchor",NULL,FD_VOID);
  else if (FD_EMPTY_LISTP(body))
    return fd_err(fd_SyntaxError,"doanchor",NULL,FD_VOID);
  if (FD_STRINGP(target)) {
    u8_printf(out,"<a href='");
    attrib_entify(out,FD_STRDATA(target));
    u8_puts(out,"'>");}
  else if (FD_SYMBOLP(target)) {
    u8_printf(out,"<a href='#");
    attrib_entify(out,FD_SYMBOL_NAME(target));
    u8_printf(out,"'>");}
  else if (FD_OIDP(target)) {
    FD_OID addr=FD_OID_ADDR(target);
    fdtype browseinfo=get_browseinfo(target);
    u8_string uri=NULL, class=NULL;
    unpack_browseinfo(browseinfo,&uri,&class,NULL);
    u8_printf(out,"<a href='%s:@%x/%x' class='%s'>",
	      uri,FD_OID_HI(addr),FD_OID_LO(addr),class);
    fd_decref(browseinfo);}
  else {
    return fd_type_error(_("valid anchor target"),"doanchor",target);}
  xmloidfn=fd_symeval(xmloidfn_symbol,env);
  while (FD_PAIRP(body)) {
    fdtype value=fasteval(FD_CAR(body),env);
    if (FD_ABORTP(value)) {
      fd_decref(xmloidfn); fd_decref(target);
      return value;}
    else if (xmlout_helper(out,&tmpout,value,xmloidfn,env))
      fd_decref(value);
    else {
      fd_decref(xmloidfn); fd_decref(target);
      return value;}
    body=FD_CDR(body);}
  u8_printf(out,"</a>");
  u8_flush(out);
  fd_decref(xmloidfn); fd_decref(target);
  if (tmpout.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(tmpout.u8_outbuf);
  return FD_VOID;
}

static int has_class_attrib(fdtype attribs)
{
  fdtype scan=attribs;
  while (FD_PAIRP(scan))
    if (FD_EQ(FD_CAR(scan),class_symbol)) return 1;
    else if ((FD_PAIRP(FD_CAR(scan))) &&
	     (FD_EQ(FD_CAR(FD_CAR(scan)),class_symbol)))
      return 1;
    else {
      scan=FD_CDR(scan);
      if (FD_PAIRP(scan)) scan=FD_CDR(scan);}
  return 0;
}

static fdtype doanchor_star(fdtype expr,fd_lispenv env)
{
  U8_OUTPUT *out=fd_get_default_output(), tmpout;
  fdtype target=fd_eval(fd_get_arg(expr,1),env), xmloidfn=FD_VOID;
  fdtype attribs=fd_get_arg(expr,2);
  fdtype body=fd_get_body(expr,3);
  u8_byte buf[128]; U8_INIT_OUTPUT_BUF(&tmpout,128,buf);
  if (FD_VOIDP(target)) 
    return fd_err(fd_SyntaxError,"doanchor",NULL,FD_VOID);
  else if (FD_EMPTY_LISTP(body))
    return fd_err(fd_SyntaxError,"doanchor",NULL,FD_VOID);
  if (FD_STRINGP(target)) 
    attribs=fd_init_pair
      (NULL,href_symbol,fd_init_pair(NULL,fd_incref(target),fd_incref(attribs)));
  else if (FD_SYMBOLP(target)) {
    tmpout.u8_outptr=tmpout.u8_outbuf;
    u8_printf(out,"#%s",FD_SYMBOL_NAME(target));
    attribs=fd_init_pair
      (NULL,href_symbol,
       fd_init_pair(NULL,fd_init_string
		    (NULL,tmpout.u8_outptr-tmpout.u8_outbuf,tmpout.u8_outbuf),
		    fd_incref(attribs)));}
  else if (FD_OIDP(target)) {
    FD_OID addr=FD_OID_ADDR(target);
    fdtype browseinfo=get_browseinfo(target);
    u8_string uri=NULL, class=NULL;
    unpack_browseinfo(browseinfo,&uri,&class,NULL);
    if (has_class_attrib(attribs))
      fd_incref(attribs);
    else attribs=fd_init_pair
	   (NULL,fd_intern("CLASS"),
	    fd_init_pair(NULL,fdtype_string(class),fd_incref(attribs)));
    tmpout.u8_outptr=tmpout.u8_outbuf;
    u8_printf(&tmpout,"%s:@%x/%x",uri,FD_OID_HI(addr),(FD_OID_LO(addr)));
    attribs=fd_init_pair
      (NULL,href_symbol,
       fd_init_pair(NULL,
		    fd_extract_string(NULL,tmpout.u8_outbuf,tmpout.u8_outptr),
		    attribs));
    fd_decref(browseinfo);}
  else return fd_type_error(_("valid anchor target"),"doanchor_star",target);
  xmloidfn=fd_symeval(xmloidfn_symbol,env);
  if (open_markup(out,&tmpout,"a",attribs,env,0)<0) {
    fd_decref(attribs); fd_decref(xmloidfn); fd_decref(target);
    return FD_ERROR_VALUE;}
  while (FD_PAIRP(body)) {
    fdtype value=fasteval(FD_CAR(body),env);
    if (FD_ABORTP(value)) {
      fd_decref(attribs); fd_decref(xmloidfn); fd_decref(target);
      return value;}
    else if (xmlout_helper(out,&tmpout,value,xmloidfn,env))
      fd_decref(value);
    else {
      fd_decref(attribs); fd_decref(xmloidfn); fd_decref(target);
      return value;}
    body=FD_CDR(body);}
  u8_printf(out,"</a>");
  u8_flush(out);
  fd_decref(attribs); fd_decref(xmloidfn); fd_decref(target);
  if (tmpout.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(tmpout.u8_outbuf);
  return FD_VOID;
}

FD_EXPORT void fd_xmloid(u8_output out,fdtype arg)
{
  FD_OID addr=FD_OID_ADDR(arg);
  fdtype browseinfo=get_browseinfo(arg), name, displayer=FD_VOID;
  u8_string uri=NULL, class=NULL;
  unpack_browseinfo(browseinfo,&uri,&class,&displayer);
  if (out==NULL) out=fd_get_default_output();
  u8_printf(out,"<a class='%s' href='%s?:@%x/%x'>",
	    class,uri,FD_OID_HI(addr),FD_OID_LO(addr));
  if ((FD_OIDP(displayer)) || (FD_SYMBOLP(displayer)))
    name=fd_frame_get(arg,displayer);
  else if (FD_APPLICABLEP(displayer))
    name=fd_apply(displayer,1,&arg);
  else name=fd_frame_get(arg,obj_name);
  if (FD_EMPTY_CHOICEP(name))
    u8_printf(out,"%q",arg);
  else if (FD_VOIDP(name)) {}
  else xmlout_helper(out,NULL,name,FD_VOID,NULL);
  fd_decref(name);
  u8_printf(out,"</a>");
  fd_decref(browseinfo);
}

static fdtype xmloid(fdtype oid_arg)
{
  fd_xmloid(NULL,oid_arg);
  return FD_VOID;
}

/* Scripturl primitive */

static fdtype scripturl(int n,fdtype *args)
{
  struct U8_OUTPUT out; 
  if (!(FD_STRINGP(args[0]))) 
    return fd_err(fd_TypeError,"scripturl",
		  u8_strdup("script name"),args[0]);
  else if ((n>2) && ((n%2)==0))
    return fd_err(fd_SyntaxError,"scripturl",
		  strd("odd number of arguments"),FD_VOID);
  else {U8_INIT_OUTPUT(&out,64);}
  if (n==1) {
    fd_uri_output(&out,FD_STRDATA(args[0]),"?#=&");}
  else if (n == 2) {
    fd_uri_output(&out,FD_STRDATA(args[0]),"?#=&");
    u8_putc(&out,'?');
    if (FD_STRINGP(args[1]))
      fd_uri_output(&out,FD_STRDATA(args[1]),"#&=;");
    else if (FD_OIDP(args[1])) {
      FD_OID addr=FD_OID_ADDR(args[1]);
      u8_printf(&out,":@%x/%x",FD_OID_HI(addr),FD_OID_LO(addr));}
    else {
      u8_string as_string=fd_dtype2string(args[1]);
      u8_putc(&out,':');
      fd_uri_output(&out,as_string,"#&=;");
      u8_free(as_string);}}
  else if (n%2) {
    int i=1, params=0;
    fd_uri_output(&out,FD_STRDATA(args[0]),"?#=&");
    u8_putc(&out,'?');
    while (i <n) {
      if (FD_EMPTY_CHOICEP(args[i+1])) {
	i=i+2; continue;}
      if (params>0) u8_putc(&out,'&');
      params++;
      if (FD_STRINGP(args[i]))
	fd_uri_output(&out,FD_STRDATA(args[i]),"?#=&");
      else if (FD_SYMBOLP(args[i]))
	fd_uri_output(&out,FD_SYMBOL_NAME(args[i]),"?#=&");
      else {
	u8_free(out.u8_outbuf);
	return fd_err(fd_SyntaxError,"scripturl",
		      u8_strdup(_("invalid parameter")),
		      fd_incref(args[i]));}
      u8_putc(&out,'=');
      if (FD_STRINGP(args[i+1]))
	fd_uri_output(&out,FD_STRDATA(args[i+1]),"?#=&");
      else if (FD_OIDP(args[i+1])) {
	FD_OID addr=FD_OID_ADDR(args[i+1]);
	u8_printf(&out,":@%x/%x",FD_OID_HI(addr),FD_OID_LO(addr));}
      else if (FD_CHOICEP(args[i+1])) {
	u8_string name=((FD_STRINGP(args[i])) ? (FD_STRDATA(args[i])) :
			(FD_SYMBOLP(args[i])) ? (FD_SYMBOL_NAME(args[i]))
			: ((u8_string)"NEVER"));
	int first_one=1;
	FD_DO_CHOICES(value,args[i+1])
	  if (FD_STRINGP(value)) {
	    if (first_one) {
	      fd_uri_output(&out,FD_STRING_DATA(value),"?#=&");
	      first_one=0;}
	    else {
	      u8_putc(&out,'&');
	      fd_uri_output(&out,name,"?#=&");
	      u8_putc(&out,'=');
	      fd_uri_output(&out,FD_STRING_DATA(value),"?#=&");}}
	  else {
	    u8_string as_string=fd_dtype2string(value);
	    if (first_one) {
	      fd_uri_output(&out,as_string,"?#=&"); first_one=0;}
	    else {
	      u8_putc(&out,'&');
	      fd_uri_output(&out,name,"?#=&");
	      u8_putc(&out,'=');
	      fd_uri_output(&out,as_string,"?#=&");}
	    u8_free(as_string);}}
      else {
	u8_string as_string=fd_dtype2string(args[i+1]);
	fd_uri_output(&out,as_string,"?#=&");
	u8_free(as_string);}
      i=i+2;}}
  else return fd_err(fd_SyntaxError,"scripturl",u8_strdup(FD_STRDATA(args[0])),FD_VOID);
  return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
}

static fdtype fdscripturl(int n,fdtype *args)
{
  struct U8_OUTPUT out; 
  if (!(FD_STRINGP(args[0]))) 
    return fd_err(fd_TypeError,"scripturl",
		  u8_strdup("script name"),args[0]);
  else if ((n>2) && ((n%2)==0))
    return fd_err(fd_SyntaxError,"scripturl",
		  strd("odd number of arguments"),FD_VOID);
  else {U8_INIT_OUTPUT(&out,64);}
  if (n == 2) {
    fd_uri_output(&out,FD_STRDATA(args[0]),"?#=&");
    u8_putc(&out,'?');
    if (FD_STRINGP(args[1]))
      fd_uri_output(&out,FD_STRDATA(args[1]),"#&=;");
    else if (FD_OIDP(args[1])) {
      FD_OID addr=FD_OID_ADDR(args[1]);
      u8_printf(&out,":@%x/%x",FD_OID_HI(addr),FD_OID_LO(addr));}
    else {
      u8_string as_string=fd_dtype2string(args[1]);
      u8_putc(&out,':');
      fd_uri_output(&out,as_string,"#&=;");
      u8_free(as_string);}}
  else if (n%2) {
    int i=1, params=0;
    fd_uri_output(&out,FD_STRDATA(args[0]),"?#=&");
    u8_putc(&out,'?');
    while (i <n) {
      if (params>0) u8_putc(&out,'&');
      params++;
      if (FD_STRINGP(args[i]))
	fd_uri_output(&out,FD_STRDATA(args[i]),"?#=&");
      else if (FD_SYMBOLP(args[i]))
	fd_uri_output(&out,FD_SYMBOL_NAME(args[i]),"?#=&");
      else {
	u8_free(out.u8_outbuf);
	return fd_err(fd_SyntaxError,"scripturl",
		      u8_strdup(_("invalid parameter")),
		      fd_incref(args[i]));}
      u8_putc(&out,'=');
      if (FD_STRINGP(args[i+1]))
	fd_uri_output(&out,FD_STRDATA(args[i+1]),"?#=&");
      else if (FD_OIDP(args[i+1])) {
	FD_OID addr=FD_OID_ADDR(args[i+1]);
	u8_printf(&out,":@%x/%x",FD_OID_HI(addr),FD_OID_LO(addr));}
      else {
	u8_string as_string=fd_dtype2string(args[i+1]);
	u8_putc(&out,':');
	fd_uri_output(&out,as_string,"?#=&");
	u8_free(as_string);}
      i=i+2;}}
  else return fd_err(fd_SyntaxError,"fdscripturl",u8_strdup(FD_STRDATA(args[0])),FD_VOID);
  return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
}

static fdtype scripturlplus(int n,fdtype *args)
{
  struct U8_OUTPUT out; 
  u8_string baseuri; int baseuri_len;
  if (!(FD_STRINGP(args[0]))) 
    return fd_err(fd_TypeError,"scripturl",
		  u8_strdup("script name"),args[0]);
  else if ((n>2) && ((n%2)==0))
    return fd_err(fd_SyntaxError,"scripturl",
		  strd("odd number of arguments"),FD_VOID);
  else {U8_INIT_OUTPUT(&out,64);}
  baseuri=FD_STRDATA(args[0]);
  baseuri_len=FD_STRLEN(args[0]);
  if (n == 2) {
    u8_puts(&out,baseuri);
    if (strchr(baseuri,'?')==NULL) u8_putc(&out,'?');
    else if (baseuri[baseuri_len-1]!='&') u8_putc(&out,'&');
    if (FD_STRINGP(args[1]))
      fd_uri_output(&out,FD_STRDATA(args[1]),"#&=;");
    else if (FD_OIDP(args[1])) {
      FD_OID addr=FD_OID_ADDR(args[1]);
      u8_printf(&out,":@%x/%x",FD_OID_HI(addr),FD_OID_LO(addr));}
    else {
      u8_string as_string=fd_dtype2string(args[1]);
      u8_putc(&out,':');
      fd_uri_output(&out,as_string,"#&=;");
      u8_free(as_string);}}
  else if (n%2) {
    int i=1, params=0;
    u8_puts(&out,FD_STRDATA(args[0]));
    if (strchr(baseuri,'?')==NULL) u8_putc(&out,'?');
    else if (baseuri[baseuri_len-1]!='&') u8_putc(&out,'&');
    while (i <n) {
      if (FD_EMPTY_CHOICEP(args[i+1])) {
	i=i+2; continue;}
      if (params>0) u8_putc(&out,'&');
      params++;
      if (FD_STRINGP(args[i]))
	fd_uri_output(&out,FD_STRDATA(args[i]),"?#=&");
      else if (FD_SYMBOLP(args[i]))
	fd_uri_output(&out,FD_SYMBOL_NAME(args[i]),"?#=&");
      else {
	u8_free(out.u8_outbuf);
	return fd_err(fd_SyntaxError,"scripturl",
		      u8_strdup(_("invalid parameter")),
		      fd_incref(args[i]));}
      u8_putc(&out,'=');
      if (FD_STRINGP(args[i+1]))
	fd_uri_output(&out,FD_STRDATA(args[i+1]),"?#=&");
      else if (FD_OIDP(args[i+1])) {
	FD_OID addr=FD_OID_ADDR(args[i+1]);
	u8_printf(&out,":@%x/%x",FD_OID_HI(addr),FD_OID_LO(addr));}
      else if (FD_CHOICEP(args[i+1])) {
	u8_string name=((FD_STRINGP(args[i])) ? (FD_STRDATA(args[i])) :
			(FD_SYMBOLP(args[i])) ? (FD_SYMBOL_NAME(args[i]))
			: ((u8_string)"NEVER"));
	int first_one=1;
	FD_DO_CHOICES(value,args[i+1])
	  if (FD_STRINGP(value)) {
	    if (first_one) {
	      fd_uri_output(&out,FD_STRING_DATA(value),"?#=&");
	      first_one=0;}
	    else {
	      u8_putc(&out,'&');
	      fd_uri_output(&out,name,"?#=&");
	      u8_putc(&out,'=');
	      fd_uri_output(&out,FD_STRING_DATA(value),"?#=&");}}
	  else {
	    u8_string as_string=fd_dtype2string(value);
	    if (first_one) {
	      fd_uri_output(&out,as_string,"?#=&"); first_one=0;}
	    else {
	      u8_putc(&out,'&');
	      fd_uri_output(&out,name,"?#=&");
	      u8_putc(&out,'=');
	      fd_uri_output(&out,as_string,"?#=&");}
	    u8_free(as_string);}}
      else {
	u8_string as_string=fd_dtype2string(args[i+1]);
	fd_uri_output(&out,as_string,"?#=&");
	u8_free(as_string);}
      i=i+2;}}
  else return fd_err(fd_SyntaxError,"scripturl",u8_strdup(FD_STRDATA(args[0])),FD_VOID);
  return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
}

/* Outputing tables to XHTML */

static void output_xhtml_table(U8_OUTPUT *out,fdtype tbl,fdtype keys,
			       u8_string class_name,fdtype xmloidfn)
{
  u8_printf(out,"<table class='%s'>\n",class_name);
  if (FD_OIDP(tbl))
    u8_printf(out,"<tr><th colspan='2' class='header'>%lk</th></tr>\n",tbl);
  else if (FD_HASHTABLEP(tbl))
    u8_printf(out,"<tr><th colspan='2' class='header'>%lk</th></tr>\n",tbl);
  else u8_printf(out,"<tr><th colspan='2' class='header'>%s</th></tr>\n",
		 fd_type_names[FD_PTR_TYPE(tbl)]);
  {
    FD_DO_CHOICES(key,keys) {
      fdtype _value=
	((FD_OIDP(tbl)) ? (fd_frame_get(tbl,key)) :
	 (fd_get(tbl,key,FD_EMPTY_CHOICE)));
      fdtype values=fd_simplify_choice(_value);
      u8_printf(out,"  <tr><th>");
      xmlout_helper(out,NULL,key,xmloidfn,NULL);
      if (FD_EMPTY_CHOICEP(values))
	u8_printf(out,"</th>\n    <td class='novalues'>No values</td></tr>\n");
      else if (FD_CHOICEP(values)) {
	int first_item=1;
	FD_DO_CHOICES(value,values) {
	  if (first_item) {
	    u8_puts(out,"</th>\n    <td><div class='value'>");
	    first_item=0;}
	  else
	    u8_puts(out,"        <div class='value'>");
	  xmlout_helper(out,NULL,value,xmloidfn,NULL);
	  u8_puts(out,"</div> \n");}
	u8_printf(out,"    </td></tr>\n");}
      else {
	u8_printf(out,"</th>\n      <td class='singlevalue'>");
	xmlout_helper(out,NULL,values,xmloidfn,NULL);
	u8_printf(out,"</td></tr>\n");}}}
  u8_printf(out,"</table>\n");
}

static fdtype table2html_handler(fdtype expr,fd_lispenv env)
{
  u8_string classname=NULL;
  U8_OUTPUT *out=fd_get_default_output();
  fdtype xmloidfn=fd_symeval(xmloidfn_symbol,env);
  fdtype tables, classarg, slotids;
  tables=fd_eval(fd_get_arg(expr,1),env);
  if (FD_ABORTP(tables))return tables;
  else if (FD_VOIDP(tables))
    return fd_err(fd_SyntaxError,"table2html_handler",NULL,expr);
  classarg=fd_eval(fd_get_arg(expr,2),env);
  if (FD_ABORTP(classarg)) {
    fd_decref(tables); return classarg;}
  else if (FD_STRINGP(classarg)) classname=FD_STRDATA(classarg);
  else if ((FD_VOIDP(classarg)) || (FD_FALSEP(classarg))) {}
  else {
    fd_decref(tables);
    return fd_type_error(_("string"),"table2html_handler",classarg);}
  slotids=fd_eval(fd_get_arg(expr,3),env);
  if (FD_ABORTP(slotids)) {
    fd_decref(tables); fd_decref(classarg); return slotids;}
  {
    FD_DO_CHOICES(table,tables)
      if (FD_TABLEP(table)) {
	fdtype keys=((FD_VOIDP(slotids)) ? (fd_getkeys(table)) : (slotids));
	if (classname)
	  output_xhtml_table(out,table,keys,classname,xmloidfn);
	else if (FD_OIDP(table))
	  output_xhtml_table(out,table,keys,"frame_table",xmloidfn);
	else output_xhtml_table(out,table,keys,"table_table",xmloidfn);}
      else {
	fd_decref(tables); fd_decref(classarg); fd_decref(slotids);
	return fd_type_error(_("table"),"table2html_handler",table);}}
  return FD_VOID;
}

/* XMLEVAL primitives */

static fdtype xmleval_handler(fdtype expr,fd_lispenv env)
{
  if (!(FD_PAIRP(FD_CDR(expr))))
    return fd_err(fd_SyntaxError,"xmleval_handler",NULL,FD_VOID);
  else {
    U8_OUTPUT *out=fd_get_default_output();
    fdtype xmlarg=fd_eval(FD_CADR(expr),env), v=FD_VOID;
    fdtype envarg=fd_eval(fd_get_arg(expr,2),env);
    fd_lispenv target_env=env;
    if (FD_ABORTP(xmlarg)) return xmlarg;
    if (FD_VOIDP(envarg)) {}
    else if (FD_PTR_TYPEP(envarg,fd_environment_type)) 
      target_env=(fd_lispenv)envarg;
    else {
      fd_decref(xmlarg);
      return fd_type_error("environment","xmleval_handler",envarg);}
    v=fd_xmleval(out,xmlarg,target_env);
    u8_flush(out); fd_decref(xmlarg); fd_decref(envarg);
    return v;}
}

/* Javascript output */

/* This generates a javascript function call, transforming the
   arguments into strings.  This uses double quotes to quote the
   arguments because the output is typically inserted in attributes
   which are single quoted. */
static fdtype output_javascript(u8_output out,fdtype args,fd_lispenv env)
{
  if (FD_EMPTY_LISTP(args))
    return fd_err(fd_SyntaxError,"output_javascript",NULL,args);
  else {
    int i=0;
    fdtype head_expr=FD_CAR(args), head=fd_eval(head_expr,env), body=FD_CDR(args);
    if (!(FD_STRINGP(head)))
      return fd_type_error(_("javascript function name"),
			   "output_javascript",head);
    else u8_printf(out,"%s(",FD_STRDATA(head));
    FD_DOLIST(elt,body) {
      fdtype val;
      if (i>0) u8_putc(out,','); i++;
      if (FD_NEED_EVALP(elt))
	val=fd_eval(elt,env);
      else val=fd_incref(elt);
      if (FD_VOIDP(val)) {}
      else if (FD_FIXNUMP(val))
	u8_printf(out,"%d",FD_FIX2INT(val));
      else if (FD_FLONUMP(val))
	u8_printf(out,"%f",FD_FLONUM(val));
      else if (FD_STRINGP(val)) {
	u8_byte *scan=FD_STRDATA(val);
	u8_putc(out,'"');
	while (*scan) {
	  int c=u8_sgetc(&scan);
	  if (c=='"') {u8_putc(out,'\\'); u8_putc(out,'"');}
	  else u8_putc(out,c);}
      	u8_putc(out,'"');}
      else if (FD_OIDP(val))
	u8_printf(out,"\":@%x/%x\"",
		  FD_OID_HI(FD_OID_ADDR(val)),
		  FD_OID_LO(FD_OID_ADDR(val)));
      else {
	U8_OUTPUT tmp; u8_byte buf[128]; u8_byte *scan;
	U8_INIT_OUTPUT_BUF(&tmp,128,buf);
	tmp.u8_streaminfo=tmp.u8_streaminfo|U8_STREAM_TACITURN;
	u8_puts(out,"\":");
	fd_unparse(&tmp,val);
	scan=tmp.u8_outbuf; while (scan<tmp.u8_outptr) {
	  int c=u8_sgetc(&scan);
	  if (c<0) break;
	  else if (c=='\\') {
	    c=u8_sgetc(&scan);
	    if (c<0) {u8_putc(out,'\\'); break;}
	    else {u8_putc(out,'\\'); u8_putc(out,c);}}
	  else if (c=='"') {
	    u8_putc(out,'\\'); u8_putc(out,c);}
	  else u8_putc(out,c);}
	if (tmp.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(tmp.u8_outbuf);
	u8_puts(out,"\"");}
      fd_decref(val);}
    u8_putc(out,')');
    fd_decref(head);
    return FD_VOID;}
}

static fdtype javascript_handler(fdtype expr,fd_lispenv env)
{
  fdtype retval; struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,256);
  retval=output_javascript(&out,FD_CDR(expr),env);
  if (FD_VOIDP(retval))
    return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
  else {
    u8_free(out.u8_outbuf); return retval;}
}

static fdtype javastmt_handler(fdtype expr,fd_lispenv env)
{
  fdtype retval; struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,256);
  retval=output_javascript(&out,FD_CDR(expr),env);
  if (FD_VOIDP(retval)) {
    u8_putc(&out,';');
    return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);}
  else {
    u8_free(out.u8_outbuf); return retval;}
}

/* Soap envelope generation */

/* This should probably be customizable */
static u8_string soapenvprefix="SOAP-ENV";
static u8_string soapenvopen=
  "<SOAP-ENV:Envelope xmlns:SE='http://www.w3.org/2003/05/soap-envelope' xmlns:xsd='http://www.w3.org/2001/XMLSchema' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'>\n";
static u8_string soapenvclose="\n</SOAP-ENV:Envelope>";
static u8_string soapbodyopen="<SOAP-ENV:Body>";
static u8_string soapbodyclose="</SOAP-ENV:Body>";
static u8_string soapheaderopen="  <SOAP-ENV:Header>\n";
static u8_string soapheaderclose="\n  </SOAP-ENV:Header>";

static fdtype soapenvelope_handler(fdtype expr,fd_lispenv env)
{
  U8_OUTPUT *out=fd_get_default_output();
  fdtype header_arg=fd_get_arg(expr,1);
  fdtype body=fd_get_body(expr,2);
  u8_puts(out,soapenvopen);
  if (FD_NEED_EVALP(header_arg)) {
    fdtype value;
    u8_puts(out,soapheaderopen);
    value=fd_eval(header_arg,env);
    if (FD_STRINGP(value)) u8_puts(out,FD_STRDATA(value));
    fd_decref(value);
    u8_puts(out,soapheaderclose);}
  u8_puts(out,soapbodyopen);
  fd_printout_to(out,body,env);
  u8_puts(out,soapbodyclose);
  u8_puts(out,soapenvclose);
  return FD_VOID;
}

/* Initialization functions */

static u8_string markup_printf_handler
  (u8_output s,char *cmd,u8_byte *buf,int bufsiz,va_list *args)
{
  if (strchr(cmd,'l')) {
    fdtype val=va_arg(*args,fdtype);
    u8_string str=fd_dtype2string(val);
    emit_xmlcontent(s,str); u8_free(str);}
  else {
    u8_string str=va_arg(*args,u8_string);
    if (str) emit_xmlcontent(s,str);
    else emit_xmlcontent(s,"(null)");}
  return NULL;
}

FD_EXPORT void fd_init_xmloutput_c()
{
  fdtype fdweb_module=
    fd_new_module("FDWEB",(0));
  fdtype safe_fdweb_module=
    fd_new_module("FDWEB",(FD_MODULE_SAFE));
  fdtype xhtml_module=
    fd_new_module("XHTML",FD_MODULE_SAFE);

  fdtype markup_prim=fd_make_special_form("markup",markup_handler);
  fdtype markupstar_prim=fd_make_special_form("markup*",markupstar_handler);
  fdtype markupblock_prim=
    fd_make_special_form("markupblock",markupblock_handler);
  fdtype markupstarblock_prim=
    fd_make_special_form("markup*block",markupstarblock_handler);
  fdtype emptymarkup_prim=
    fd_make_special_form("emptymarkup",emptymarkup_handler);
  fdtype xmlout_prim=fd_make_special_form("XMLOUT",xmlout);
  fdtype xmlblock_prim=fd_make_special_form("XMLBLOCK",xmlblock);
  fdtype xmlblockn_prim=fd_make_special_form("XMLBLOCKN",xmlblockn);
  fdtype xmlelt_prim=fd_make_special_form("XMLELT",xmlentry);

  /* Applicable XML generators (not special forms) */
  fdtype xmlempty_dproc=fd_make_cprimn("XMLEMPTY",xmlemptyelt,0);
  fdtype xmlempty_proc=fd_make_ndprim(xmlempty_dproc);
  fdtype xmlify_proc=fd_make_cprim1("XMLIFY",xmlify,1);
  fdtype oid2id_proc=
    fd_make_cprim2x("OID2ID",oid2id,1,fd_oid_type,FD_VOID,-1,FD_VOID);
  fdtype scripturl_proc=
    fd_make_ndprim(fd_make_cprimn("SCRIPTURL",scripturl,1));
  fdtype scripturlplus_proc=
    fd_make_ndprim(fd_make_cprimn("SCRIPTURL+",scripturlplus,2));
  fdtype fdscripturl_proc=
    fd_make_ndprim(fd_make_cprimn("FDSCRIPTURL",fdscripturl,2));

  u8_printf_handlers['k']=markup_printf_handler;

  {
    fdtype module=safe_fdweb_module;
    fd_store(module,fd_intern("XMLOUT"),xmlout_prim);
    fd_store(module,fd_intern("XMLBLOCK"),xmlblock_prim);
    fd_store(module,fd_intern("XMLBLOCKN"),xmlblockn_prim);
    fd_store(module,fd_intern("XMLELT"),xmlelt_prim);
    fd_defn(module,xmlempty_proc);
    fd_defn(module,xmlify_proc);
    fd_defn(module,oid2id_proc);
    fd_defn(module,scripturl_proc);
    fd_defn(module,scripturlplus_proc);
    fd_defn(module,fdscripturl_proc);
    fd_store(module,fd_intern("MARKUPFN"),markup_prim);
    fd_store(module,fd_intern("MARKUP*FN"),markupstar_prim);
    fd_defspecial(module,"SOAPENVELOPE",soapenvelope_handler);
  }
  
  {
    fdtype module=fdweb_module;
    fd_store(module,fd_intern("XMLOUT"),xmlout_prim);
    fd_store(module,fd_intern("XMLBLOCK"),xmlblock_prim);
    fd_store(module,fd_intern("XMLBLOCKN"),xmlblockn_prim);
    fd_store(module,fd_intern("XMLELT"),xmlelt_prim);
    fd_idefn(module,xmlempty_proc);
    fd_idefn(module,xmlify_proc);
    fd_idefn(module,oid2id_proc);
    fd_idefn(module,scripturl_proc);
    fd_idefn(module,scripturlplus_proc);
    fd_idefn(module,fdscripturl_proc);
    fd_store(module,fd_intern("MARKUPFN"),markup_prim);
    fd_store(module,fd_intern("MARKUP*FN"),markupstar_prim);
    fd_defspecial(module,"SOAPENVELOPE",soapenvelope_handler);
  }
  
  fd_defspecial(xhtml_module,"ANCHOR",doanchor);
  fd_defspecial(xhtml_module,"ANCHOR*",doanchor_star);
  fd_idefn(xhtml_module,fd_make_cprim1("%XMLOID",xmloid,1));

  fd_defspecial(xhtml_module,"XHTML",raw_xhtml_handler);
  fd_idefn(xhtml_module,fd_make_cprim0("NBSP",nbsp_prim,0));

  fd_store(xhtml_module,fd_intern("DIV"),markupstarblock_prim);
  fd_store(xhtml_module,fd_intern("SPAN"),markupstar_prim);
  
  fd_store(xhtml_module,fd_intern("P"),markupblock_prim);
  fd_store(xhtml_module,fd_intern("P*"),markupstarblock_prim);
  fd_store(xhtml_module,fd_intern("H1"),markupblock_prim);
  fd_store(xhtml_module,fd_intern("H1*"),markupstarblock_prim);
  fd_store(xhtml_module,fd_intern("H2"),markupblock_prim);
  fd_store(xhtml_module,fd_intern("H2*"),markupstarblock_prim);

  fd_store(xhtml_module,fd_intern("UL"),markupblock_prim);
  fd_store(xhtml_module,fd_intern("UL*"),markupstarblock_prim);
  fd_store(xhtml_module,fd_intern("LI"),markupblock_prim);
  fd_store(xhtml_module,fd_intern("LI*"),markupstarblock_prim);

  fd_store(xhtml_module,fd_intern("STRONG"),markup_prim);
  fd_store(xhtml_module,fd_intern("EM"),markup_prim);
  fd_store(xhtml_module,fd_intern("TT"),markup_prim);
  fd_store(xhtml_module,fd_intern("DEFN"),markup_prim);

  fd_store(xhtml_module,fd_intern("FORM"),markupstarblock_prim);

  fd_store(xhtml_module,fd_intern("TABLE"),markupblock_prim);
  fd_store(xhtml_module,fd_intern("TABLE*"),markupstarblock_prim);
  fd_store(xhtml_module,fd_intern("TR"),markupblock_prim);
  fd_store(xhtml_module,fd_intern("TR*"),markupstarblock_prim);
  fd_store(xhtml_module,fd_intern("TD"),markup_prim);
  fd_store(xhtml_module,fd_intern("TD*"),markupstar_prim);
  fd_store(xhtml_module,fd_intern("TH"),markup_prim);
  fd_store(xhtml_module,fd_intern("TH*"),markupstar_prim);

  fd_store(xhtml_module,fd_intern("IMG"),emptymarkup_prim);
  fd_store(xhtml_module,fd_intern("INPUT"),emptymarkup_prim);
  fd_store(xhtml_module,fd_intern("BR"),emptymarkup_prim);
  fd_store(xhtml_module,fd_intern("HR"),emptymarkup_prim);

  fd_defspecial(xhtml_module,"TABLE->HTML",table2html_handler);

  fd_defspecial(fdweb_module,"XMLEVAL",xmleval_handler);
  fd_defspecial(safe_fdweb_module,"XMLEVAL",xmleval_handler);

  fd_decref(markup_prim); fd_decref(markupstar_prim);
  fd_decref(markupblock_prim); fd_decref(markupstarblock_prim);
  fd_decref(emptymarkup_prim); fd_decref(xmlout_prim);
  fd_decref(xmlblockn_prim); fd_decref(xmlblock_prim);
  fd_decref(xmlelt_prim);

  /* Not strictly XML of course, but a neighbor */
  fd_defspecial(xhtml_module,"JAVASCRIPT",javascript_handler);
  fd_defspecial(xhtml_module,"JAVASTMT",javastmt_handler);

  xmloidfn_symbol=fd_intern("%XMLOID");
  id_symbol=fd_intern("%ID");
  href_symbol=fd_intern("HREF");
  class_symbol=fd_intern("CLASS");
  obj_name=fd_intern("OBJ-NAME");
  quote_symbol=fd_intern("QUOTE");
  raw_name_symbol=fd_intern("%%NAME");
  browseinfo_symbol=fd_intern("BROWSEINFO");

  error_stylesheet=u8_strdup("/css/fdweb.css");
  fd_register_config
    ("ERRORSTYLESHEET",_("Default style sheet for web errors"),
     fd_sconfig_get,fd_sconfig_set,&error_stylesheet);

  fd_register_config
    ("HTMLBACKTRACEINDENT",
     _("How many entries in a web backtrace to indent "),
     fd_intconfig_get,fd_intconfig_set,&backtrace_indent_depth);

  fd_register_config
    ("BROWSEINFO",
     _("How to display OIDs for browsing in HTML/XML"),
     browseinfo_config_get,browseinfo_config_set,NULL);
  fd_register_config
    ("BROWSEOIDURI",
     _("Default anchor URI for OID references"),
     fd_sconfig_get,fd_sconfig_set,&default_browse_uri);
  fd_register_config
    ("BROWSEOIDCLASS",
     _("Default HTML CSS class for OID references"),
     fd_sconfig_get,fd_sconfig_set,&default_browse_class);

#if (FD_THREADS_ENABLED)
  u8_init_mutex(&browseinfo_lock);
#endif

}


