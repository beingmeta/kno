/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
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
#include "fdb/tables.h"
#include "fdb/eval.h"
#include "fdb/ports.h"
#include "fdb/fdweb.h"

#include <libu8/xfiles.h>
#include <libu8/u8stringfns.h>

FD_EXPORT void fd_uri_output(u8_output out,u8_string uri,char *escape);

#define strd u8_strdup

/*
  (XMLOUT .. .. ..) does escaping
  (XMLTAG 'tag p v p v ...)
  (XMLENV `(tag p v p v p v) ...) 
  (DIV () ...)
  (SPAN () ...)
  (special* () ...)
  (special ....)

  (URI string)
*/

static fdtype xmloidfn_symbol, obj_name, id_symbol;
static fdtype href_symbol, class_symbol, raw_name_symbol;

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

static void entify(u8_output out,u8_string value)
{
  u8_byte *scan=value; int c;
  while ((c=u8_sgetc(&scan))>=0)
    if (c=='<') u8_puts(out,"&#60;");
    else if (c=='>') u8_puts(out,"&#62;");
    else if (c=='&') u8_puts(out,"&#38;");
    else u8_putc(out,c);
} 

static void entify_lower(u8_output out,u8_string value)
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

static int emit_xmlattrib
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

static fdtype oidunxmlify(fdtype string)
{
  u8_string s=FD_STRDATA(string), addr_start=strchr(s,'_');
  FD_OID addr; unsigned int hi, lo;
  if (addr_start) sscanf(addr_start,"_%x_%x",&hi,&lo);
  else return FD_EMPTY_CHOICE;
  FD_SET_OID_HI(addr,hi); FD_SET_OID_LO(addr,lo);
  return fd_make_oid(addr);
}

static int emit_xmlcontent(u8_output out,u8_string content)
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
	fd_seterr(fd_SyntaxError,"open_markup",NULL,fd_incref(attribs));
	return -1;}
    else if ((FD_PAIRP(elt)) && (FD_PAIRP(FD_CDR(elt)))) {
      fdtype attrib_name=FD_CAR(elt);
      fdtype attrib_expr=FD_CAR(FD_CDR(elt));
      if (!((FD_SYMBOLP(attrib_name)) || (FD_STRINGP(attrib_name))))
	fd_seterr(fd_SyntaxError,"open_markup",NULL,fd_incref(attribs));
      if ((FD_SYMBOLP(attrib_expr)) || (FD_PAIRP(attrib_expr))) {
	fdtype val=((env) ? (fd_eval(attrib_expr,env)) : (fd_incref(attrib_expr)));
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
  if (FD_EXCEPTIONP(x)) return 0;
  else if (FD_VOIDP(x)) return 1;
  if (FD_STRINGP(x))
    emit_xmlcontent(out,FD_STRDATA(x));
  else if ((FD_APPLICABLEP(xmloidfn)) && (FD_OIDP(x))) {
    fdtype result=fd_apply((fd_function)xmloidfn,1,&x);
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

static fdtype xmlblock(fdtype expr,fd_lispenv env)
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
    if (FD_EXCEPTIONP(tagspec)) {
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
    return fd_erreify();}
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
  if (close_markup(out,tagname)<0) {
    fd_decref(xmloidfn);
    return fd_erreify();}
  if (tagname!=tagbuf) u8_free(tagname);
  u8_flush(out);
  fd_decref(xmloidfn);
  if (tmpout.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(tmpout.u8_outbuf);
  return FD_VOID;
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
      return fd_erreify();}
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
      return fd_erreify();}
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
    return fd_erreify();
  else {
    return FD_VOID;}
}

/* XHTML error report */

#define DEFAULT_DOCTYPE \
  "<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.1//EN\"\
               \"http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd\">"
#define DEFAULT_XMLPI \
  "<?xml version='1.0' charset='utf-8' ?>"

static u8_string error_stylesheet="/css/fdweberr.css";

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
    u8_lock_mutex(&(sm->lock));
    slotmap_size=FD_XSLOTMAP_SIZE(sm);
    scan=sm->keyvals; limit=sm->keyvals+slotmap_size;
    while (scan<limit)
      if (embeddedp(focus,scan->key)) {
	u8_unlock_mutex(&(sm->lock)); return 1;}
      else if (embeddedp(focus,scan->value)) {
	u8_unlock_mutex(&(sm->lock)); return 1;}
      else scan++;
    u8_unlock_mutex(&(sm->lock));
    return 0;}
  else return 0;
}

static void output_backtrace(u8_output s,fdtype bt,fdtype head)
{
  if (FD_PAIRP(bt)) {
    fdtype entry=FD_CAR(bt); 
    if (FD_VECTORP(entry)) {
      fdtype head=FD_VECTOR_REF(entry,0);
      int i=1, len=FD_VECTOR_LENGTH(entry);
      output_backtrace(s,FD_CDR(bt),FD_VOID);
      u8_printf(s,"<div class='rail'>(<span class='name'>%lk</span>",head);
      while (i<len) {
	fdtype elt=FD_VECTOR_REF(entry,i); i++;
	if ((FD_PAIRP(elt)) || (FD_SYMBOLP(elt)))
	  u8_printf(s,"<span class='arg'>&apos;%lk</span>",elt);
	else u8_printf(s,"<span class='arg'>%lk</span>",elt);}
      u8_printf(s,")</div>\n");}
    else if ((!(FD_PAIRP(entry))) && (FD_TABLEP(entry))) {
      fdtype keys=fd_getkeys(entry);
      output_backtrace(s,FD_CDR(bt),FD_VOID);
      u8_printf(s,"<div class='bindings'>");
      if (FD_SYMBOLP(head))
	u8_printf(s,"<span class='head'>%lk</span>",head);
      if (FD_EXCEPTIONP(keys)) {
	fd_decref(keys);
	u8_printf(s,"%lk</div>\n",entry);}
      else {
	FD_DO_CHOICES(key,keys) {
	  fdtype val=fd_get(entry,key,FD_VOID);
	  u8_printf(s,"<span class='binding'>%lk=%lk;</span> ",key,val);
	  fd_decref(val);}
	fd_decref(keys);
	u8_printf(s,"</div>\n");}}
    else if (FD_STRINGP(entry)) {
      output_backtrace(s,FD_CDR(bt),head);
      u8_printf(s,"<div class='defcxt'>%k</div>\n",FD_STRDATA(entry));}
    else {
      fdtype scan=FD_CDR(bt), focus=entry;
      U8_OUTPUT tmp; u8_byte *focus_start;
      while ((FD_PAIRP(scan)) &&
	     (FD_PAIRP(FD_CAR(scan))) &&
	     (embeddedp(FD_CAR(scan),entry))) {
	focus=FD_CAR(scan); scan=FD_CDR(scan);}
      if (FD_PAIRP(focus))
	output_backtrace(s,scan,FD_CAR(focus));
      else output_backtrace(s,scan,FD_VOID);
      U8_INIT_OUTPUT(&tmp,128);
      u8_printf(s,"<div class='expr'>");
      fd_pprint_focus(&tmp,entry,focus,NULL,0,80,"#@?#","#@?#");
      if (focus_start=strstr(tmp.u8_outbuf,"#@?#")) {
	u8_byte *focus_end=strstr(focus_start+4,"#@?#");
	*focus_start='\0'; fd_entify(s,tmp.u8_outbuf);
	*focus_end='\0'; u8_printf(s,"<span class='focus'>");
	fd_entify(s,focus_start+4);
	u8_printf(s,"</span>");
	fd_entify(s,focus_end+4);}
      else u8_puts(s,tmp.u8_outbuf);
      u8_free(tmp.u8_outbuf);
      u8_printf(s,"\n</div>\n");}}
}

FD_EXPORT
void fd_xhtmlerrorpage(u8_output s,fdtype error)
{
  struct FD_EXCEPTION_OBJECT *eo=
    FD_GET_CONS(error,fd_exception_type,FD_EXCEPTION_OBJECT *);
  struct FD_ERRDATA *e=&(eo->data);
  s->u8_outptr=s->u8_outbuf;
  u8_printf(s,"%s\n%s\n",DEFAULT_DOCTYPE,DEFAULT_XMLPI);
  u8_printf(s,"<html>\n<head>\n<title>");
  u8_printf(s,"%k",e->cond);
  if (e->cxt) u8_printf(s,"%k",e->cxt);
  if (e->details) u8_printf(s," (%k)",e->details);
  if (!(FD_VOIDP(e->irritant))) u8_printf(s,": %lk",e->irritant);
  u8_printf(s,"</title>\n");
  u8_printf(s,"<link rel='stylesheet' type='text/css' href='%s'/>\n",
	    error_stylesheet);
  u8_printf(s,"</head>\n<body>\n<h1 class='server_sorry'>");
  u8_printf(s,"There was an unexpected error processing your request</h1>\n");
  u8_printf(s,"<div class='error'>\n");
  if (e->cxt) u8_printf(s,"In <span class='cxt'>%k</span>, ",e->cxt);
  u8_printf(s," <span class='ex'>%k</span>",e->cond);
  if (e->details) u8_printf(s," <span class='details'>%k</span>",e->details);
  if (!(FD_VOIDP(e->irritant))) 
    u8_printf(s,"<span class='irritant'>%lk</span>",e->irritant);
  u8_printf(s,"</div>\n");
  u8_printf(s,"<div class='backtrace'>\n");
  output_backtrace(s,eo->backtrace,FD_VOID);
  u8_printf(s,"</div>\n");
  u8_printf(s,"</body>\n</html>\n");  
}

/* Getting oid display data */

static fdtype default_browse_info=FD_EMPTY_CHOICE;
static struct FD_HASHTABLE browse_info_table;

static fdtype get_browse_info(fdtype arg)
{
  fd_pool p=fd_oid2pool(arg);
  if (p==NULL) return FD_VOID;
  else {
    fdtype lp=fd_pool2lisp(p);
    return fd_hashtable_get(&browse_info_table,lp,default_browse_info);}
}

static fdtype set_browse_info
  (fdtype poolarg,fdtype script,fdtype classname,fdtype displayer)
{
  fd_pool p; fdtype entry;
  if (FD_PRIM_TYPEP(poolarg,fd_pool_type)) p=fd_lisp2pool(poolarg);
  else if (FD_STRINGP(poolarg)) 
    p=fd_name2pool(FD_STRDATA(poolarg));
  if (FD_FALSEP(poolarg)) {}
  else if (p==NULL)
    return fd_type_error("pool","set_browse_info",poolarg);
  if (FD_VOIDP(script)) script=fdtype_string("browse.fdcgi");
  if (FD_VOIDP(classname)) classname=fdtype_string("oid");
  if (FD_VOIDP(displayer)) displayer=fd_intern("OBJ-NAME");
  entry=fd_make_vector(3,fd_incref(script),fd_incref(classname),
		       fd_incref(displayer));
  if (FD_FALSEP(poolarg)) {
    fd_decref(default_browse_info);
    default_browse_info=fd_incref(entry);}
  else fd_hashtable_store(&browse_info_table,fd_pool2lisp(p),entry);
  fd_decref(entry);
  return FD_VOID;
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
    fdtype browse_info=get_browse_info(target);
    if (FD_VECTORP(browse_info)) 
      u8_printf(out,"<a href='%s?:@%x/%x' class='%s'>",
		FD_STRDATA(FD_VECTOR_REF(browse_info,0)),
		FD_OID_HI(addr),FD_OID_LO(addr),
		FD_STRDATA(FD_VECTOR_REF(browse_info,1)));
    else u8_printf(out,"<a href='browse.fdcgi?:@%x/%x' class='oid'>",
		   FD_OID_HI(addr),(FD_OID_LO(addr)));}
  else {
    return fd_type_error(_("valid anchor target"),"doanchor",target);}
  xmloidfn=fd_symeval(xmloidfn_symbol,env);
  while (FD_PAIRP(body)) {
    fdtype value=fasteval(FD_CAR(body),env);
    if (FD_ABORTP(value)) {
      fd_decref(xmloidfn);
      return value;}
    else if (xmlout_helper(out,&tmpout,value,xmloidfn,env))
      fd_decref(value);
    else {
      fd_decref(xmloidfn);
      return value;}
    body=FD_CDR(body);}
  u8_printf(out,"</a>");
  u8_flush(out);
  fd_decref(xmloidfn);
  if (tmpout.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(tmpout.u8_outbuf);
  return FD_VOID;
}

static int has_class_attrib(fdtype attribs)
{
  fdtype scan=attribs;
  while (FD_PAIRP(scan))
    if (FD_EQ(FD_CAR(scan),class_symbol)) return 1;
    else {
      scan=FD_CDR(scan);
      if (FD_PAIRP(scan)) scan=FD_CDR(scan);}
  return 0;
}

static fdtype doanchor_star(fdtype expr,fd_lispenv env)
{
  U8_OUTPUT *out=fd_get_default_output(), tmpout;
  fdtype target=fd_eval(fd_get_arg(expr,1),env), xmloidfn;
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
    fdtype browse_info=get_browse_info(target);
    if (has_class_attrib(attribs)) {}
    else if (FD_VECTORP(browse_info))
      attribs=fd_init_pair
	(NULL,fd_intern("CLASS"),
	 fd_init_pair(NULL,fd_incref(FD_VECTOR_REF(browse_info,1)),
		      fd_incref(attribs)));
    else attribs=fd_init_pair
      (NULL,fd_intern("CLASS"),
       fd_init_pair(NULL,fdtype_string("oid"),fd_incref(attribs)));
    tmpout.u8_outptr=tmpout.u8_outbuf;
    if ((FD_VECTORP(browse_info)) &&
	(FD_STRINGP(FD_VECTOR_REF(browse_info,1))))
      u8_printf(&tmpout,"%s?:@%x/%x",
		FD_STRDATA(FD_VECTOR_REF(browse_info,1)),
		FD_OID_HI(addr),(FD_OID_LO(addr)));
    else u8_printf(&tmpout,"browse.fdcgi?:@%x/%x",
		   FD_OID_HI(addr),(FD_OID_LO(addr)));
    attribs=fd_init_pair
      (NULL,href_symbol,
       fd_init_pair(NULL,fd_init_string
		    (NULL,tmpout.u8_outptr-tmpout.u8_outbuf,tmpout.u8_outbuf),
		    fd_incref(attribs)));}
  else return fd_type_error(_("valid anchor target"),"doanchor_star",target);
  xmloidfn=fd_symeval(xmloidfn_symbol,env);
  if (open_markup(out,&tmpout,"a",attribs,env,0)<0)
    return fd_erreify();
  while (FD_PAIRP(body)) {
    fdtype value=fasteval(FD_CAR(body),env);
    if (FD_ABORTP(value)) {
      fd_decref(xmloidfn);
      return value;}
    else if (xmlout_helper(out,&tmpout,value,xmloidfn,env))
      fd_decref(value);
    else {
      fd_decref(xmloidfn);
      return value;}
    body=FD_CDR(body);}
  u8_printf(out,"</a>");
  u8_flush(out);
  fd_decref(xmloidfn);
  if (tmpout.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(tmpout.u8_outbuf);
  return FD_VOID;
}

FD_EXPORT void fd_xmloid(u8_output out,fdtype arg)
{
  FD_OID addr=FD_OID_ADDR(arg);
  fdtype browse_info=get_browse_info(arg), name;
  if (out==NULL) out=fd_get_default_output();
  if (FD_VECTORP(browse_info)) {
    fdtype displayer=FD_VECTOR_REF(browse_info,2);
    u8_printf(out,"<a class='%s' href='%s?:@%x/%x'>",
	      FD_STRDATA(FD_VECTOR_REF(browse_info,1)),
	      FD_STRDATA(FD_VECTOR_REF(browse_info,0)),
	      FD_OID_HI(addr),FD_OID_LO(addr));
    if ((FD_OIDP(displayer)) || (FD_SYMBOLP(displayer)))
      name=fd_frame_get(arg,displayer);
    else if (FD_APPLICABLEP(displayer))
      name=fd_apply((fd_function)displayer,1,&arg);
    else name=fd_frame_get(arg,obj_name);
    if (FD_EMPTY_CHOICEP(name))
      u8_printf(out,"%q",arg);
    else if (FD_VOIDP(name)) {}
    else xmlout_helper(out,NULL,name,FD_VOID,NULL);
    fd_decref(name);
    u8_printf(out,"</a>");}
  else {
    u8_printf(out,"<a class='oid' href='browse.fdcgi?:@%x/%x'>",
	      FD_OID_HI(addr),FD_OID_LO(addr));
    name=fd_frame_get(arg,obj_name);
    if (FD_ABORTP(name)) 
      u8_printf(out,"%q",arg);
    else if (FD_EMPTY_CHOICEP(name))
      u8_printf(out,"%q",arg);
    else xmlout_helper(out,NULL,name,FD_VOID,NULL);
    fd_decref(name);
    u8_printf(out,"</a>");}
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
  if (n == 2) {
    fd_uri_output(&out,FD_STRDATA(args[0]),"?#=&");
    u8_putc(&out,'?');
    if (FD_STRINGP(args[1]))
      fd_uri_output(&out,FD_STRDATA(args[1]),"#&=;");
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
    else if (FD_PRIM_TYPEP(envarg,fd_environment_type)) 
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
      if ((FD_PAIRP(elt)) || (FD_SYMBOLP(elt)))
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
    emit_xmlcontent(s,str);}
  return NULL;
}

FD_EXPORT void fd_init_xmloutput_c()
{
  fdtype fdweb_module=fd_new_module("FDWEB",FD_MODULE_DEFAULT);
  fdtype safe_fdweb_module=fd_new_module("FDWEB",(FD_MODULE_DEFAULT|FD_MODULE_SAFE));
  fdtype xhtml_module=fd_new_module("XHTML",FD_MODULE_SAFE);

  fdtype markup_prim=fd_make_special_form("markup",markup_handler);
  fdtype markupstar_prim=fd_make_special_form("markup*",markupstar_handler);
  fdtype markupblock_prim=
    fd_make_special_form("markupblock",markupblock_handler);
  fdtype markupstarblock_prim=
    fd_make_special_form("markup*block",markupstarblock_handler);
  fdtype emptymarkup_prim=
    fd_make_special_form("emptymarkup",emptymarkup_handler);
  fdtype xmlout_prim=fd_make_special_form("XMLOUT",xmlout);
  fdtype xmlblock_prim=fd_make_special_form("XMLOUT",xmlblock);
  fdtype xmlempty_dproc=fd_make_cprimn("XMLEMPTY",xmlemptyelt,0);
  fdtype xmlempty_proc=fd_make_ndprim(xmlempty_dproc);
  fdtype xmltag_proc=fd_make_cprimn("XMLTAG",xmlemptyelt,0);
  fdtype xmlify_proc=fd_make_cprim1("XMLIFY",xmlify,1);
  fdtype oid2id_proc=
    fd_make_cprim2x("OID2ID",oid2id,1,fd_oid_type,FD_VOID,-1,FD_VOID);
  fdtype scripturl_proc=fd_make_ndprim(fd_make_cprimn("SCRIPTURL",scripturl,2));
  fdtype fdscripturl_proc=fd_make_ndprim(fd_make_cprimn("FDSCRIPTURL",fdscripturl,2));

  u8_printf_handlers['k']=markup_printf_handler;

  {fdtype module=safe_fdweb_module;
  fd_store(module,fd_intern("XMLOUT"),xmlout_prim);
  fd_store(module,fd_intern("XMLBLOCK"),xmlblock_prim);
  fd_defn(module,xmltag_proc);
  fd_defn(module,xmlempty_proc);
  fd_defn(module,xmlify_proc);
  fd_defn(module,oid2id_proc);
  fd_defn(module,scripturl_proc);
  fd_store(module,fd_intern("MARKUPFN"),markup_prim);
  fd_store(module,fd_intern("MARKUP*FN"),markupstar_prim);}

  {fdtype module=fdweb_module;
  fd_store(module,fd_intern("XMLOUT"),xmlout_prim);
  fd_store(module,fd_intern("XMLBLOCK"),xmlblock_prim);
  fd_idefn(module,xmltag_proc);
  fd_idefn(module,xmlempty_proc);
  fd_idefn(module,xmlify_proc);
  fd_idefn(module,oid2id_proc);
  fd_idefn(module,scripturl_proc);
  fd_idefn(module,fdscripturl_proc);
  fd_store(module,fd_intern("MARKUPFN"),markup_prim);
  fd_store(module,fd_intern("MARKUP*FN"),markupstar_prim);}

  fd_defspecial(xhtml_module,"ANCHOR",doanchor);
  fd_defspecial(xhtml_module,"ANCHOR*",doanchor_star);
  fd_idefn(xhtml_module,fd_make_cprim1("%XMLOID",xmloid,1));
  fd_idefn(xhtml_module,
	   fd_make_cprim4("SET-BROWSE-INFO!",set_browse_info,2));

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

  fd_defspecial(safe_fdweb_module,"XMLEVAL",xmleval_handler);

  /* Not strictly XML of course, but a neighbor */
  fd_defspecial(xhtml_module,"JAVASCRIPT",javascript_handler);
  fd_defspecial(xhtml_module,"JAVASTMT",javastmt_handler);

  fd_make_hashtable(&browse_info_table,17,NULL);

  xmloidfn_symbol=fd_intern("%XMLOID");
  id_symbol=fd_intern("%ID");
  href_symbol=fd_intern("HREF");
  class_symbol=fd_intern("CLASS");
  obj_name=fd_intern("OBJ-NAME");
  raw_name_symbol=fd_intern("%%NAME");
}

static int fdweb_init_done=0;

FD_EXPORT void fd_init_fdweb()
{
  if (fdweb_init_done) return;
  else {
    fdtype fdweb_module=fd_new_module("FDWEB",FD_MODULE_DEFAULT);
    fdtype safe_fdweb_module=fd_new_module("FDWEB",(FD_MODULE_DEFAULT|FD_MODULE_SAFE));
    fdtype xhtml_module=fd_new_module("XHTML",FD_MODULE_SAFE);
    fdweb_init_done=1;
    fd_init_xmloutput_c();
    fd_init_xmldata_c();
    fd_init_xmlinput_c();
    fd_init_mime_c();
    fd_init_xmleval_c();
    fd_init_cgiexec_c();
    fd_init_urifns_c();
#if (FD_WITH_CURL)
    fd_init_curl_c();
#endif
#if (FD_WITH_EXIF)
    fd_init_exif_c();
#endif
    fd_finish_module(safe_fdweb_module);
    fd_finish_module(fdweb_module);
    fd_finish_module(xhtml_module);}
  fd_register_config("ERRORSTYLESHEET",fd_sconfig_get,fd_sconfig_set,&error_stylesheet);
  fd_register_source_file(FDB_FDWEB_H_VERSION);
  fd_register_source_file(versionid);
}
