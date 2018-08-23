/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1
#define FD_PROVIDE_FASTEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/frames.h"
#include "framerd/tables.h"
#include "framerd/eval.h"
#include "framerd/ports.h"
#include "framerd/fdweb.h"
#include "framerd/support.h"

#include "framerd/support.h"

#include <ctype.h>

#define fast_eval(x,env) (fd_stack_eval(x,env,_stack,0))

#include <libu8/u8xfiles.h>

static lispval xmloidfn_symbol, obj_name, id_symbol, quote_symbol;
static lispval href_symbol, class_symbol, rawtag_symbol, browseinfo_symbol;
static lispval embedded_symbol, estylesheet_symbol, xmltag_symbol;
static lispval modules_symbol, xml_env_symbol;

/* Utility output functions */

static void emit_xmlname(u8_output out,u8_string name)
{
  u8_puts(out,name);
}

static void attrib_entify(u8_output out,u8_string value)
{
  const u8_byte *scan = value; int c;
  while ((c = u8_sgetc(&scan))>=0)
    if (strchr("'<>&\"",c)) /* (strchr("'<>&\"!@$%(){}[]",c)) */
      /* For now, we're not escaping +/-, even though some sources suggest
         it would be a good idea. */
      switch(c) {
      case '\'': u8_puts(out,"&#39;"); break;
      case '\"': u8_puts(out,"&#34;"); break;
      case '<': u8_puts(out,"&#60;"); break;
      case '>': u8_puts(out,"&#62;"); break;
      case '&': u8_puts(out,"&#38;"); break;
        /*
          case '(': u8_puts(out,"&#40;"); break;
          case ')': u8_puts(out,"&#41;"); break;
          case '[': u8_puts(out,"&#91;"); break;
          case ']': u8_puts(out,"&#93;"); break;
          case '{': u8_puts(out,"&#123;"); break;
          case '}': u8_puts(out,"&#125;"); break;
          case '@': u8_puts(out,"&#64;"); break;
          case '!': u8_puts(out,"&#33;"); break;
          case '$': u8_puts(out,"&#36;"); break;
          case '%': u8_puts(out,"&#37;"); break;
          case '-': u8_puts(out,"&#45;"); break;
          case '+': u8_puts(out,"&#43;"); break;
        */
      }
        /* u8_printf(out,"&#%d;",c); */
    else u8_putc(out,c);
}

FD_INLINE_FCN void entify(u8_output out,u8_string value)
{
  const u8_byte *scan = value; int c;
  while ((c = u8_sgetc(&scan))>=0)
    if (c=='<') u8_puts(out,"&#60;");
    else if (c=='>') u8_puts(out,"&#62;");
    else if (c=='&') u8_puts(out,"&#38;");
    else u8_putc(out,c);
}

static U8_MAYBE_UNUSED void entify_lower(u8_output out,u8_string value)
{
  const u8_byte *scan = value; int c;
  while ((c = u8_sgetc(&scan))>=0)
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
  (u8_output out,u8_output tmp,u8_string name,lispval value,int lower)
{
  int c; const u8_byte *scan = name;
  /* Start every attrib with a space, just in case */
  u8_putc(out,' ');
  if (lower) {
    while ((c = u8_sgetc(&scan))>0) u8_putc(out,u8_tolower(c));}
  else u8_puts(out,name);
  u8_puts(out,"=\"");
  if (STRINGP(value))
    attrib_entify(out,CSTRING(value));
  else if (PACKETP(value))
    attrib_entify(out,FD_PACKET_DATA(value));
  else if (SYMBOLP(value)) {
    u8_putc(out,':');
    attrib_entify(out,SYM_NAME(value));}
  else if (OIDP(value))
    u8_printf(out,":@%x/%x",
              FD_OID_HI(FD_OID_ADDR(value)),
              FD_OID_LO(FD_OID_ADDR(value)));
  else if (FIXNUMP(value))
    u8_printf(out,"%lld",FIX2INT(value));
  else if (FD_FLONUMP(value))
    u8_printf(out,"%f",FD_FLONUM(value));
  else if (tmp) {
    tmp->u8_write = tmp->u8_outbuf;
    tmp->u8_streaminfo = tmp->u8_streaminfo|U8_STREAM_TACITURN;
    fd_unparse(tmp,value);
    tmp->u8_streaminfo = tmp->u8_streaminfo&(~U8_STREAM_TACITURN);
    u8_puts(out,":");
    attrib_entify(out,tmp->u8_outbuf);}
  else {
    U8_OUTPUT tmp; u8_byte buf[128];
    U8_INIT_STATIC_OUTPUT_BUF(tmp,128,buf);
    tmp.u8_streaminfo = tmp.u8_streaminfo|U8_STREAM_TACITURN;
    fd_unparse(&tmp,value);
    u8_puts(out,":");
    attrib_entify(out,tmp.u8_outbuf);
    if (tmp.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(tmp.u8_outbuf);}
  u8_puts(out,"\"");
}

FD_EXPORT void fd_emit_xmlattrib
(u8_output out,u8_output tmp,u8_string name,lispval value,int lower)
{
  return emit_xmlattrib(out,tmp,name,value,lower);
}

static lispval xmlify(lispval value)
{
  if (STRINGP(value))
    return value;
  else if (OIDP(value)) {
    U8_OUTPUT tmp; U8_INIT_OUTPUT(&tmp,32);
    u8_printf(&tmp,":%40%x%2f%x",
              FD_OID_HI(FD_OID_ADDR(value)),
              FD_OID_LO(FD_OID_ADDR(value)));
    return fd_init_string(NULL,tmp.u8_write-tmp.u8_outbuf,tmp.u8_outbuf);}
  else {
    U8_OUTPUT tmp; U8_INIT_OUTPUT(&tmp,32);
    tmp.u8_streaminfo = tmp.u8_streaminfo|U8_STREAM_TACITURN;
    u8_putc(&tmp,':'); fd_unparse(&tmp,value);
    return fd_init_string(NULL,tmp.u8_write-tmp.u8_outbuf,tmp.u8_outbuf);}
}

static U8_MAYBE_UNUSED lispval oidunxmlify(lispval string)
{
  u8_string s = CSTRING(string), addr_start = strchr(s,'_');
  FD_OID addr; unsigned int hi, lo;
  if (addr_start) sscanf(addr_start,"_%x_%x",&hi,&lo);
  else return EMPTY;
  memset(&addr,0,sizeof(FD_OID));
  FD_SET_OID_HI(addr,hi); FD_SET_OID_LO(addr,lo);
  return fd_make_oid(addr);
}

static void emit_xmlcontent(u8_output out,u8_string content)
{
  entify(out,content);
}

FD_EXPORT void fd_emit_xmlcontent(u8_output out,u8_string content)
{
  entify(out,content);
}

static int output_markup_attrib
  (u8_output out,u8_output tmp,
   lispval name_expr,lispval value_expr,
   fd_lexenv env)
{
  u8_string attrib_name; lispval attrib_val;
  lispval free_name = VOID, free_value = VOID;
  if (SYMBOLP(name_expr)) attrib_name = SYM_NAME(name_expr);
  else if (STRINGP(name_expr)) attrib_name = CSTRING(name_expr);
  else if ((env) && (PAIRP(name_expr))) {
    free_name = fd_eval(name_expr,env);
    if (FD_ABORTED(free_name)) return free_name;
    else if (SYMBOLP(free_name)) attrib_name = SYM_NAME(free_name);
    else if (STRINGP(free_name)) attrib_name = CSTRING(free_name);
    else attrib_name = NULL;}
  else attrib_name = NULL;
  if (attrib_name) {
    if ((env)&&(FD_NEED_EVALP(value_expr))) {
      free_value = fd_eval(value_expr,env);
      if (FD_ABORTED(free_value)) {
        fd_decref(free_name);
        return -1;}
      else if (FD_DEFAULTP(free_value)) {
        fd_decref(free_name);
        return 0;}
      else attrib_val = free_value;}
    else if (FD_DEFAULTP(value_expr)) {
      fd_decref(free_name);
      return 0;}
    else attrib_val = value_expr;}
  if (attrib_name) {
    if (VOIDP(value_expr)) {
      u8_putc(out,' '); attrib_entify(out,attrib_name);}
    else if (VOIDP(attrib_val)) {
      fd_decref(free_name); fd_decref(free_value);
      return 0;}
    else {
      emit_xmlattrib(out,tmp,attrib_name,attrib_val,SYMBOLP(name_expr));
      fd_decref(free_value);}}
  return 1;
}

static int open_markup(u8_output out,u8_output tmp,u8_string eltname,
                       lispval attribs,fd_lexenv env,int empty)
{
  u8_putc(out,'<');
  emit_xmlname(out,eltname);
  while (PAIRP(attribs)) {
    lispval elt = FD_CAR(attribs);
    /* Kludge to handle case where the attribute name is quoted. */
    if ((PAIRP(elt)) && (FD_CAR(elt) == quote_symbol) &&
        (PAIRP(FD_CDR(elt))) && (SYMBOLP(FD_CADR(elt))))
      elt = FD_CADR(elt);
    if (STRINGP(elt)) {
      u8_putc(out,' ');
      attrib_entify(out,CSTRING(elt));
      attribs = FD_CDR(attribs);}
    else if ((SYMBOLP(elt))&&(PAIRP(FD_CDR(attribs))))
      if (output_markup_attrib(out,tmp,elt,FD_CADR(attribs),env)>=0)
        attribs = FD_CDR(FD_CDR(attribs));
      else {
        if (empty) u8_puts(out,"/>"); else u8_puts(out,">");
        return -1;}
    else if (SYMBOLP(elt)) {
      u8_byte errbuf[150];
      if (empty) u8_puts(out,"/>"); else u8_puts(out,">");
      fd_seterr(fd_SyntaxError,"open_markup",
                u8_sprintf(errbuf,150,
                           _("missing alternating attrib value for %s"),
                           SYM_NAME(elt)),
                fd_incref(attribs));
      return -1;}
    else if ((PAIRP(elt))) {
      lispval val_expr = ((PAIRP(FD_CDR(elt)))?(FD_CADR(elt)):(VOID));
      if (output_markup_attrib(out,tmp,FD_CAR(elt),val_expr,env)>=0)
        attribs = FD_CDR(attribs);
      else {
        if (empty) u8_puts(out,"/>"); else u8_puts(out,">");
        return -1;}}
    else {
      u8_string details=fd_lisp2string(elt);
      if (empty) u8_puts(out,"/>"); else u8_puts(out,">");
      fd_seterr(fd_SyntaxError,"open_markup",
                details,fd_incref(attribs));
      u8_free(details);
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
  (u8_output out,u8_string eltname,lispval attribs,int empty)
{
  return open_markup(out,NULL,eltname,attribs,NULL,empty);
}

static u8_string get_tagname(lispval tag,u8_byte *buf,int len)
{
  U8_OUTPUT out; U8_INIT_STATIC_OUTPUT_BUF(out,len,buf);
  if (SYMBOLP(tag)) {
    const u8_byte *scan = SYM_NAME(tag); int c;
    while ((c = u8_sgetc(&scan))>=0)
      if ((c=='*') && (*scan=='\0')) break;
      else if (u8_isupper(c))
        u8_putc(&out,u8_tolower(c));
      else u8_putc(&out,c);}
  else if (STRINGP(tag)) {
    u8_puts(&out,CSTRING(tag));}
  else return NULL;
  return out.u8_outbuf;
}

/* XMLOUTPUT primitives */

FD_EXPORT int fd_xmlout_helper
(U8_OUTPUT *out,U8_OUTPUT *tmp,lispval x,
 lispval xmloidfn,fd_lexenv env)
{
  if (FD_ABORTP(x)) return 0;
  else if (VOIDP(x)) return 1;
  if (STRINGP(x))
    emit_xmlcontent(out,CSTRING(x));
  else if ((FD_APPLICABLEP(xmloidfn)) && (OIDP(x))) {
    lispval result = fd_apply(xmloidfn,1,&x);
    fd_decref(result);}
  else if (OIDP(x))
    if (fd_oid_test(x,xmltag_symbol,VOID))
      fd_xmleval(out,x,env);
    else fd_xmloid(out,x);
  else if ((SLOTMAPP(x)) &&
           (fd_slotmap_test((fd_slotmap)x,xmltag_symbol,VOID)))
    fd_xmleval(out,x,env);
  else {
    U8_OUTPUT _out; u8_byte buf[128];
    if (tmp == NULL) {
      U8_INIT_STATIC_OUTPUT_BUF(_out,64,buf); tmp = &_out;}
    tmp->u8_write = tmp->u8_outbuf;
    fd_unparse(tmp,x);
    /* if (OIDP(x)) output_oid(tmp,x); else {} */
    emit_xmlcontent(out,tmp->u8_outbuf);}
  return 1;
}

static lispval xmlout_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval body = fd_get_body(expr,1);
  U8_OUTPUT *out = u8_current_output, tmpout;
  lispval xmloidfn = fd_symeval(xmloidfn_symbol,env);
  u8_byte buf[128];
  U8_INIT_STATIC_OUTPUT_BUF(tmpout,128,buf);
  while (PAIRP(body)) {
    lispval value = fast_eval(FD_CAR(body),env);
    if (FD_ABORTP(value)) {
      fd_decref(xmloidfn);
      return value;}
    else if (fd_xmlout_helper(out,&tmpout,value,xmloidfn,env))
      fd_decref(value);
    else return value;
    body = FD_CDR(body);}
  u8_flush(out);
  if (tmpout.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(tmpout.u8_outbuf);
  fd_decref(xmloidfn);
  return VOID;
}

FD_EXPORT int fd_lisp2xml(u8_output out,lispval x,fd_lexenv env)
{
  int retval = -1;
  lispval xmloidfn = fd_symeval(xmloidfn_symbol,env);
  if (out == NULL) out = u8_current_output;
  retval = fd_xmlout_helper(out,NULL,x,xmloidfn,env);
  fd_decref(xmloidfn);
  return retval;
}

static lispval raw_xhtml_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval body = fd_get_body(expr,1);
  U8_OUTPUT *out = u8_current_output, tmpout;
  lispval xmloidfn = fd_symeval(xmloidfn_symbol,env);
  u8_byte buf[128];
  U8_INIT_STATIC_OUTPUT_BUF(tmpout,128,buf);
  while (PAIRP(body)) {
    lispval value = fast_eval(FD_CAR(body),env);
    if (FD_ABORTP(value)) {
      fd_decref(xmloidfn);
      return value;}
    else if (STRINGP(value))
      u8_putn(out,CSTRING(value),STRLEN(value));
    else if ((VOIDP(value)) || (EMPTYP(value))) {}
    else u8_printf(out,"%q",value);
    fd_decref(value);
    body = FD_CDR(body);}
  u8_flush(out);
  if (tmpout.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(tmpout.u8_outbuf);
  fd_decref(xmloidfn);
  return VOID;
}

static lispval nbsp_prim()
{
  U8_OUTPUT *out = u8_current_output;
  u8_puts(out,"&nbsp;");
  return VOID;
}

static lispval xmlemptyelt(int n,lispval *args)
{
  U8_OUTPUT *out = u8_current_output;
  lispval eltname = args[0];
  const u8_byte *tagname;
  u8_byte tagbuf[128];
  int i = 1;
  tagname = get_tagname(eltname,tagbuf,128);
  if (tagname) {
    u8_putc(out,'<');
    emit_xmlname(out,tagname);
    if (tagname!=tagbuf) u8_free(tagname);}
  else return fd_err(fd_TypeError,"xmlemptyelt",_("invalid XML element name"),eltname);
  while (i<n) {
    lispval elt = args[i];
    u8_putc(out,' ');
    if (STRINGP(elt)) {
      entify(out,CSTRING(elt)); i++;}
    else if (SYMBOLP(elt))
      if (i+1<n) {
        lispval val = args[i+1];
        if (!(EMPTYP(val)))
          emit_xmlattrib(out,NULL,SYM_NAME(elt),val,1);
        i = i+2;}
      else return fd_err(fd_SyntaxError,"xmlemptyelt",_("odd number of arguments"),elt);
    else return fd_err(fd_SyntaxError,"xmlemptyelt",_("invalid XML attribute name"),elt);}
  u8_puts(out,"/>");
  return VOID;
}

static lispval xmlentry_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  U8_OUTPUT *out = u8_current_output;
  lispval head = fd_get_arg(expr,1), args = FD_CDR(FD_CDR(expr));
  u8_byte tagbuf[128]; u8_string tagname;
  if ((PAIRP(head)))  head = fd_eval(head,env);
  else head = fd_incref(head);
  if (FD_ABORTED(head)) return head;
  tagname = get_tagname(head,tagbuf,128);
  if (tagname == NULL) {
    fd_decref(head);
    return fd_err(fd_SyntaxError,"xmlentry",NULL,expr);}
  else if (open_markup(out,NULL,tagname,args,env,1)<0) {
    fd_decref(head);
    u8_flush(out);
    return FD_ERROR;}
  else {
    fd_decref(head);
    u8_flush(out);
    return VOID;}
}

static lispval xmlstart_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  U8_OUTPUT *out = u8_current_output;
  lispval head = fd_get_arg(expr,1), args = FD_CDR(FD_CDR(expr));
  u8_byte tagbuf[128]; u8_string tagname;
  if ((PAIRP(head)))  head = fd_eval(head,env);
  else head = fd_incref(head);
  if (FD_ABORTED(head)) return head;
  tagname = get_tagname(head,tagbuf,128);
  if (tagname == NULL) {
    fd_decref(head);
    return fd_err(fd_SyntaxError,"xmlentry",NULL,expr);}
  else if (open_markup(out,NULL,tagname,args,env,0)<0) {
    fd_decref(head);
    u8_flush(out);
    return FD_ERROR;}
  else {
    fd_decref(head);
    u8_flush(out);
    return VOID;}
}

static lispval xmlend_prim(lispval head)
{
  U8_OUTPUT *out = u8_current_output;
  u8_byte tagbuf[128]; u8_string tagname;
  tagname = get_tagname(head,tagbuf,128);
  if (tagname == NULL) {
    fd_decref(head);
    return fd_err(fd_SyntaxError,"xmlend",NULL,head);}
  else u8_printf(out,"</%s>",tagname);
  return VOID;
}

static lispval doxmlblock(lispval expr,fd_lexenv env,
                         fd_stack _stack,int newline)
{
  lispval tagspec = fd_get_arg(expr,1), attribs, body;
  lispval xmloidfn = fd_symeval(xmloidfn_symbol,env);
  u8_byte tagbuf[128], buf[128];
  u8_string tagname; int eval_attribs = 0;
  U8_OUTPUT *out, tmpout;
  if (SYMBOLP(tagspec)) {
    attribs = fd_get_arg(expr,2); body = fd_get_body(expr,3);
    eval_attribs = 1;}
  else if (STRINGP(tagspec)) {
    attribs = fd_get_arg(expr,2); body = fd_get_body(expr,3);
    fd_incref(tagspec); eval_attribs = 1;}
  else {
    body = fd_get_body(expr,2);
    tagspec = fd_eval(tagspec,env);
    if (FD_ABORTED(tagspec)) {
      fd_decref(xmloidfn);
      return tagspec;}
    else if (SYMBOLP(tagspec)) attribs = NIL;
    else if (STRINGP(tagspec)) attribs = NIL;
    else if (PAIRP(tagspec)) {
      lispval name = FD_CAR(tagspec); attribs = fd_incref(FD_CDR(tagspec));
      if (SYMBOLP(name)) {}
      else if (STRINGP(name)) fd_incref(name);
      else {
        fd_decref(xmloidfn);
        return fd_err(fd_SyntaxError,"xmlblock",NULL,tagspec);}
      fd_decref(tagspec); tagspec = name;}
    else {
      fd_decref(xmloidfn);
      return fd_err(fd_SyntaxError,"xmlblock",NULL,tagspec);}}
  tagname = get_tagname(tagspec,tagbuf,128);
  if (tagname == NULL) {
    fd_decref(xmloidfn);
    return fd_err(fd_SyntaxError,"xmlblock",NULL,expr);}
  out = u8_current_output;
  U8_INIT_STATIC_OUTPUT_BUF(tmpout,128,buf);
  if (open_markup(out,&tmpout,tagname,attribs,
                  ((eval_attribs)?(env):(NULL)),0)<0) {
    fd_decref(xmloidfn);
    return FD_ERROR;}
  if (newline) u8_putc(out,'\n');
  while (PAIRP(body)) {
    lispval value = fast_eval(FD_CAR(body),env);
    if (FD_ABORTED(value)) {
      fd_decref(xmloidfn);
      close_markup(out,tagname);
      return value;}
    else if (fd_xmlout_helper(out,&tmpout,value,xmloidfn,env))
      fd_decref(value);
    else {
      fd_decref(xmloidfn);
      return value;}
    body = FD_CDR(body);}
  if (newline) u8_putc(out,'\n');
  if (close_markup(out,tagname)<0) {
    fd_decref(xmloidfn);
    return FD_ERROR;}
  if (tagname!=tagbuf) u8_free(tagname);
  u8_flush(out);
  fd_decref(xmloidfn);
  if (tmpout.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(tmpout.u8_outbuf);
  return VOID;
}

/* Does a block without wrapping content in newlines */
static lispval xmlblock_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  return doxmlblock(expr,env,_stack,0);
}
/* Does a block and wraps content in newlines */
static lispval xmlblockn_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  return doxmlblock(expr,env,_stack,1);
}

static lispval handle_markup(lispval expr,fd_lexenv env,fd_stack _stack,
                            int star,int block)
{
  if ((PAIRP(expr)) && (SYMBOLP(FD_CAR(expr)))) {
    lispval attribs = fd_get_arg(expr,1), body = fd_get_body(expr,2);
    lispval xmloidfn = fd_symeval(xmloidfn_symbol,env);
    U8_OUTPUT *out = u8_current_output, tmpout;
    u8_byte tagbuf[128], buf[128];
    u8_string tagname;
    U8_INIT_STATIC_OUTPUT_BUF(tmpout,128,buf);
    if (star) {
      attribs = fd_get_arg(expr,1); body = fd_get_body(expr,2);}
    else {attribs = NIL; body = fd_get_body(expr,1);}
    tagname = get_tagname(FD_CAR(expr),tagbuf,128);
    if (tagname == NULL) {
      fd_decref(xmloidfn);
      return fd_err(fd_SyntaxError,"handle_markup",NULL,expr);}
    if (block) u8_printf(out,"\n");
    if (open_markup(out,&tmpout,tagname,attribs,env,0)<0) {
      fd_decref(xmloidfn);
      return FD_ERROR;}
    if (block) u8_printf(out,"\n");
    while (PAIRP(body)) {
      lispval value = fast_eval(FD_CAR(body),env);
      if (FD_ABORTED(value)) {
        close_markup(out,tagname);
        if (block) u8_printf(out,"\n");
        fd_decref(xmloidfn);
        return value;}
      else if (fd_xmlout_helper(out,&tmpout,value,xmloidfn,env))
        fd_decref(value);
      else {
        fd_decref(xmloidfn);
        return value;}
      body = FD_CDR(body);}
    if (block) u8_printf(out,"\n");
    if (close_markup(out,tagname)<0) {
      fd_decref(xmloidfn);
      return FD_ERROR;}
    if (block) u8_printf(out,"\n");
    if (tagname!=tagbuf) u8_free(tagname);
    u8_flush(out);
    if (tmpout.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(tmpout.u8_outbuf);
    fd_decref(xmloidfn);
    return VOID;}
  else return fd_err(fd_SyntaxError,"XML markup",NULL,fd_incref(expr));
}

static lispval markup_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  return handle_markup(expr,env,_stack,0,0);
}

static lispval markupblock_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  return handle_markup(expr,env,_stack,0,1);
}

static lispval markupstarblock_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  return handle_markup(expr,env,_stack,1,1);
}

static lispval markupstar_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  return handle_markup(expr,env,_stack,1,0);
}

static lispval emptymarkup_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  u8_byte tagbuf[128];
  U8_OUTPUT *out = u8_current_output;
  lispval head = FD_CAR(expr), args = FD_CDR(expr);
  u8_string tagname = get_tagname(head,tagbuf,128);
  if (tagname == NULL)
    return fd_err(fd_SyntaxError,"emptymarkup_evalfn",NULL,expr);
  else if (open_markup(out,NULL,tagname,args,env,1)<0)
    return FD_ERROR;
  else {
    u8_flush(out);
    return VOID;}
}

/* Getting oid display data */

static lispval global_browseinfo = EMPTY;
static u8_string default_browse_uri = NULL;
static u8_string default_browse_class = NULL;

static u8_mutex browseinfo_lock;

static lispval get_browseinfo(lispval arg)
{
  fd_pool p = fd_oid2pool(arg);
  if (p == NULL) return EMPTY;
  else {
    lispval pool = fd_pool2lisp(p), browseinfo = fd_thread_get(browseinfo_symbol), dflt = VOID;
    DO_CHOICES(info,browseinfo) {
      if ((VECTORP(info)) && (VEC_LEN(info)>0))
        if (FD_EQ(VEC_REF(info,0),pool)) {
          fd_incref(info); fd_decref(browseinfo);
          return info;}
        else if (FD_TRUEP(VEC_REF(info,0))) {
          dflt = info;}
        else {}
      else dflt = info;}
    if (VOIDP(dflt)) {
      u8_lock_mutex(&browseinfo_lock);
      {DO_CHOICES(info,global_browseinfo) {
          if ((VECTORP(info)) && (VEC_LEN(info)>0)) {
            if (FD_EQ(VEC_REF(info,0),pool)) {
              fd_incref(info);
              u8_unlock_mutex(&browseinfo_lock);
              return info;}
            else if (FD_TRUEP(VEC_REF(info,0)))
              dflt = info;}}
        fd_incref(dflt);
        u8_unlock_mutex(&browseinfo_lock);
        if (VOIDP(dflt)) return EMPTY;
        else return dflt;}}
    else {
      fd_incref(dflt); fd_decref(browseinfo);
      return dflt;}}
}

static int unpack_browseinfo(lispval info,u8_string *baseuri,u8_string *classname,lispval *displayer)
{
  if ((EMPTYP(info)) || (VOIDP(info))) {
    if (*baseuri == NULL) {
      if (default_browse_uri)
        *baseuri = default_browse_uri;
      else *baseuri="browse.fdcgi?";}
    if (*classname == NULL) {
      if (default_browse_class)
        *classname = default_browse_class;
      else *classname="oid";}}
  else if (STRINGP(info)) {
    *baseuri = CSTRING(info);
    if (*classname == NULL) {
      if (default_browse_class)
        *classname = default_browse_class;
      else *classname="oid";}
    if (displayer) *displayer = VOID;}
  else if ((VECTORP(info)) && (VEC_LEN(info)>1)) {
    if (*classname == NULL)
      *classname = ((default_browse_class) ? (default_browse_class) : ((u8_string)"oid"));
    if (*baseuri == NULL)
      *baseuri = ((default_browse_uri) ? (default_browse_uri) : ((u8_string)"browse.fdcgi?"));
    switch (VEC_LEN(info)) {
    case 2:
      if (STRINGP(VEC_REF(info,1)))
        *baseuri = CSTRING(VEC_REF(info,1));
      else u8_log(LOG_WARN,fd_TypeError,"Bad browse info %q",info);
      break;
    case 3:
      if (STRINGP(VEC_REF(info,2)))
        *classname = CSTRING(VEC_REF(info,2));
      else u8_log(LOG_WARN,fd_TypeError,"Bad browse info %q",info);
      break;
    case 4:
      if (displayer) *displayer = VEC_REF(info,3);}}
  else {
    u8_log(LOG_WARN,fd_TypeError,"Bad browse info %q",info);
    *baseuri="browse.fdcgi?";}
  return 0;
}

static lispval browseinfo_config_get(lispval var,void *ignored)
{
  lispval result;
  u8_lock_mutex(&browseinfo_lock);
  result = global_browseinfo; fd_incref(result);
  u8_unlock_mutex(&browseinfo_lock);
  return result;
}

static int browseinfo_config_set(lispval var,lispval val,void *ignored)
{
  lispval new_browseinfo = EMPTY, old_browseinfo;
  u8_lock_mutex(&browseinfo_lock);
  old_browseinfo = global_browseinfo;
  if ((STRINGP(val)) || ((VECTORP(val)) && (VEC_LEN(val)>1))) {
    lispval target = VEC_REF(val,0);
    DO_CHOICES(info,old_browseinfo) {
      if ((VECTORP(info)) && (FD_EQ(target,VEC_REF(info,0)))) {}
      else if ((STRINGP(info)) && (FD_TRUEP(target))) {}
      else {
        fd_incref(info); CHOICE_ADD(new_browseinfo,info);}}
    fd_incref(val); CHOICE_ADD(new_browseinfo,val);
    global_browseinfo = fd_simplify_choice(new_browseinfo);
    u8_unlock_mutex(&browseinfo_lock);
    return 1;}
  else {
    u8_unlock_mutex(&browseinfo_lock);
    fd_seterr(fd_TypeError,"browse info",NULL,val);
    return -1;}
}

/* Doing anchor output */

static lispval doanchor_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  U8_OUTPUT *out = u8_current_output, tmpout;
  lispval target = fd_eval(fd_get_arg(expr,1),env), xmloidfn;
  lispval body = fd_get_body(expr,2);
  u8_byte buf[128]; U8_INIT_STATIC_OUTPUT_BUF(tmpout,128,buf);
  if (FD_ABORTED(target))
    return target;
  else if (VOIDP(target))
    return fd_err(fd_SyntaxError,"doanchor",NULL,VOID);
  else if (NILP(body))
    return fd_err(fd_SyntaxError,"doanchor",NULL,VOID);
  if (STRINGP(target)) {
    u8_printf(out,"<a href='");
    attrib_entify(out,CSTRING(target));
    u8_puts(out,"'>");}
  else if (SYMBOLP(target)) {
    u8_printf(out,"<a href='#");
    attrib_entify(out,SYM_NAME(target));
    u8_printf(out,"'>");}
  else if (OIDP(target)) {
    FD_OID addr = FD_OID_ADDR(target);
    lispval browseinfo = get_browseinfo(target);
    u8_string uri = NULL, class = NULL;
    unpack_browseinfo(browseinfo,&uri,&class,NULL);
    u8_printf(out,"<a href='%s:@%x/%x' class='%s'>",
              uri,FD_OID_HI(addr),FD_OID_LO(addr),class);
    fd_decref(browseinfo);}
  else {
    return fd_type_error(_("valid anchor target"),"doanchor",target);}
  xmloidfn = fd_symeval(xmloidfn_symbol,env);
  while (PAIRP(body)) {
    lispval value = fast_eval(FD_CAR(body),env);
    if (FD_ABORTED(value)) {
      fd_decref(xmloidfn); fd_decref(target);
      return value;}
    else if (fd_xmlout_helper(out,&tmpout,value,xmloidfn,env))
      fd_decref(value);
    else {
      fd_decref(xmloidfn); fd_decref(target);
      return value;}
    body = FD_CDR(body);}
  u8_printf(out,"</a>");
  u8_flush(out);
  fd_decref(xmloidfn); fd_decref(target);
  if (tmpout.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(tmpout.u8_outbuf);
  return VOID;
}

static int has_class_attrib(lispval attribs)
{
  lispval scan = attribs;
  while (PAIRP(scan))
    if (FD_EQ(FD_CAR(scan),class_symbol)) return 1;
    else if ((PAIRP(FD_CAR(scan))) &&
             (FD_EQ(FD_CAR(FD_CAR(scan)),class_symbol)))
      return 1;
    else {
      scan = FD_CDR(scan);
      if (PAIRP(scan)) scan = FD_CDR(scan);}
  return 0;
}

static lispval doanchor_star_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  U8_OUTPUT *out = u8_current_output, tmpout;
  lispval target = fd_eval(fd_get_arg(expr,1),env), xmloidfn = VOID;
  lispval attribs = fd_get_arg(expr,2);
  lispval body = fd_get_body(expr,3);
  u8_byte buf[128]; U8_INIT_STATIC_OUTPUT_BUF(tmpout,128,buf);
  if (FD_ABORTED(target))
    return target;
  else if (VOIDP(target))
    return fd_err(fd_SyntaxError,"doanchor",NULL,VOID);
  else if (NILP(body))
    return fd_err(fd_SyntaxError,"doanchor",NULL,VOID);
  if (STRINGP(target))
    attribs = fd_conspair(href_symbol,fd_conspair(fd_incref(target),fd_incref(attribs)));
  else if (SYMBOLP(target)) {
    tmpout.u8_write = tmpout.u8_outbuf;
    u8_printf(out,"#%s",SYM_NAME(target));
    attribs = fd_conspair(href_symbol,
                        fd_conspair(fd_stream2string(&tmpout),fd_incref(attribs)));}
  else if (OIDP(target)) {
    FD_OID addr = FD_OID_ADDR(target);
    lispval browseinfo = get_browseinfo(target);
    u8_string uri = NULL, class = NULL;
    unpack_browseinfo(browseinfo,&uri,&class,NULL);
    if (has_class_attrib(attribs))
      fd_incref(attribs);
    else attribs = fd_conspair(fd_intern("CLASS"),
                             fd_conspair(lispval_string(class),fd_incref(attribs)));
    tmpout.u8_write = tmpout.u8_outbuf;
    u8_printf(&tmpout,"%s:@%x/%x",uri,FD_OID_HI(addr),(FD_OID_LO(addr)));
    attribs = fd_conspair
      (href_symbol,
       fd_conspair(fd_substring(tmpout.u8_outbuf,tmpout.u8_write),
                   attribs));
    fd_decref(browseinfo);}
  else return fd_type_error(_("valid anchor target"),"doanchor_star",target);
  xmloidfn = fd_symeval(xmloidfn_symbol,env);
  if (open_markup(out,&tmpout,"a",attribs,env,0)<0) {
    fd_decref(attribs); fd_decref(xmloidfn); fd_decref(target);
    return FD_ERROR;}
  while (PAIRP(body)) {
    lispval value = fast_eval(FD_CAR(body),env);
    if (FD_ABORTED(value)) {
      fd_decref(attribs); fd_decref(xmloidfn); fd_decref(target);
      return value;}
    else if (fd_xmlout_helper(out,&tmpout,value,xmloidfn,env))
      fd_decref(value);
    else {
      fd_decref(attribs); fd_decref(xmloidfn); fd_decref(target);
      return value;}
    body = FD_CDR(body);}
  u8_printf(out,"</a>");
  u8_flush(out);
  fd_decref(attribs); fd_decref(xmloidfn); fd_decref(target);
  if (tmpout.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(tmpout.u8_outbuf);
  return VOID;
}

FD_EXPORT void fd_xmloid(u8_output out,lispval arg)
{
  FD_OID addr = FD_OID_ADDR(arg);
  lispval browseinfo = get_browseinfo(arg), name, displayer = VOID;
  u8_string uri = NULL, class = NULL;
  unpack_browseinfo(browseinfo,&uri,&class,&displayer);
  if (out == NULL) out = u8_current_output;
  u8_printf(out,"<a class='%s' href='%s?:@%x/%x'>",
            class,uri,FD_OID_HI(addr),FD_OID_LO(addr));
  if ((OIDP(displayer)) || (SYMBOLP(displayer)))
    name = fd_frame_get(arg,displayer);
  else if (FD_APPLICABLEP(displayer))
    name = fd_apply(displayer,1,&arg);
  else name = fd_frame_get(arg,obj_name);
  if (EMPTYP(name))
    u8_printf(out,"%q",arg);
  else if (VOIDP(name)) {}
  else fd_xmlout_helper(out,NULL,name,VOID,NULL);
  fd_decref(name);
  u8_printf(out,"</a>");
  fd_decref(browseinfo);
}

static lispval xmloid(lispval oid_arg)
{
  fd_xmloid(NULL,oid_arg);
  return VOID;
}

/* XMLEVAL primitives */

static lispval xmleval_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval xmlarg = fd_get_arg(expr,1);
  if (VOIDP(xmlarg))
    return fd_err(fd_SyntaxError,"xmleval_evalfn",NULL,VOID);
  else {
    U8_OUTPUT *out = u8_current_output;
    if (STRINGP(xmlarg)) {
      u8_string data = CSTRING(xmlarg);
      if (data[0]=='<') u8_putn(out,data,STRLEN(xmlarg));
      else emit_xmlcontent(out,data);
      return VOID;}
    else {
      lispval xml = fd_eval(xmlarg,env);
      if (FD_ABORTED(xml)) return xml;
      lispval env_arg = fd_eval(fd_get_arg(expr,2),env);
      if (FD_ABORTED(env_arg)) { fd_decref(xml); return env_arg;}
      lispval xml_env_arg = fd_eval(fd_get_arg(expr,3),env);
      if (FD_ABORTED(xml_env_arg)) {
        fd_decref(env_arg);
        fd_decref(xml);
        return xml_env_arg;}
      if (!((VOIDP(env_arg)) || (FALSEP(env_arg)) ||
            (FD_TRUEP(env_arg)) || (FD_LEXENVP(env_arg)) ||
            (TABLEP(env_arg)))) {
        lispval err = fd_type_error("SCHEME environment","xmleval_evalfn",env_arg);
        fd_decref(xml);
        fd_decref(env_arg);
        fd_decref(xml_env_arg);
        return err;}
      else if (!((VOIDP(xml_env_arg)) || (FALSEP(xml_env_arg)) ||
                 (FD_LEXENVP(xml_env_arg)) || (TABLEP(xml_env_arg)))) {
        fd_decref(xml);
        fd_decref(env_arg);
        return fd_type_error("environment","xmleval_evalfn",xml_env_arg);}
      else {
        lispval result = fd_xmleval_with(out,xml,env_arg,xml_env_arg);
        fd_decref(xml);
        fd_decref(env_arg);
        fd_decref(xml_env_arg);
        return result;}
    }
  }
}

static lispval xml2string_prim(lispval xml,lispval env_arg,lispval xml_env_arg)
{
  if (!((VOIDP(env_arg)) || (FALSEP(env_arg)) ||
        (FD_TRUEP(env_arg)) || (FD_LEXENVP(env_arg)) ||
        (TABLEP(env_arg)))) {
    return fd_type_error("SCHEME environment","xmleval_evalfn",env_arg);}
  else if (!((VOIDP(xml_env_arg)) || (FALSEP(xml_env_arg)) ||
               (FD_LEXENVP(xml_env_arg)) || (TABLEP(xml_env_arg)))) {
    return fd_type_error("environment","xmleval_evalfn",xml_env_arg);}
  if (STRINGP(xml)) {
    lispval parsed = fd_fdxml_arg(xml);
    lispval result = xml2string_prim(parsed,env_arg,xml_env_arg);
    fd_decref(parsed);
    return result;}
  else {
    U8_OUTPUT out; char buf[1024];
    U8_INIT_OUTPUT_BUF(&out,1024,buf);
    lispval result = fd_xmleval_with(&out,xml,env_arg,xml_env_arg);
    fd_decref(result);
    return fd_stream2string(&out);}
}

static lispval xmlopen_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  if (!(PAIRP(FD_CDR(expr))))
    return fd_err(fd_SyntaxError,"xmleval_evalfn",NULL,VOID);
  else {
    lispval node = fd_eval(FD_CADR(expr),env);
    if (FD_ABORTED(node))
      return node;
    else if (TABLEP(node)) {
      lispval result = fd_open_xml(node,env);
      fd_decref(node);
      return result;}
    else return VOID;}
}

static lispval xmlclose_prim(lispval arg)
{
  if (!(TABLEP(arg)))
    return fd_type_error("XML node","xmlclose_prim",arg);
  else {
    lispval tag = fd_close_xml(arg);
    fd_decref(tag);
    return VOID;}
}

/* Javascript output */

/* This generates a javascript function call, transforming the
   arguments into strings.  This uses double quotes to quote the
   arguments because the output is typically inserted in attributes
   which are single quoted. */
static lispval output_javascript(u8_output out,lispval args,fd_lexenv env)
{
  if (NILP(args))
    return fd_err(fd_SyntaxError,"output_javascript",NULL,args);
  else {
    int i = 0;
    lispval head_expr = FD_CAR(args), head = fd_eval(head_expr,env), body = FD_CDR(args);
    if (FD_ABORTED(head))
      return head;
    else if (!(STRINGP(head)))
      return fd_type_error(_("javascript function name"),
                           "output_javascript",head);
    else u8_printf(out,"%s(",CSTRING(head));
    {FD_DOELTS(elt,body,count) {
        lispval val;
        if (i>0) u8_putc(out,',');
        i++;
        if (FD_NEED_EVALP(elt))
          val = fd_eval(elt,env);
        else val = fd_incref(elt);
        if (FD_ABORTED(val))
          return val;
        else if (VOIDP(val)) {}
        else if (FIXNUMP(val))
          u8_printf(out,"%lld",FIX2INT(val));
        else if (FD_FLONUMP(val))
          u8_printf(out,"%f",FD_FLONUM(val));
        else if (STRINGP(val)) {
          const u8_byte *scan = CSTRING(val);
          u8_putc(out,'"');
          while (*scan) {
            int c = u8_sgetc(&scan);
            if (c=='"') {u8_putc(out,'\\'); u8_putc(out,'"');}
            else u8_putc(out,c);}
          u8_putc(out,'"');}
        else if (OIDP(val))
          u8_printf(out,"\":@%x/%x\"",
                    FD_OID_HI(FD_OID_ADDR(val)),
                    FD_OID_LO(FD_OID_ADDR(val)));
        else {
          U8_OUTPUT tmp; u8_byte buf[128]; const u8_byte *scan;
          U8_INIT_STATIC_OUTPUT_BUF(tmp,128,buf);
          tmp.u8_streaminfo = tmp.u8_streaminfo|U8_STREAM_TACITURN;
          u8_puts(out,"\":");
          fd_unparse(&tmp,val);
          scan = tmp.u8_outbuf; while (scan<tmp.u8_write) {
            int c = u8_sgetc(&scan);
            if (c<0) break;
            else if (c=='\\') {
              c = u8_sgetc(&scan);
              if (c<0) {u8_putc(out,'\\'); break;}
              else {u8_putc(out,'\\'); u8_putc(out,c);}}
            else if (c=='"') {
              u8_putc(out,'\\'); u8_putc(out,c);}
            else u8_putc(out,c);}
          if (tmp.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(tmp.u8_outbuf);
          u8_puts(out,"\"");}
        fd_decref(val);}}
    u8_putc(out,')');
    fd_decref(head);
    return VOID;}
}

static lispval javascript_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval retval; struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,256);
  retval = output_javascript(&out,FD_CDR(expr),env);
  if (VOIDP(retval))
    return fd_stream2string(&out);
  else {
    u8_free(out.u8_outbuf); return retval;}
}

static lispval javastmt_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval retval; struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,256);
  retval = output_javascript(&out,FD_CDR(expr),env);
  if (VOIDP(retval)) {
    u8_putc(&out,';');
    return fd_stream2string(&out);}
  else {
    u8_free(out.u8_outbuf); return retval;}
}

/* Soap envelope generation */

/* This should probably be customizable */

static u8_string soapenvopen=
  "<SOAP-ENV:Envelope xmlns:SE='http://www.w3.org/2003/05/soap-envelope' xmlns:xsd='http://www.w3.org/2001/XMLSchema' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'>\n";
static u8_string soapenvclose="\n</SOAP-ENV:Envelope>";
static u8_string soapbodyopen="<SOAP-ENV:Body>";
static u8_string soapbodyclose="</SOAP-ENV:Body>";
static u8_string soapheaderopen="  <SOAP-ENV:Header>\n";
static u8_string soapheaderclose="\n  </SOAP-ENV:Header>";

static lispval soapenvelope_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  U8_OUTPUT *out = u8_current_output;
  lispval header_arg = fd_get_arg(expr,1);
  lispval body = fd_get_body(expr,2);
  u8_puts(out,soapenvopen);
  if (FD_NEED_EVALP(header_arg)) {
    lispval value;
    u8_puts(out,soapheaderopen);
    value = fd_eval(header_arg,env);
    if (FD_ABORTED(value)) return value;
    if (STRINGP(value)) u8_puts(out,CSTRING(value));
    fd_decref(value);
    u8_puts(out,soapheaderclose);}
  u8_puts(out,soapbodyopen);
  fd_printout_to(out,body,env);
  u8_puts(out,soapbodyclose);
  u8_puts(out,soapenvclose);
  return VOID;
}

/* Initialization functions */

static u8_string markup_printf_handler
  (u8_output s,char *cmd,u8_byte *buf,int bufsiz,va_list *args)
{
  U8_STATIC_OUTPUT(vstream,1024);
  if (strchr(cmd,'l')) {
    lispval val = va_arg(*args,lispval);
    if (strstr(cmd,"ll"))
      fd_pprint(&vstream,val,NULL,0,0,80);
    else fd_unparse(&vstream,val);
    emit_xmlcontent(s,vstream.u8_outbuf);
    u8_close_output(&vstream);}
  else {
    u8_string str = va_arg(*args,u8_string);
    if (str) emit_xmlcontent(s,str);
    else emit_xmlcontent(s,"(null)");}
  return NULL;
}

FD_EXPORT void fd_init_xmloutput_c()
{
  lispval fdweb_module = fd_new_module("FDWEB",(0));
  lispval safe_fdweb_module = fd_new_module("FDWEB",(FD_MODULE_SAFE));
  lispval xhtml_module = fd_new_module("XHTML",FD_MODULE_SAFE);

  lispval markup_prim = fd_make_evalfn("markup",markup_evalfn);
  lispval markupstar_prim = fd_make_evalfn("markup*",markupstar_evalfn);
  lispval markupblock_prim=
    fd_make_evalfn("markupblock",markupblock_evalfn);
  lispval markupstarblock_prim=
    fd_make_evalfn("markup*block",markupstarblock_evalfn);
  lispval emptymarkup_prim=
    fd_make_evalfn("emptymarkup",emptymarkup_evalfn);
  lispval xmlout_prim = fd_make_evalfn("XMLOUT",xmlout_evalfn);
  lispval xmlblock_prim = fd_make_evalfn("XMLBLOCK",xmlblock_evalfn);
  lispval xmlblockn_prim = fd_make_evalfn("XMLBLOCKN",xmlblockn_evalfn);
  lispval xmlelt_prim = fd_make_evalfn("XMLELT",xmlentry_evalfn);

  /* Applicable XML generators (not evalfns) */
  lispval xmlempty_dproc = fd_make_cprimn("XMLEMPTY",xmlemptyelt,0);
  lispval xmlempty_proc = fd_make_ndprim(xmlempty_dproc);
  lispval xmlify_proc = fd_make_cprim1("XMLIFY",xmlify,1);

  u8_printf_handlers['k']=markup_printf_handler;

  fd_store(safe_fdweb_module,fd_intern("XMLOUT"),xmlout_prim);
  fd_store(safe_fdweb_module,fd_intern("XMLBLOCK"),xmlblock_prim);
  fd_store(safe_fdweb_module,fd_intern("XMLBLOCKN"),xmlblockn_prim);
  fd_store(safe_fdweb_module,fd_intern("XMLELT"),xmlelt_prim);
  fd_defn(safe_fdweb_module,xmlempty_proc);
  fd_defn(safe_fdweb_module,xmlify_proc);
  fd_store(safe_fdweb_module,fd_intern("MARKUPFN"),markup_prim);
  fd_store(safe_fdweb_module,fd_intern("MARKUP*FN"),markupstar_prim);
  fd_store(safe_fdweb_module,fd_intern("BLOCKMARKUPFN"),markupblock_prim);
  fd_store(safe_fdweb_module,fd_intern("BLOCKMARKUP*FN"),markupstarblock_prim);
  fd_store(safe_fdweb_module,fd_intern("EMPTYMARKUPFN"),emptymarkup_prim);
  fd_def_evalfn(safe_fdweb_module,"SOAPENVELOPE","",soapenvelope_evalfn);
  fd_defn(safe_fdweb_module,fd_make_cprim3("XML->STRING",xml2string_prim,1));

  fd_store(fdweb_module,fd_intern("XMLOUT"),xmlout_prim);
  fd_store(fdweb_module,fd_intern("XMLBLOCK"),xmlblock_prim);
  fd_store(fdweb_module,fd_intern("XMLBLOCKN"),xmlblockn_prim);
  fd_store(fdweb_module,fd_intern("XMLELT"),xmlelt_prim);
  fd_defn(fdweb_module,xmlempty_proc);
  fd_defn(fdweb_module,xmlify_proc);
  fd_store(fdweb_module,fd_intern("MARKUPFN"),markup_prim);
  fd_store(fdweb_module,fd_intern("MARKUP*FN"),markupstar_prim);
  fd_store(fdweb_module,fd_intern("BLOCKMARKUPFN"),markupblock_prim);
  fd_store(fdweb_module,fd_intern("BLOCKMARKUP*FN"),markupstarblock_prim);
  fd_store(fdweb_module,fd_intern("EMPTYMARKUPFN"),emptymarkup_prim);
  fd_def_evalfn(fdweb_module,"SOAPENVELOPE","",soapenvelope_evalfn);
  fd_defn(fdweb_module,fd_make_cprim3("XML->STRING",xml2string_prim,1));

  fd_def_evalfn(xhtml_module,"ANCHOR","",doanchor_evalfn);
  fd_def_evalfn(xhtml_module,"ANCHOR*","",doanchor_star_evalfn);
  fd_idefn(xhtml_module,fd_make_cprim1("%XMLOID",xmloid,1));

  fd_def_evalfn(xhtml_module,"XHTML","",raw_xhtml_evalfn);
  fd_idefn(xhtml_module,fd_make_cprim0("NBSP",nbsp_prim));

  fd_store(xhtml_module,fd_intern("DIV"),markupstarblock_prim);
  fd_store(xhtml_module,fd_intern("SPAN"),markupstar_prim);

  fd_store(xhtml_module,fd_intern("HGROUP"),markupblock_prim);
  fd_store(xhtml_module,fd_intern("HGROUP*"),markupstarblock_prim);

  fd_store(xhtml_module,fd_intern("P"),markupblock_prim);
  fd_store(xhtml_module,fd_intern("P*"),markupstarblock_prim);
  fd_store(xhtml_module,fd_intern("H1"),markupblock_prim);
  fd_store(xhtml_module,fd_intern("H1*"),markupstarblock_prim);
  fd_store(xhtml_module,fd_intern("H2"),markupblock_prim);
  fd_store(xhtml_module,fd_intern("H2*"),markupstarblock_prim);
  fd_store(xhtml_module,fd_intern("H3"),markupblock_prim);
  fd_store(xhtml_module,fd_intern("H3*"),markupstarblock_prim);
  fd_store(xhtml_module,fd_intern("H4"),markupblock_prim);
  fd_store(xhtml_module,fd_intern("H4*"),markupstarblock_prim);

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

  fd_def_evalfn(fdweb_module,"XMLEVAL","",xmleval_evalfn);
  fd_def_evalfn(safe_fdweb_module,"XMLEVAL","",xmleval_evalfn);
  fd_def_evalfn(fdweb_module,"XMLOPEN","",xmlopen_evalfn);
  fd_def_evalfn(safe_fdweb_module,"XMLOPEN","",xmlopen_evalfn);
  fd_def_evalfn(fdweb_module,"XMLSTART","",xmlstart_evalfn);
  fd_def_evalfn(safe_fdweb_module,"XMLSTART","",xmlstart_evalfn);
  {
    lispval xmlcloseprim=
      fd_make_cprim1("XMLCLOSE",xmlclose_prim,1);
    lispval xmlendprim=
      fd_make_cprim1("XMLEND",xmlend_prim,1);
    fd_defn(fdweb_module,xmlcloseprim);
    fd_idefn(safe_fdweb_module,xmlcloseprim);
    fd_defn(fdweb_module,xmlendprim);
    fd_idefn(safe_fdweb_module,xmlendprim);}

  /* Not strictly XML of course, but a neighbor */
  fd_def_evalfn(xhtml_module,"JAVASCRIPT","",javascript_evalfn);
  fd_def_evalfn(xhtml_module,"JAVASTMT","",javastmt_evalfn);

  fd_decref(markup_prim); fd_decref(markupstar_prim);
  fd_decref(markupblock_prim); fd_decref(markupstarblock_prim);
  fd_decref(emptymarkup_prim); fd_decref(xmlout_prim);
  fd_decref(xmlblockn_prim); fd_decref(xmlblock_prim);
  fd_decref(xmlempty_proc); fd_decref(xmlify_proc);
  fd_decref(xmlelt_prim);

  xmloidfn_symbol = fd_intern("%XMLOID");
  id_symbol = fd_intern("%ID");
  href_symbol = fd_intern("HREF");
  class_symbol = fd_intern("CLASS");
  obj_name = fd_intern("OBJ-NAME");
  quote_symbol = fd_intern("QUOTE");
  xmltag_symbol = fd_intern("%XMLTAG");
  rawtag_symbol = fd_intern("%RAWTAG");
  browseinfo_symbol = fd_intern("BROWSEINFO");
  embedded_symbol = fd_intern("%EMBEDDED");
  estylesheet_symbol = fd_intern("%ERRORSTYLE");
  modules_symbol = fd_intern("%MODULES");
  xml_env_symbol = fd_intern("%XMLENV");

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

  u8_init_mutex(&browseinfo_lock);

  u8_register_source_file(_FILEINFO);

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
