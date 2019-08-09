/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1
#define KNO_PROVIDE_FASTEVAL 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/frames.h"
#include "kno/tables.h"
#include "kno/eval.h"
#include "kno/ports.h"
#include "kno/webtools.h"
#include "kno/support.h"
#include "kno/cprims.h"

#include <ctype.h>

#define fast_eval(x,env) (kno_stack_eval(x,env,_stack,0))

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

KNO_INLINE_FCN void entify(u8_output out,u8_string value)
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

KNO_EXPORT
void kno_entify(u8_output out,u8_string value)
{
  entify(out,value);
}

KNO_EXPORT
void kno_attrib_entify(u8_output out,u8_string value)
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
    attrib_entify(out,KNO_PACKET_DATA(value));
  else if (SYMBOLP(value)) {
    u8_putc(out,':');
    attrib_entify(out,SYM_NAME(value));}
  else if (OIDP(value))
    u8_printf(out,":@%x/%x",
              KNO_OID_HI(KNO_OID_ADDR(value)),
              KNO_OID_LO(KNO_OID_ADDR(value)));
  else if (FIXNUMP(value))
    u8_printf(out,"%lld",FIX2INT(value));
  else if (KNO_FLONUMP(value))
    u8_printf(out,"%f",KNO_FLONUM(value));
  else if (tmp) {
    tmp->u8_write = tmp->u8_outbuf;
    tmp->u8_streaminfo = tmp->u8_streaminfo|U8_STREAM_TACITURN;
    kno_unparse(tmp,value);
    tmp->u8_streaminfo = tmp->u8_streaminfo&(~U8_STREAM_TACITURN);
    u8_puts(out,":");
    attrib_entify(out,tmp->u8_outbuf);}
  else {
    U8_OUTPUT tmp; u8_byte buf[128];
    U8_INIT_STATIC_OUTPUT_BUF(tmp,128,buf);
    tmp.u8_streaminfo = tmp.u8_streaminfo|U8_STREAM_TACITURN;
    kno_unparse(&tmp,value);
    u8_puts(out,":");
    attrib_entify(out,tmp.u8_outbuf);
    if (tmp.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(tmp.u8_outbuf);}
  u8_puts(out,"\"");
}

KNO_EXPORT void kno_emit_xmlattrib
(u8_output out,u8_output tmp,u8_string name,lispval value,int lower)
{
  return emit_xmlattrib(out,tmp,name,value,lower);
}

DEFPRIM1("xmlify",xmlify,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
         "`(XMLIFY *arg0*)` **undocumented**",
         kno_any_type,KNO_VOID);
static lispval xmlify(lispval value)
{
  if (STRINGP(value))
    return value;
  else if (OIDP(value)) {
    U8_OUTPUT tmp; U8_INIT_OUTPUT(&tmp,32);
    u8_printf(&tmp,":%40%x%2f%x",
              KNO_OID_HI(KNO_OID_ADDR(value)),
              KNO_OID_LO(KNO_OID_ADDR(value)));
    return kno_init_string(NULL,tmp.u8_write-tmp.u8_outbuf,tmp.u8_outbuf);}
  else {
    U8_OUTPUT tmp; U8_INIT_OUTPUT(&tmp,32);
    tmp.u8_streaminfo = tmp.u8_streaminfo|U8_STREAM_TACITURN;
    u8_putc(&tmp,':'); kno_unparse(&tmp,value);
    return kno_init_string(NULL,tmp.u8_write-tmp.u8_outbuf,tmp.u8_outbuf);}
}

static U8_MAYBE_UNUSED lispval oidunxmlify(lispval string)
{
  u8_string s = CSTRING(string), addr_start = strchr(s,'_');
  KNO_OID addr; unsigned int hi, lo;
  if (addr_start) sscanf(addr_start,"_%x_%x",&hi,&lo);
  else return EMPTY;
  memset(&addr,0,sizeof(KNO_OID));
  KNO_SET_OID_HI(addr,hi); KNO_SET_OID_LO(addr,lo);
  return kno_make_oid(addr);
}

static void emit_xmlcontent(u8_output out,u8_string content)
{
  entify(out,content);
}

KNO_EXPORT void kno_emit_xmlcontent(u8_output out,u8_string content)
{
  entify(out,content);
}

static int output_markup_attrib
(u8_output out,u8_output tmp,
 lispval name_expr,lispval value_expr,
 kno_lexenv env)
{
  u8_string attrib_name; lispval attrib_val;
  lispval free_name = VOID, free_value = VOID;
  if (SYMBOLP(name_expr)) attrib_name = SYM_NAME(name_expr);
  else if (STRINGP(name_expr)) attrib_name = CSTRING(name_expr);
  else if ((env) && (PAIRP(name_expr))) {
    free_name = kno_eval(name_expr,env);
    if (KNO_ABORTED(free_name)) return free_name;
    else if (SYMBOLP(free_name)) attrib_name = SYM_NAME(free_name);
    else if (STRINGP(free_name)) attrib_name = CSTRING(free_name);
    else attrib_name = NULL;}
  else attrib_name = NULL;
  if (attrib_name) {
    if ((env)&&(KNO_NEED_EVALP(value_expr))) {
      free_value = kno_eval(value_expr,env);
      if (KNO_ABORTED(free_value)) {
        kno_decref(free_name);
        return -1;}
      else if (KNO_DEFAULTP(free_value)) {
        kno_decref(free_name);
        return 0;}
      else attrib_val = free_value;}
    else if (KNO_DEFAULTP(value_expr)) {
      kno_decref(free_name);
      return 0;}
    else attrib_val = value_expr;}
  if (attrib_name) {
    if (VOIDP(value_expr)) {
      u8_putc(out,' '); attrib_entify(out,attrib_name);}
    else if (VOIDP(attrib_val)) {
      kno_decref(free_name); kno_decref(free_value);
      return 0;}
    else {
      emit_xmlattrib(out,tmp,attrib_name,attrib_val,SYMBOLP(name_expr));
      kno_decref(free_value);}}
  return 1;
}

static int open_markup(u8_output out,u8_output tmp,u8_string eltname,
                       lispval attribs,kno_lexenv env,int empty)
{
  u8_putc(out,'<');
  emit_xmlname(out,eltname);
  while (PAIRP(attribs)) {
    lispval elt = KNO_CAR(attribs);
    /* Kludge to handle case where the attribute name is quoted. */
    if ((PAIRP(elt)) && (KNO_CAR(elt) == quote_symbol) &&
        (PAIRP(KNO_CDR(elt))) && (SYMBOLP(KNO_CADR(elt))))
      elt = KNO_CADR(elt);
    if (STRINGP(elt)) {
      u8_putc(out,' ');
      attrib_entify(out,CSTRING(elt));
      attribs = KNO_CDR(attribs);}
    else if ((SYMBOLP(elt))&&(PAIRP(KNO_CDR(attribs))))
      if (output_markup_attrib(out,tmp,elt,KNO_CADR(attribs),env)>=0)
        attribs = KNO_CDR(KNO_CDR(attribs));
      else {
        if (empty) u8_puts(out,"/>"); else u8_puts(out,">");
        return -1;}
    else if (SYMBOLP(elt)) {
      u8_byte errbuf[150];
      if (empty) u8_puts(out,"/>"); else u8_puts(out,">");
      kno_seterr(kno_SyntaxError,"open_markup",
                 u8_sprintf(errbuf,150,
                            _("missing alternating attrib value for %s"),
                            SYM_NAME(elt)),
                 kno_incref(attribs));
      return -1;}
    else if ((PAIRP(elt))) {
      lispval val_expr = ((PAIRP(KNO_CDR(elt)))?(KNO_CADR(elt)):(VOID));
      if (output_markup_attrib(out,tmp,KNO_CAR(elt),val_expr,env)>=0)
        attribs = KNO_CDR(attribs);
      else {
        if (empty) u8_puts(out,"/>"); else u8_puts(out,">");
        return -1;}}
    else {
      u8_string details=kno_lisp2string(elt);
      if (empty) u8_puts(out,"/>"); else u8_puts(out,">");
      kno_seterr(kno_SyntaxError,"open_markup",
                 details,kno_incref(attribs));
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

KNO_EXPORT int kno_open_markup
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

KNO_EXPORT int kno_xmlout_helper
(U8_OUTPUT *out,U8_OUTPUT *tmp,lispval x,
 lispval xmloidfn,kno_lexenv env)
{
  if (KNO_ABORTP(x)) return 0;
  else if (VOIDP(x)) return 1;
  if (STRINGP(x))
    emit_xmlcontent(out,CSTRING(x));
  else if ((KNO_APPLICABLEP(xmloidfn)) && (OIDP(x))) {
    lispval result = kno_apply(xmloidfn,1,&x);
    kno_decref(result);}
  else if (OIDP(x))
    if (kno_oid_test(x,xmltag_symbol,VOID))
      kno_xmleval(out,x,env);
    else kno_xmloid(out,x);
  else if ((SLOTMAPP(x)) &&
           (kno_slotmap_test((kno_slotmap)x,xmltag_symbol,VOID)))
    kno_xmleval(out,x,env);
  else {
    U8_OUTPUT _out; u8_byte buf[128];
    if (tmp == NULL) {
      U8_INIT_STATIC_OUTPUT_BUF(_out,64,buf); tmp = &_out;}
    tmp->u8_write = tmp->u8_outbuf;
    kno_unparse(tmp,x);
    /* if (OIDP(x)) output_oid(tmp,x); else {} */
    emit_xmlcontent(out,tmp->u8_outbuf);}
  return 1;
}

static lispval xmlout_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval body = kno_get_body(expr,1);
  U8_OUTPUT *out = u8_current_output, tmpout;
  lispval xmloidfn = kno_symeval(xmloidfn_symbol,env);
  u8_byte buf[128];
  U8_INIT_STATIC_OUTPUT_BUF(tmpout,128,buf);
  while (PAIRP(body)) {
    lispval value = fast_eval(KNO_CAR(body),env);
    if (KNO_ABORTP(value)) {
      kno_decref(xmloidfn);
      return value;}
    else if (kno_xmlout_helper(out,&tmpout,value,xmloidfn,env))
      kno_decref(value);
    else return value;
    body = KNO_CDR(body);}
  u8_flush(out);
  if (tmpout.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(tmpout.u8_outbuf);
  kno_decref(xmloidfn);
  return VOID;
}

KNO_EXPORT int kno_lisp2xml(u8_output out,lispval x,kno_lexenv env)
{
  int retval = -1;
  lispval xmloidfn = kno_symeval(xmloidfn_symbol,env);
  if (out == NULL) out = u8_current_output;
  retval = kno_xmlout_helper(out,NULL,x,xmloidfn,env);
  kno_decref(xmloidfn);
  return retval;
}

static lispval raw_xhtml_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval body = kno_get_body(expr,1);
  U8_OUTPUT *out = u8_current_output, tmpout;
  lispval xmloidfn = kno_symeval(xmloidfn_symbol,env);
  u8_byte buf[128];
  U8_INIT_STATIC_OUTPUT_BUF(tmpout,128,buf);
  while (PAIRP(body)) {
    lispval value = fast_eval(KNO_CAR(body),env);
    if (KNO_ABORTP(value)) {
      kno_decref(xmloidfn);
      return value;}
    else if (STRINGP(value))
      u8_putn(out,CSTRING(value),STRLEN(value));
    else if ((VOIDP(value)) || (EMPTYP(value))) {}
    else u8_printf(out,"%q",value);
    kno_decref(value);
    body = KNO_CDR(body);}
  u8_flush(out);
  if (tmpout.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(tmpout.u8_outbuf);
  kno_decref(xmloidfn);
  return VOID;
}

DEFPRIM("nbsp",nbsp_prim,KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
        "`(NBSP)` **undocumented**");
static lispval nbsp_prim()
{
  U8_OUTPUT *out = u8_current_output;
  u8_puts(out,"&nbsp;");
  return VOID;
}

DEFPRIM("xmlempty",xmlemptyelt,KNO_VAR_ARGS|KNO_MIN_ARGS(0)|KNO_NDOP,
        "`(XMLEMPTY *args...*)` **undocumented**");
static lispval xmlemptyelt(int n,kno_argvec args)
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
  else return kno_err(kno_TypeError,"xmlemptyelt",_("invalid XML element name"),eltname);
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
      else return kno_err(kno_SyntaxError,"xmlemptyelt",_("odd number of arguments"),elt);
    else return kno_err(kno_SyntaxError,"xmlemptyelt",_("invalid XML attribute name"),elt);}
  u8_puts(out,"/>");
  return VOID;
}

static lispval xmlentry_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  U8_OUTPUT *out = u8_current_output;
  lispval head = kno_get_arg(expr,1), args = KNO_CDR(KNO_CDR(expr));
  u8_byte tagbuf[128]; u8_string tagname;
  if ((PAIRP(head)))  head = kno_eval(head,env);
  else head = kno_incref(head);
  if (KNO_ABORTED(head)) return head;
  tagname = get_tagname(head,tagbuf,128);
  if (tagname == NULL) {
    kno_decref(head);
    return kno_err(kno_SyntaxError,"xmlentry",NULL,expr);}
  else if (open_markup(out,NULL,tagname,args,env,1)<0) {
    kno_decref(head);
    u8_flush(out);
    return KNO_ERROR;}
  else {
    kno_decref(head);
    u8_flush(out);
    return VOID;}
}

static lispval xmlstart_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  U8_OUTPUT *out = u8_current_output;
  lispval head = kno_get_arg(expr,1), args = KNO_CDR(KNO_CDR(expr));
  u8_byte tagbuf[128]; u8_string tagname;
  if ((PAIRP(head)))  head = kno_eval(head,env);
  else head = kno_incref(head);
  if (KNO_ABORTED(head)) return head;
  tagname = get_tagname(head,tagbuf,128);
  if (tagname == NULL) {
    kno_decref(head);
    return kno_err(kno_SyntaxError,"xmlentry",NULL,expr);}
  else if (open_markup(out,NULL,tagname,args,env,0)<0) {
    kno_decref(head);
    u8_flush(out);
    return KNO_ERROR;}
  else {
    kno_decref(head);
    u8_flush(out);
    return VOID;}
}

DEFPRIM1("xmlend",xmlend_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
         "`(XMLEND *arg0*)` **undocumented**",
         kno_any_type,KNO_VOID);
static lispval xmlend_prim(lispval head)
{
  U8_OUTPUT *out = u8_current_output;
  u8_byte tagbuf[128]; u8_string tagname;
  tagname = get_tagname(head,tagbuf,128);
  if (tagname == NULL) {
    kno_decref(head);
    return kno_err(kno_SyntaxError,"xmlend",NULL,head);}
  else u8_printf(out,"</%s>",tagname);
  return VOID;
}

static lispval doxmlblock(lispval expr,kno_lexenv env,
                          kno_stack _stack,int newline)
{
  lispval tagspec = kno_get_arg(expr,1), attribs, body;
  lispval xmloidfn = kno_symeval(xmloidfn_symbol,env);
  u8_byte tagbuf[128], buf[128];
  u8_string tagname; int eval_attribs = 0;
  U8_OUTPUT *out, tmpout;
  if (SYMBOLP(tagspec)) {
    attribs = kno_get_arg(expr,2); body = kno_get_body(expr,3);
    eval_attribs = 1;}
  else if (STRINGP(tagspec)) {
    attribs = kno_get_arg(expr,2); body = kno_get_body(expr,3);
    kno_incref(tagspec); eval_attribs = 1;}
  else {
    body = kno_get_body(expr,2);
    tagspec = kno_eval(tagspec,env);
    if (KNO_ABORTED(tagspec)) {
      kno_decref(xmloidfn);
      return tagspec;}
    else if (SYMBOLP(tagspec)) attribs = NIL;
    else if (STRINGP(tagspec)) attribs = NIL;
    else if (PAIRP(tagspec)) {
      lispval name = KNO_CAR(tagspec); attribs = kno_incref(KNO_CDR(tagspec));
      if (SYMBOLP(name)) {}
      else if (STRINGP(name)) kno_incref(name);
      else {
        kno_decref(xmloidfn);
        return kno_err(kno_SyntaxError,"xmlblock",NULL,tagspec);}
      kno_decref(tagspec); tagspec = name;}
    else {
      kno_decref(xmloidfn);
      return kno_err(kno_SyntaxError,"xmlblock",NULL,tagspec);}}
  tagname = get_tagname(tagspec,tagbuf,128);
  if (tagname == NULL) {
    kno_decref(xmloidfn);
    return kno_err(kno_SyntaxError,"xmlblock",NULL,expr);}
  out = u8_current_output;
  U8_INIT_STATIC_OUTPUT_BUF(tmpout,128,buf);
  if (open_markup(out,&tmpout,tagname,attribs,
                  ((eval_attribs)?(env):(NULL)),0)<0) {
    kno_decref(xmloidfn);
    return KNO_ERROR;}
  if (newline) u8_putc(out,'\n');
  while (PAIRP(body)) {
    lispval value = fast_eval(KNO_CAR(body),env);
    if (KNO_ABORTED(value)) {
      kno_decref(xmloidfn);
      close_markup(out,tagname);
      return value;}
    else if (kno_xmlout_helper(out,&tmpout,value,xmloidfn,env))
      kno_decref(value);
    else {
      kno_decref(xmloidfn);
      return value;}
    body = KNO_CDR(body);}
  if (newline) u8_putc(out,'\n');
  if (close_markup(out,tagname)<0) {
    kno_decref(xmloidfn);
    return KNO_ERROR;}
  if (tagname!=tagbuf) u8_free(tagname);
  u8_flush(out);
  kno_decref(xmloidfn);
  if (tmpout.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(tmpout.u8_outbuf);
  return VOID;
}

/* Does a block without wrapping content in newlines */
static lispval xmlblock_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  return doxmlblock(expr,env,_stack,0);
}
/* Does a block and wraps content in newlines */
static lispval xmlblockn_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  return doxmlblock(expr,env,_stack,1);
}

static lispval handle_markup(lispval expr,kno_lexenv env,kno_stack _stack,
                             int star,int block)
{
  if ((PAIRP(expr)) && (SYMBOLP(KNO_CAR(expr)))) {
    lispval attribs = kno_get_arg(expr,1), body = kno_get_body(expr,2);
    lispval xmloidfn = kno_symeval(xmloidfn_symbol,env);
    U8_OUTPUT *out = u8_current_output, tmpout;
    u8_byte tagbuf[128], buf[128];
    u8_string tagname;
    U8_INIT_STATIC_OUTPUT_BUF(tmpout,128,buf);
    if (star) {
      attribs = kno_get_arg(expr,1); body = kno_get_body(expr,2);}
    else {attribs = NIL; body = kno_get_body(expr,1);}
    tagname = get_tagname(KNO_CAR(expr),tagbuf,128);
    if (tagname == NULL) {
      kno_decref(xmloidfn);
      return kno_err(kno_SyntaxError,"handle_markup",NULL,expr);}
    if (block) u8_printf(out,"\n");
    if (open_markup(out,&tmpout,tagname,attribs,env,0)<0) {
      kno_decref(xmloidfn);
      return KNO_ERROR;}
    if (block) u8_printf(out,"\n");
    while (PAIRP(body)) {
      lispval value = fast_eval(KNO_CAR(body),env);
      if (KNO_ABORTED(value)) {
        close_markup(out,tagname);
        if (block) u8_printf(out,"\n");
        kno_decref(xmloidfn);
        return value;}
      else if (kno_xmlout_helper(out,&tmpout,value,xmloidfn,env))
        kno_decref(value);
      else {
        kno_decref(xmloidfn);
        return value;}
      body = KNO_CDR(body);}
    if (block) u8_printf(out,"\n");
    if (close_markup(out,tagname)<0) {
      kno_decref(xmloidfn);
      return KNO_ERROR;}
    if (block) u8_printf(out,"\n");
    if (tagname!=tagbuf) u8_free(tagname);
    u8_flush(out);
    if (tmpout.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(tmpout.u8_outbuf);
    kno_decref(xmloidfn);
    return VOID;}
  else return kno_err(kno_SyntaxError,"XML markup",NULL,kno_incref(expr));
}

static lispval markup_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  return handle_markup(expr,env,_stack,0,0);
}

static lispval markupblock_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  return handle_markup(expr,env,_stack,0,1);
}

static lispval markupstarblock_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  return handle_markup(expr,env,_stack,1,1);
}

static lispval markupstar_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  return handle_markup(expr,env,_stack,1,0);
}

static lispval emptymarkup_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  u8_byte tagbuf[128];
  U8_OUTPUT *out = u8_current_output;
  lispval head = KNO_CAR(expr), args = KNO_CDR(expr);
  u8_string tagname = get_tagname(head,tagbuf,128);
  if (tagname == NULL)
    return kno_err(kno_SyntaxError,"emptymarkup_evalfn",NULL,expr);
  else if (open_markup(out,NULL,tagname,args,env,1)<0)
    return KNO_ERROR;
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
  kno_pool p = kno_oid2pool(arg);
  if (p == NULL) return EMPTY;
  else {
    lispval pool = kno_pool2lisp(p), browseinfo = kno_thread_get(browseinfo_symbol), dflt = VOID;
    DO_CHOICES(info,browseinfo) {
      if ((VECTORP(info)) && (VEC_LEN(info)>0))
        if (KNO_EQ(VEC_REF(info,0),pool)) {
          kno_incref(info); kno_decref(browseinfo);
          return info;}
        else if (KNO_TRUEP(VEC_REF(info,0))) {
          dflt = info;}
        else {}
      else dflt = info;}
    if (VOIDP(dflt)) {
      u8_lock_mutex(&browseinfo_lock);
      {DO_CHOICES(info,global_browseinfo) {
          if ((VECTORP(info)) && (VEC_LEN(info)>0)) {
            if (KNO_EQ(VEC_REF(info,0),pool)) {
              kno_incref(info);
              u8_unlock_mutex(&browseinfo_lock);
              return info;}
            else if (KNO_TRUEP(VEC_REF(info,0)))
              dflt = info;}}
        kno_incref(dflt);
        u8_unlock_mutex(&browseinfo_lock);
        if (VOIDP(dflt)) return EMPTY;
        else return dflt;}}
    else {
      kno_incref(dflt); kno_decref(browseinfo);
      return dflt;}}
}

static int unpack_browseinfo(lispval info,u8_string *baseuri,u8_string *classname,lispval *displayer)
{
  if ((EMPTYP(info)) || (VOIDP(info))) {
    if (*baseuri == NULL) {
      if (default_browse_uri)
        *baseuri = default_browse_uri;
      else *baseuri="browse.knocgi?";}
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
      *baseuri = ((default_browse_uri) ? (default_browse_uri) : ((u8_string)"browse.knocgi?"));
    switch (VEC_LEN(info)) {
    case 2:
      if (STRINGP(VEC_REF(info,1)))
        *baseuri = CSTRING(VEC_REF(info,1));
      else u8_log(LOG_WARN,kno_TypeError,"Bad browse info %q",info);
      break;
    case 3:
      if (STRINGP(VEC_REF(info,2)))
        *classname = CSTRING(VEC_REF(info,2));
      else u8_log(LOG_WARN,kno_TypeError,"Bad browse info %q",info);
      break;
    case 4:
      if (displayer) *displayer = VEC_REF(info,3);}}
  else {
    u8_log(LOG_WARN,kno_TypeError,"Bad browse info %q",info);
    *baseuri="browse.knocgi?";}
  return 0;
}

static lispval browseinfo_config_get(lispval var,void *ignored)
{
  lispval result;
  u8_lock_mutex(&browseinfo_lock);
  result = global_browseinfo; kno_incref(result);
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
      if ((VECTORP(info)) && (KNO_EQ(target,VEC_REF(info,0)))) {}
      else if ((STRINGP(info)) && (KNO_TRUEP(target))) {}
      else {
        kno_incref(info); CHOICE_ADD(new_browseinfo,info);}}
    kno_incref(val); CHOICE_ADD(new_browseinfo,val);
    global_browseinfo = kno_simplify_choice(new_browseinfo);
    u8_unlock_mutex(&browseinfo_lock);
    return 1;}
  else {
    u8_unlock_mutex(&browseinfo_lock);
    kno_seterr(kno_TypeError,"browse info",NULL,val);
    return -1;}
}

/* Doing anchor output */

static lispval doanchor_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  U8_OUTPUT *out = u8_current_output, tmpout;
  lispval target = kno_eval(kno_get_arg(expr,1),env), xmloidfn;
  lispval body = kno_get_body(expr,2);
  u8_byte buf[128]; U8_INIT_STATIC_OUTPUT_BUF(tmpout,128,buf);
  if (KNO_ABORTED(target))
    return target;
  else if (VOIDP(target))
    return kno_err(kno_SyntaxError,"doanchor",NULL,VOID);
  else if (NILP(body))
    return kno_err(kno_SyntaxError,"doanchor",NULL,VOID);
  if (STRINGP(target)) {
    u8_printf(out,"<a href='");
    attrib_entify(out,CSTRING(target));
    u8_puts(out,"'>");}
  else if (SYMBOLP(target)) {
    u8_printf(out,"<a href='#");
    attrib_entify(out,SYM_NAME(target));
    u8_printf(out,"'>");}
  else if (OIDP(target)) {
    KNO_OID addr = KNO_OID_ADDR(target);
    lispval browseinfo = get_browseinfo(target);
    u8_string uri = NULL, class = NULL;
    unpack_browseinfo(browseinfo,&uri,&class,NULL);
    u8_printf(out,"<a href='%s:@%x/%x' class='%s'>",
              uri,KNO_OID_HI(addr),KNO_OID_LO(addr),class);
    kno_decref(browseinfo);}
  else {
    return kno_type_error(_("valid anchor target"),"doanchor",target);}
  xmloidfn = kno_symeval(xmloidfn_symbol,env);
  while (PAIRP(body)) {
    lispval value = fast_eval(KNO_CAR(body),env);
    if (KNO_ABORTED(value)) {
      kno_decref(xmloidfn); kno_decref(target);
      return value;}
    else if (kno_xmlout_helper(out,&tmpout,value,xmloidfn,env))
      kno_decref(value);
    else {
      kno_decref(xmloidfn); kno_decref(target);
      return value;}
    body = KNO_CDR(body);}
  u8_printf(out,"</a>");
  u8_flush(out);
  kno_decref(xmloidfn); kno_decref(target);
  if (tmpout.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(tmpout.u8_outbuf);
  return VOID;
}

static int has_class_attrib(lispval attribs)
{
  lispval scan = attribs;
  while (PAIRP(scan))
    if (KNO_EQ(KNO_CAR(scan),class_symbol)) return 1;
    else if ((PAIRP(KNO_CAR(scan))) &&
             (KNO_EQ(KNO_CAR(KNO_CAR(scan)),class_symbol)))
      return 1;
    else {
      scan = KNO_CDR(scan);
      if (PAIRP(scan)) scan = KNO_CDR(scan);}
  return 0;
}

static lispval doanchor_star_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  U8_OUTPUT *out = u8_current_output, tmpout;
  lispval target = kno_eval(kno_get_arg(expr,1),env), xmloidfn = VOID;
  lispval attribs = kno_get_arg(expr,2);
  lispval body = kno_get_body(expr,3);
  u8_byte buf[128]; U8_INIT_STATIC_OUTPUT_BUF(tmpout,128,buf);
  if (KNO_ABORTED(target))
    return target;
  else if (VOIDP(target))
    return kno_err(kno_SyntaxError,"doanchor",NULL,VOID);
  else if (NILP(body))
    return kno_err(kno_SyntaxError,"doanchor",NULL,VOID);
  if (STRINGP(target))
    attribs = kno_conspair(href_symbol,kno_conspair(kno_incref(target),kno_incref(attribs)));
  else if (SYMBOLP(target)) {
    tmpout.u8_write = tmpout.u8_outbuf;
    u8_printf(out,"#%s",SYM_NAME(target));
    attribs = kno_conspair(href_symbol,
                           kno_conspair(kno_stream2string(&tmpout),kno_incref(attribs)));}
  else if (OIDP(target)) {
    KNO_OID addr = KNO_OID_ADDR(target);
    lispval browseinfo = get_browseinfo(target);
    u8_string uri = NULL, class = NULL;
    unpack_browseinfo(browseinfo,&uri,&class,NULL);
    if (has_class_attrib(attribs))
      kno_incref(attribs);
    else attribs = kno_conspair(kno_intern("class"),
                                kno_conspair(kno_mkstring(class),kno_incref(attribs)));
    tmpout.u8_write = tmpout.u8_outbuf;
    u8_printf(&tmpout,"%s:@%x/%x",uri,KNO_OID_HI(addr),(KNO_OID_LO(addr)));
    attribs = kno_conspair
      (href_symbol,
       kno_conspair(kno_substring(tmpout.u8_outbuf,tmpout.u8_write),
                    attribs));
    kno_decref(browseinfo);}
  else return kno_type_error(_("valid anchor target"),"doanchor_star",target);
  xmloidfn = kno_symeval(xmloidfn_symbol,env);
  if (open_markup(out,&tmpout,"a",attribs,env,0)<0) {
    kno_decref(attribs); kno_decref(xmloidfn); kno_decref(target);
    return KNO_ERROR;}
  while (PAIRP(body)) {
    lispval value = fast_eval(KNO_CAR(body),env);
    if (KNO_ABORTED(value)) {
      kno_decref(attribs); kno_decref(xmloidfn); kno_decref(target);
      return value;}
    else if (kno_xmlout_helper(out,&tmpout,value,xmloidfn,env))
      kno_decref(value);
    else {
      kno_decref(attribs); kno_decref(xmloidfn); kno_decref(target);
      return value;}
    body = KNO_CDR(body);}
  u8_printf(out,"</a>");
  u8_flush(out);
  kno_decref(attribs); kno_decref(xmloidfn); kno_decref(target);
  if (tmpout.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(tmpout.u8_outbuf);
  return VOID;
}

KNO_EXPORT void kno_xmloid(u8_output out,lispval arg)
{
  KNO_OID addr = KNO_OID_ADDR(arg);
  lispval browseinfo = get_browseinfo(arg), name, displayer = VOID;
  u8_string uri = NULL, class = NULL;
  unpack_browseinfo(browseinfo,&uri,&class,&displayer);
  if (out == NULL) out = u8_current_output;
  u8_printf(out,"<a class='%s' href='%s?:@%x/%x'>",
            class,uri,KNO_OID_HI(addr),KNO_OID_LO(addr));
  if ((OIDP(displayer)) || (SYMBOLP(displayer)))
    name = kno_frame_get(arg,displayer);
  else if (KNO_APPLICABLEP(displayer))
    name = kno_apply(displayer,1,&arg);
  else name = kno_frame_get(arg,obj_name);
  if (EMPTYP(name))
    u8_printf(out,"%q",arg);
  else if (VOIDP(name)) {}
  else kno_xmlout_helper(out,NULL,name,VOID,NULL);
  kno_decref(name);
  u8_printf(out,"</a>");
  kno_decref(browseinfo);
}

DEFPRIM1("%xmloid",xmloid,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
         "`(%XMLOID *arg0*)` **undocumented**",
         kno_any_type,KNO_VOID);
static lispval xmloid(lispval oid_arg)
{
  kno_xmloid(NULL,oid_arg);
  return VOID;
}

/* XMLEVAL primitives */

static lispval xmleval_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval xmlarg = kno_get_arg(expr,1);
  if (VOIDP(xmlarg))
    return kno_err(kno_SyntaxError,"xmleval_evalfn",NULL,VOID);
  else {
    U8_OUTPUT *out = u8_current_output;
    if (STRINGP(xmlarg)) {
      u8_string data = CSTRING(xmlarg);
      if (data[0]=='<') u8_putn(out,data,STRLEN(xmlarg));
      else emit_xmlcontent(out,data);
      return VOID;}
    else {
      lispval xml = kno_eval(xmlarg,env);
      if (KNO_ABORTED(xml)) return xml;
      lispval env_arg = kno_eval(kno_get_arg(expr,2),env);
      if (KNO_ABORTED(env_arg)) { kno_decref(xml); return env_arg;}
      lispval xml_env_arg = kno_eval(kno_get_arg(expr,3),env);
      if (KNO_ABORTED(xml_env_arg)) {
        kno_decref(env_arg);
        kno_decref(xml);
        return xml_env_arg;}
      if (!((VOIDP(env_arg)) || (FALSEP(env_arg)) ||
            (KNO_TRUEP(env_arg)) || (KNO_LEXENVP(env_arg)) ||
            (TABLEP(env_arg)))) {
        lispval err = kno_type_error("SCHEME environment","xmleval_evalfn",env_arg);
        kno_decref(xml);
        kno_decref(env_arg);
        kno_decref(xml_env_arg);
        return err;}
      else if (!((VOIDP(xml_env_arg)) || (FALSEP(xml_env_arg)) ||
                 (KNO_LEXENVP(xml_env_arg)) || (TABLEP(xml_env_arg)))) {
        kno_decref(xml);
        kno_decref(env_arg);
        return kno_type_error("environment","xmleval_evalfn",xml_env_arg);}
      else {
        lispval result = kno_xmleval_with(out,xml,env_arg,xml_env_arg);
        kno_decref(xml);
        kno_decref(env_arg);
        kno_decref(xml_env_arg);
        return result;}
    }
  }
}

DEFPRIM3("xml->string",xml2string_prim,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
         "`(XML->STRING *arg0* [*arg1*] [*arg2*])` **undocumented**",
         kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
         kno_any_type,KNO_VOID);
static lispval xml2string_prim(lispval xml,lispval env_arg,lispval xml_env_arg)
{
  if (!((VOIDP(env_arg)) || (FALSEP(env_arg)) ||
        (KNO_TRUEP(env_arg)) || (KNO_LEXENVP(env_arg)) ||
        (TABLEP(env_arg)))) {
    return kno_type_error("SCHEME environment","xmleval_evalfn",env_arg);}
  else if (!((VOIDP(xml_env_arg)) || (FALSEP(xml_env_arg)) ||
             (KNO_LEXENVP(xml_env_arg)) || (TABLEP(xml_env_arg)))) {
    return kno_type_error("environment","xmleval_evalfn",xml_env_arg);}
  if (STRINGP(xml)) {
    lispval parsed = kno_knoml_arg(xml);
    lispval result = xml2string_prim(parsed,env_arg,xml_env_arg);
    kno_decref(parsed);
    return result;}
  else {
    U8_OUTPUT out; char buf[1024];
    U8_INIT_OUTPUT_BUF(&out,1024,buf);
    lispval result = kno_xmleval_with(&out,xml,env_arg,xml_env_arg);
    kno_decref(result);
    return kno_stream2string(&out);}
}

static lispval xmlopen_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  if (!(PAIRP(KNO_CDR(expr))))
    return kno_err(kno_SyntaxError,"xmleval_evalfn",NULL,VOID);
  else {
    lispval node = kno_eval(KNO_CADR(expr),env);
    if (KNO_ABORTED(node))
      return node;
    else if (TABLEP(node)) {
      lispval result = kno_open_xml(node,env);
      kno_decref(node);
      return result;}
    else return VOID;}
}

DEFPRIM1("xmlclose",xmlclose_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
         "`(XMLCLOSE *arg0*)` **undocumented**",
         kno_any_type,KNO_VOID);
static lispval xmlclose_prim(lispval arg)
{
  if (!(TABLEP(arg)))
    return kno_type_error("XML node","xmlclose_prim",arg);
  else {
    lispval tag = kno_close_xml(arg);
    kno_decref(tag);
    return VOID;}
}

/* Javascript output */

/* This generates a javascript function call, transforming the
   arguments into strings.  This uses double quotes to quote the
   arguments because the output is typically inserted in attributes
   which are single quoted. */
static lispval output_javascript(u8_output out,lispval args,kno_lexenv env)
{
  if (NILP(args))
    return kno_err(kno_SyntaxError,"output_javascript",NULL,args);
  else {
    int i = 0;
    lispval head_expr = KNO_CAR(args), head = kno_eval(head_expr,env), body = KNO_CDR(args);
    if (KNO_ABORTED(head))
      return head;
    else if (!(STRINGP(head)))
      return kno_type_error(_("javascript function name"),
                            "output_javascript",head);
    else u8_printf(out,"%s(",CSTRING(head));
    {KNO_DOELTS(elt,body,count) {
        lispval val;
        if (i>0) u8_putc(out,',');
        i++;
        if (KNO_NEED_EVALP(elt))
          val = kno_eval(elt,env);
        else val = kno_incref(elt);
        if (KNO_ABORTED(val))
          return val;
        else if (VOIDP(val)) {}
        else if (FIXNUMP(val))
          u8_printf(out,"%lld",FIX2INT(val));
        else if (KNO_FLONUMP(val))
          u8_printf(out,"%f",KNO_FLONUM(val));
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
                    KNO_OID_HI(KNO_OID_ADDR(val)),
                    KNO_OID_LO(KNO_OID_ADDR(val)));
        else {
          U8_OUTPUT tmp; u8_byte buf[128]; const u8_byte *scan;
          U8_INIT_STATIC_OUTPUT_BUF(tmp,128,buf);
          tmp.u8_streaminfo = tmp.u8_streaminfo|U8_STREAM_TACITURN;
          u8_puts(out,"\":");
          kno_unparse(&tmp,val);
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
        kno_decref(val);}}
    u8_putc(out,')');
    kno_decref(head);
    return VOID;}
}

static lispval javascript_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval retval; struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,256);
  retval = output_javascript(&out,KNO_CDR(expr),env);
  if (VOIDP(retval))
    return kno_stream2string(&out);
  else {
    u8_free(out.u8_outbuf); return retval;}
}

static lispval javastmt_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval retval; struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,256);
  retval = output_javascript(&out,KNO_CDR(expr),env);
  if (VOIDP(retval)) {
    u8_putc(&out,';');
    return kno_stream2string(&out);}
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

static lispval soapenvelope_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  U8_OUTPUT *out = u8_current_output;
  lispval header_arg = kno_get_arg(expr,1);
  lispval body = kno_get_body(expr,2);
  u8_puts(out,soapenvopen);
  if (KNO_NEED_EVALP(header_arg)) {
    lispval value;
    u8_puts(out,soapheaderopen);
    value = kno_eval(header_arg,env);
    if (KNO_ABORTED(value)) return value;
    if (STRINGP(value)) u8_puts(out,CSTRING(value));
    kno_decref(value);
    u8_puts(out,soapheaderclose);}
  u8_puts(out,soapbodyopen);
  kno_printout_to(out,body,env);
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
      kno_pprint(&vstream,val,NULL,0,0,80);
    else kno_unparse(&vstream,val);
    emit_xmlcontent(s,vstream.u8_outbuf);
    u8_close_output(&vstream);}
  else {
    u8_string str = va_arg(*args,u8_string);
    if (str) emit_xmlcontent(s,str);
    else emit_xmlcontent(s,"(null)");}
  return NULL;
}

static lispval webtools_module, xhtml_module;

KNO_EXPORT void kno_init_xmloutput_c()
{
  webtools_module = kno_new_module("WEBTOOLS",0);
  xhtml_module = kno_new_module("XHTML",0);

  lispval markup_prim = kno_make_evalfn("markup",markup_evalfn);
  lispval markupstar_prim = kno_make_evalfn("markup*",markupstar_evalfn);
  lispval markupblock_prim=
    kno_make_evalfn("markupblock",markupblock_evalfn);
  lispval markupstarblock_prim=
    kno_make_evalfn("markup*block",markupstarblock_evalfn);
  lispval emptymarkup_prim=
    kno_make_evalfn("emptymarkup",emptymarkup_evalfn);
  lispval xmlout_prim = kno_make_evalfn("XMLOUT",xmlout_evalfn);
  lispval xmlblock_prim = kno_make_evalfn("XMLBLOCK",xmlblock_evalfn);
  lispval xmlblockn_prim = kno_make_evalfn("XMLBLOCKN",xmlblockn_evalfn);
  lispval xmlelt_prim = kno_make_evalfn("XMLELT",xmlentry_evalfn);

  u8_printf_handlers['k']=markup_printf_handler;

  kno_store(webtools_module,kno_intern("xmlout"),xmlout_prim);
  kno_store(webtools_module,kno_intern("xmlblock"),xmlblock_prim);
  kno_store(webtools_module,kno_intern("xmlblockn"),xmlblockn_prim);
  kno_store(webtools_module,kno_intern("xmlelt"),xmlelt_prim);
  kno_store(webtools_module,kno_intern("markupfn"),markup_prim);
  kno_store(webtools_module,kno_intern("markup*fn"),markupstar_prim);
  kno_store(webtools_module,kno_intern("blockmarkupfn"),markupblock_prim);
  kno_store(webtools_module,kno_intern("blockmarkup*fn"),markupstarblock_prim);
  kno_store(webtools_module,kno_intern("emptymarkupfn"),emptymarkup_prim);
  kno_def_evalfn(webtools_module,"SOAPENVELOPE",soapenvelope_evalfn,
		 "*undocumented*");

  kno_def_evalfn(xhtml_module,"ANCHOR",doanchor_evalfn,
		 "*undocumented*");
  kno_def_evalfn(xhtml_module,"ANCHOR*",doanchor_star_evalfn,
		 "*undocumented*");

  kno_def_evalfn(xhtml_module,"XHTML",raw_xhtml_evalfn,
		 "*undocumented*");

  kno_store(xhtml_module,kno_intern("div"),markupstarblock_prim);
  kno_store(xhtml_module,kno_intern("span"),markupstar_prim);

  kno_store(xhtml_module,kno_intern("hgroup"),markupblock_prim);
  kno_store(xhtml_module,kno_intern("hgroup*"),markupstarblock_prim);

  kno_store(xhtml_module,kno_intern("p"),markupblock_prim);
  kno_store(xhtml_module,kno_intern("p*"),markupstarblock_prim);
  kno_store(xhtml_module,kno_intern("h1"),markupblock_prim);
  kno_store(xhtml_module,kno_intern("h1*"),markupstarblock_prim);
  kno_store(xhtml_module,kno_intern("h2"),markupblock_prim);
  kno_store(xhtml_module,kno_intern("h2*"),markupstarblock_prim);
  kno_store(xhtml_module,kno_intern("h3"),markupblock_prim);
  kno_store(xhtml_module,kno_intern("h3*"),markupstarblock_prim);
  kno_store(xhtml_module,kno_intern("h4"),markupblock_prim);
  kno_store(xhtml_module,kno_intern("h4*"),markupstarblock_prim);

  kno_store(xhtml_module,kno_intern("ul"),markupblock_prim);
  kno_store(xhtml_module,kno_intern("ul*"),markupstarblock_prim);
  kno_store(xhtml_module,kno_intern("li"),markupblock_prim);
  kno_store(xhtml_module,kno_intern("li*"),markupstarblock_prim);

  kno_store(xhtml_module,kno_intern("strong"),markup_prim);
  kno_store(xhtml_module,kno_intern("em"),markup_prim);
  kno_store(xhtml_module,kno_intern("tt"),markup_prim);
  kno_store(xhtml_module,kno_intern("defn"),markup_prim);

  kno_store(xhtml_module,kno_intern("form"),markupstarblock_prim);

  kno_store(xhtml_module,kno_intern("table"),markupblock_prim);
  kno_store(xhtml_module,kno_intern("table*"),markupstarblock_prim);
  kno_store(xhtml_module,kno_intern("tr"),markupblock_prim);
  kno_store(xhtml_module,kno_intern("tr*"),markupstarblock_prim);
  kno_store(xhtml_module,kno_intern("td"),markup_prim);
  kno_store(xhtml_module,kno_intern("td*"),markupstar_prim);
  kno_store(xhtml_module,kno_intern("th"),markup_prim);
  kno_store(xhtml_module,kno_intern("th*"),markupstar_prim);

  kno_store(xhtml_module,kno_intern("img"),emptymarkup_prim);
  kno_store(xhtml_module,kno_intern("input"),emptymarkup_prim);
  kno_store(xhtml_module,kno_intern("br"),emptymarkup_prim);
  kno_store(xhtml_module,kno_intern("hr"),emptymarkup_prim);

  kno_def_evalfn(webtools_module,"XMLEVAL",xmleval_evalfn,
		 "*undocumented*");
  kno_def_evalfn(webtools_module,"XMLEVAL",xmleval_evalfn,
		 "*undocumented*");
  kno_def_evalfn(webtools_module,"XMLOPEN",xmlopen_evalfn,
		 "*undocumented*");
  kno_def_evalfn(webtools_module,"XMLOPEN",xmlopen_evalfn,
		 "*undocumented*");
  kno_def_evalfn(webtools_module,"XMLSTART",xmlstart_evalfn,
		 "*undocumented*");
  kno_def_evalfn(webtools_module,"XMLSTART",xmlstart_evalfn,
		 "*undocumented*");


  /* Not strictly XML of course, but a neighbor */
  kno_def_evalfn(xhtml_module,"JAVASCRIPT",javascript_evalfn,
		 "*undocumented*");
  kno_def_evalfn(xhtml_module,"JAVASTMT",javastmt_evalfn,
		 "*undocumented*");

  link_local_cprims();

  kno_decref(markup_prim); kno_decref(markupstar_prim);
  kno_decref(markupblock_prim); kno_decref(markupstarblock_prim);
  kno_decref(emptymarkup_prim); kno_decref(xmlout_prim);
  kno_decref(xmlblockn_prim); kno_decref(xmlblock_prim);
  kno_decref(xmlelt_prim);

  xmloidfn_symbol = kno_intern("%xmloid");
  id_symbol = kno_intern("%id");
  href_symbol = kno_intern("href");
  class_symbol = kno_intern("class");
  obj_name = kno_intern("obj-name");
  quote_symbol = kno_intern("quote");
  xmltag_symbol = kno_intern("%xmltag");
  rawtag_symbol = kno_intern("%rawtag");
  browseinfo_symbol = kno_intern("browseinfo");
  embedded_symbol = kno_intern("%embedded");
  estylesheet_symbol = kno_intern("%errorstyle");
  modules_symbol = kno_intern("%modules");
  xml_env_symbol = kno_intern("%xmlenv");

  kno_register_config
    ("BROWSEINFO",
     _("How to display OIDs for browsing in HTML/XML"),
     browseinfo_config_get,browseinfo_config_set,NULL);
  kno_register_config
    ("BROWSEOIDURI",
     _("Default anchor URI for OID references"),
     kno_sconfig_get,kno_sconfig_set,&default_browse_uri);
  kno_register_config
    ("BROWSEOIDCLASS",
     _("Default HTML CSS class for OID references"),
     kno_sconfig_get,kno_sconfig_set,&default_browse_class);

  u8_init_mutex(&browseinfo_lock);

  u8_register_source_file(_FILEINFO);

}



static void link_local_cprims()
{
  KNO_LINK_PRIM("xmlclose",xmlclose_prim,1,webtools_module);
  KNO_LINK_PRIM("xml->string",xml2string_prim,3,webtools_module);
  KNO_LINK_PRIM("xmlend",xmlend_prim,1,webtools_module);
  KNO_LINK_VARARGS("xmlempty",xmlemptyelt,webtools_module);
  KNO_LINK_PRIM("xmlify",xmlify,1,webtools_module);

  KNO_LINK_PRIM("%xmloid",xmloid,1,xhtml_module);
  KNO_LINK_PRIM("nbsp",nbsp_prim,0,xhtml_module);

}
