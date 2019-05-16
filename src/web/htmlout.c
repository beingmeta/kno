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
#include "kno/dtype.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/frames.h"
#include "kno/tables.h"
#include "kno/eval.h"
#include "kno/ports.h"
#include "kno/fdweb.h"
#include "kno/pprint.h"
#include "kno/support.h"

/* If you  edit this file, you probably also want to edit bugjar.css */
#include "backtrace_css.h"
#include "backtrace_js.h"

#include "kno/support.h"

#include <ctype.h>

#define fast_eval(x,env) (kno_stack_eval(x,env,_stack,0))

#ifndef KNO_HTMLOUT_MAX
#define KNO_HTMLOUT_MAX 15000000
#endif

ssize_t kno_htmlout_max = KNO_HTMLOUT_MAX;

#include <libu8/u8xfiles.h>

static void output_value(u8_output out,lispval val,
                         u8_string eltname,
                         u8_string classname);
KNO_EXPORT void kno_html_exception(u8_output s,u8_exception ex,int backtrace);

static u8_string error_stylesheet=NULL;
static u8_string error_javascript=NULL;

static lispval xmloidfn_symbol, obj_name, id_symbol, quote_symbol;
static lispval href_symbol, class_symbol, rawtag_symbol, browseinfo_symbol;
static lispval embedded_symbol, error_style_symbol, error_script_symbol;
static lispval modules_symbol, xml_env_symbol, xmltag_symbol;;

static void start_errorpage(u8_output s,
                            u8_condition cond,u8_context caller,
                            u8_string details)
{
  int isembedded = 0;
  s->u8_write = s->u8_outbuf;
  lispval embeddedp = kno_req_get(embedded_symbol,VOID);
  lispval req_style = kno_req_get(error_style_symbol,VOID);
  lispval req_script = kno_req_get(error_script_symbol,VOID);
  if ((KNO_NOVOIDP(embeddedp)) || (FALSEP(embeddedp))) isembedded = 1;
  if (STRINGP(embeddedp)) u8_puts(s,CSTRING(embeddedp));
  if (isembedded==0) {
    u8_printf(s,"%s\n%s\n",DEFAULT_DOCTYPE,DEFAULT_XMLPI);
    u8_printf(s,"<html>\n<head>\n<title>");
    if (cond) u8_printf(s,"%k",cond);
    else u8_printf(s,"Unknown Exception");
    if (caller) u8_printf(s," @%k",caller);
    if ( (details) && (strlen(details)<40) )
      u8_printf(s," (%s)",details);
    u8_puts(s,"</title>\n");
    u8_printf(s,"\n<style type='text/css'>%s</style>\n",KNO_BACKTRACE_CSS);
    u8_printf(s,"\n<script language='javascript'>\n%s\n</script>\n",
              KNO_BACKTRACE_JS);
    if (error_javascript) {
      if (strchr(error_javascript,'{'))
        u8_printf(s,"<script language ='javascript'>\n%s\n</script>\n",
                  error_javascript);
      else u8_printf(s,"<script src ='%s' language ='javascript'></script>\n",
                     error_javascript);}
    if (error_stylesheet) {
      if (strchr(error_stylesheet,'{'))
        u8_printf(s,"<style type='text/css'>\n%s\n</style>\n",
                  error_stylesheet);
      else u8_printf(s,"<link href='%s' rel='stylesheet' type='text/css'/>\n",
                     error_stylesheet);}
    if (KNO_STRINGP(req_style)) {
      u8_string rstyle = KNO_CSTRING(req_style);
      if (strchr(rstyle,'{'))
        u8_printf(s,"<style type='text/css'>\n%s\n</style>\n",rstyle);
      else u8_printf(s,"<link href='%s' rel='stylesheet' type='text/css'/>\n",
                     rstyle);}
    if (KNO_STRINGP(req_script)) {
      u8_string rscript = KNO_CSTRING(req_script);
      if (strchr(rscript,'{'))
        u8_printf(s,"<script language ='javascript'>\n%s\n</script>\n",rscript);
      else u8_printf(s,"<script src ='%s' language ='javascript'></script>\n",
                     rscript);}
    u8_puts(s,"</head>\n");}
  u8_puts(s,"<body id='ERRORPAGE'>\n");
}

KNO_EXPORT
void kno_xhtmldebugpage(u8_output s,u8_exception ex)
{

  start_errorpage(s,ex->u8x_cond,ex->u8x_context,ex->u8x_details);

  u8_puts(s,"<p class='sorry'>"
          "Sorry, there was an unexpected error processing your request"
          "</p>\n");

  lispval backtrace = KNO_U8X_STACK(ex);

  if (PAIRP(backtrace)) {
    u8_puts(s,"<div class='backtrace'>\n");
    kno_html_backtrace(s,backtrace);
    u8_puts(s,"</div>\n");}
  else if (KNO_VECTORP(backtrace)) {
    int i = 0, len = KNO_VECTOR_LENGTH(backtrace);
    u8_puts(s,"<pre class='backtrace'>\n");
    struct U8_OUTPUT btbuf; U8_INIT_OUTPUT(&btbuf,10000);
    while (i < len) {
      lispval entry = KNO_VECTOR_REF(backtrace,i);
      kno_pprint_x(&btbuf,entry,NULL,2,2,0,NULL,NULL);
      kno_emit_xmlcontent(s,btbuf.u8_outbuf);
      u8_putc(s,'\n');
      btbuf.u8_write = btbuf.u8_outbuf;
      i++;}
    u8_close_output(&btbuf);
    u8_puts(s,"\n</pre>\n");}
  else {
    u8_puts(s,"<pre class='backtrace'>\n");
    struct U8_OUTPUT btbuf; U8_INIT_OUTPUT(&btbuf,100000);
    kno_pprint_x(&btbuf,backtrace,NULL,0,0,0,NULL,NULL);
    kno_emit_xmlcontent(s,btbuf.u8_outbuf);
    u8_close_output(&btbuf);
    u8_puts(s,"</pre>\n");}
  u8_puts(s,"</body>\n</html>\n");
}

KNO_EXPORT
void kno_xhtmlerrorpage(u8_output s,u8_exception ex)
{
  start_errorpage(s,ex->u8x_cond,ex->u8x_context,ex->u8x_details);

  u8_puts(s,"<p class='sorry'>"
          "Sorry, there was an unexpected error processing your request"
          "</p>\n");

  kno_html_exception(s,ex,0);

  u8_puts(s,"</body>\n</html>\n");
}

static lispval debugpage2html_prim(lispval exception,lispval where)
{
  u8_exception ex=NULL;
  struct U8_EXCEPTION tempex={0};
  if ((VOIDP(exception))||(FALSEP(exception)))
    ex = u8_current_exception;
  else if (KNO_EXCEPTIONP(exception)) {
    struct KNO_EXCEPTION *xo=
      kno_consptr(struct KNO_EXCEPTION *,exception,kno_exception_type);
    tempex.u8x_cond = xo->ex_condition;
    tempex.u8x_context = xo->ex_caller;
    tempex.u8x_details = xo->ex_details;
    tempex.u8x_xdata = (void *) xo;
    tempex.u8x_free_xdata = kno_decref_embedded_exception;
    ex = &tempex;}
  else {
    u8_log(LOG_WARN,"debugpage2html_prim","Bad exception argument %q",exception);
    ex = u8_current_exception;}
  if ((VOIDP(where))||(KNO_TRUEP(where))) {
    u8_output s = u8_current_output;
    kno_xhtmldebugpage(s,ex);
    return KNO_TRUE;}
  else if (FALSEP(where)) {
    struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,4096);
    kno_xhtmldebugpage(&out,ex);
    return kno_init_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);}
  else if (KNO_PORTP(where)) {
    struct KNO_PORT *port = (kno_port) where;
    kno_xhtmldebugpage(port->port_output,ex);
    return KNO_TRUE;}
  else return KNO_FALSE;
}

static lispval backtrace2html_prim(lispval arg,lispval where)
{
  u8_exception ex=NULL;
  lispval backtrace=KNO_VOID;
  if ((VOIDP(arg))||(FALSEP(arg))||(arg == KNO_DEFAULT_VALUE)) {
    ex = u8_current_exception;
    if (ex) backtrace=KNO_U8X_STACK(ex);}
  else if (PAIRP(arg))
    backtrace=arg;
  else if (KNO_EXCEPTIONP(arg)) {
    struct KNO_EXCEPTION *xo=
      kno_consptr(struct KNO_EXCEPTION *,arg,kno_exception_type);
    backtrace = xo->ex_stack;}
  else return kno_err("Bad exception/backtrace","backtrace2html_prim",
                     NULL,arg);
  if (!(PAIRP(backtrace)))
    return VOID;
  else if ((VOIDP(where))||(KNO_TRUEP(where))) {
    u8_output s = u8_current_output;
    kno_html_backtrace(s,backtrace);
    return KNO_TRUE;}
  else if (FALSEP(where)) {
    struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,4096);
    kno_html_backtrace(&out,backtrace);
    return kno_init_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);}
  else return KNO_FALSE;
}

/* Output Scheme objects, mostly as tables */

KNO_EXPORT void kno_lisp2html(u8_output s,lispval v,u8_string tag,u8_string cl)
{
  output_value(s,v,tag,cl);
}

/* XHTML error report */

static lispval moduleid_symbol;

static int isexprp(lispval expr)
{
  KNO_DOLIST(elt,expr) {
    if ( (PAIRP(elt)) || (CHOICEP(elt)) ||
         (SLOTMAPP(elt)) || (SCHEMAPP(elt)) ||
         (VECTORP(elt)) || (KNO_CODEP(elt)))
      return 0;}
  return 1;
}

static int isoptsp(lispval expr)
{
  if (PAIRP(expr))
    if ((SYMBOLP(KNO_CAR(expr))) && (!(PAIRP(KNO_CDR(expr)))))
      return 1;
    else return isoptsp(KNO_CAR(expr)) && isoptsp(KNO_CDR(expr));
  else if ( (KNO_CONSTANTP(expr)) || (SYMBOLP(expr)) )
    return 1;
  else if ( (SCHEMAPP(expr)) || (SLOTMAPP(expr)) )
    return 1;
  else return 0;
}

static void output_opts(u8_output out,lispval expr)
{
  if (PAIRP(expr))
    if ( (SYMBOLP(KNO_CAR(expr))) && (!(PAIRP(KNO_CDR(expr)))) ) {
      u8_printf(out,"\n <tr><th class='optname'>%s</th>\n       ",
                SYM_NAME(KNO_CAR(expr)));
      output_value(out,KNO_CDR(expr),"td","optval");
      u8_printf(out,"</tr>");}
    else {
      output_opts(out,KNO_CAR(expr));
      output_opts(out,KNO_CDR(expr));}
  else if (NILP(expr)) {}
  else if (SYMBOLP(expr))
    u8_printf(out,"\n <tr><th class='optname'>%s</th><td>%s</td></tr>",
              SYM_NAME(expr),SYM_NAME(expr));
  else if ( (SCHEMAPP(expr)) || (SLOTMAPP(expr)) ) {
    lispval keys=kno_getkeys(expr);
    DO_CHOICES(key,keys) {
      lispval optval=kno_get(expr,key,VOID);
      if (SYMBOLP(key))
        u8_printf(out,"\n <tr><th class='optname'>%s</th>",
                  SYM_NAME(expr));
      else u8_printf(out,"\n <tr><th class='optkey'>%q</th>",expr);
      output_value(out,optval,"td","optval");
      u8_printf(out,"</tr>");
      kno_decref(optval);}
    kno_decref(keys);}
}


KNO_EXPORT
void kno_html_exception(u8_output s,u8_exception ex,int backtrace)
{
  lispval irritant=kno_get_irritant(ex);
  u8_string i_string=NULL; int overflow=0;
  U8_FIXED_OUTPUT(tmp,32);
  if (!(VOIDP(irritant))) {
    kno_unparse(&tmp,irritant);
    i_string=tmp.u8_outbuf;
    overflow=(tmp.u8_streaminfo&U8_STREAM_OVERFLOW);}
  else i_string=NULL;

  u8_puts(s,"<div class='exception'>\n");
  {
    u8_printf(s,"<span class='condition'>%k</span>"
              " in <span class='context'>%k</span>",
              ex->u8x_cond,ex->u8x_context);
    if (ex->u8x_details) {
      if (strlen(ex->u8x_details)<=42)
        u8_printf(s," <span class='details'>%k</span>",ex->u8x_details);}
    if (i_string)
      u8_printf(s,": <span class='irritant'>%s%s</span>",
                i_string,((overflow)?("..."):("")));
    if ( (ex->u8x_details) && (strlen(ex->u8x_details)>42) )
      u8_printf(s,"\n<p class='details'>%k</p>",ex->u8x_details);
    if ( (!(VOIDP(irritant))) && (overflow) )
      u8_printf(s,"\n<pre class='irritant'>%Q</pre>",irritant);
  }
  u8_puts(s,"\n</div>\n"); /* exception */
  if (backtrace) {
    lispval backtrace=KNO_U8X_STACK(ex);
    if (PAIRP(backtrace))
      kno_html_backtrace(s,backtrace);
    kno_decref(backtrace);}
}

static void output_value(u8_output out,lispval val,
                         u8_string eltname,
                         u8_string classname)
{
  if ( (out->u8_write - out->u8_outbuf) >= kno_htmlout_max)
    return;
  else if (STRINGP(val))
    if (STRLEN(val)>42)
      u8_printf(out," <%s class='%s long string' title='%d characters'>“%k”</%s>",
                eltname,classname,STRLEN(val),KNO_CSTRING(val),eltname);
    else u8_printf(out," <%s class='%s string' title='% characters'>“%k”</%s>",
                   eltname,classname,STRLEN(val),KNO_CSTRING(val),eltname);
  else if (SYMBOLP(val))
    u8_printf(out," <%s class='%s symbol'>%k</%s>",
              eltname,classname,SYM_NAME(val),eltname);
  else if (NUMBERP(val))
    u8_printf(out," <%s class='%s number'>%lk</%s>",
              eltname,classname,val,eltname);
  else if (VECTORP(val)) {
    int len=VEC_LEN(val);
    if (len<2) {
      u8_printf(out," <ol class='%s short vector'>#(",classname);
      if (len==1) output_value(out,VEC_REF(val,0),"span","vecelt");
      u8_printf(out,")</ol>");}
    else {
      u8_printf(out," <ol class='%s vector'>#(",classname);
      int i=0; while (i<len) {
        if (i>0) u8_putc(out,' ');
        output_value(out,VEC_REF(val,i),"li","vecelt");
        i++;}
      u8_printf(out,")</ol>");}}
  else if ( (SLOTMAPP(val)) || (SCHEMAPP(val)) ) {
    lispval keys=kno_getkeys(val);
    int n_keys=KNO_CHOICE_SIZE(keys);
    if (n_keys==0)
      u8_printf(out," <%s class='%s map'>#[]</%s>",eltname,classname,eltname);
    else if (n_keys==1) {
      lispval value=kno_get(val,keys,VOID);
      u8_printf(out," <%s class='%s map'>#[<span class='slotid'>%lk</span> ",
                eltname,classname,keys);
      output_value(out,value,"span","slotvalue");
      u8_printf(out," ]</%s>",eltname);
      kno_decref(value);}
    else {
      u8_printf(out,"\n<div class='%s map'>",classname);
      int i=0; DO_CHOICES(key,keys) {
        lispval value=kno_get(val,key,VOID);
        u8_printf(out,"\n  <div class='%s keyval keyval%d'>",classname,i);
        output_value(out,key,"span","key");
        if (CHOICEP(value)) u8_puts(out," <span class='slotvals'>");
        {DO_CHOICES(v,value) {
            u8_putc(out,' ');
            output_value(out,value,"span","slotval");}}
        if (CHOICEP(value)) u8_puts(out," </span>");
        u8_printf(out,"</div>");
        kno_decref(value);
        i++;}
      u8_printf(out,"\n</div>",classname);}}
  else if (PAIRP(val)) {
    u8_string tmp = kno_lisp2string(val);
    if (strlen(tmp)< 50)
      u8_printf(out,"<%s class='%s listval'>%s</%s>",
                eltname,classname,tmp,eltname);
    else if (isoptsp(val)) {
      u8_printf(out,"\n<table class='%s opts'>",classname);
      output_opts(out,val);
      u8_printf(out,"\n</table>\n",classname);}
    else if (isexprp(val)) {
      u8_printf(out,"\n<pre class='listexpr'>");
      kno_pprint(out,val,"",0,0,60);
      u8_printf(out,"\n</pre>");}
    else {
      lispval scan=val;
      u8_printf(out," <ol class='%s list'>",classname);
      while (PAIRP(scan)) {
        lispval car=KNO_CAR(val); scan=KNO_CDR(scan);
        output_value(out,car,"li","listelt");}
      if (!(NILP(scan)))
        output_value(out,scan,"li","cdrelt");
      u8_printf(out,"\n</ol>");}
    u8_free(tmp);}
  else if (CHOICEP(val)) {
    int size=KNO_CHOICE_SIZE(val), i=0;
    if (size<7)
      u8_printf(out," <ul class='%s short choice'>{",classname);
    else u8_printf(out," <ul class='%s choice'>{",classname);
    DO_CHOICES(elt,val) {
      if (i>0) u8_putc(out,' ');
      output_value(out,elt,"li","choicelt");
      i++;}
    u8_printf(out,"}</ul>");}
  else if (PACKETP(val))
    if (KNO_PACKET_LENGTH(val)>128)
      u8_printf(out," <%s class='%s long packet'>%lk</%s>",
                eltname,classname,val,eltname);
    else u8_printf(out," <%s class='%s packet'>%lk</%s>",
                   eltname,classname,val,eltname);
  else {
    kno_ptr_type ptrtype=KNO_PTR_TYPE(val);
    if (kno_type_names[ptrtype])
      u8_printf(out," <%s class='%s %s'>%lk</%s>",
                eltname,classname,kno_type_names[ptrtype],
                val,eltname);}
}

#define INTVAL(x)    ((FIXNUMP(x))?(KNO_INT(x)):(-1))
#define STRINGVAL(x) ((STRINGP(x))?(CSTRING(x)):((u8_string)"uninitialized"))

static void output_stack_frame(u8_output out,lispval entry)
{
  if ((VECTORP(entry)) && (VEC_LEN(entry)>=7)) {
    lispval depth=VEC_REF(entry,0);
    lispval type=VEC_REF(entry,1);
    lispval label=VEC_REF(entry,2);
    lispval status=VEC_REF(entry,3);
    lispval op=VEC_REF(entry,4);
    lispval args=VEC_REF(entry,5);
    lispval env=VEC_REF(entry,6);
    lispval source=VEC_REF(entry,7);
    u8_puts(out,"<div class='stackframe'>\n");
    u8_printf(out,
              "  <div class='head'>"
              "   <span class='label'>%s</span>"
              "   <span class='depth'>%d</span>"
              "   <span class='type'>%s</span>",
              STRINGVAL(label),KNO_INT(depth),STRINGVAL(type));
    if (STRINGP(status))
      u8_printf(out,"\n  <p class='status'>%s</p>\n",CSTRING(status));
    u8_puts(out,"</div>"); /* class='head' */
    if (PAIRP(source))
      u8_printf(out,"\n  <pre class='source'>\n%Q\n</pre>");
    if (FALSEP(args)) {
      if (PAIRP(op)) {
        u8_puts(out,"\n  <pre class='eval expr'>\n");
        kno_pprint(out,op,NULL,0,0,100);
        u8_puts(out,"\n  </pre>");}
      else u8_printf(out,"\n  <div class='eval'>%lk</div>",op);}
    else {
      u8_puts(out,"\n  <div class='call'>\n");
      output_value(out,op,"span","handler");
      int i=0, n=VEC_LEN(args);
      while (i<n) {
        lispval arg=VEC_REF(args,i);
        output_value(out,arg,"span","arg");
        i++;}
      u8_puts(out,"\n  </div>\n");} /* class='call' */
    if (TABLEP(env)) {
      lispval vars=kno_getkeys(env);
      u8_printf(out,"<div class='bindings'>");
      DO_CHOICES(var,vars) {
        if ( (out->u8_write - out->u8_outbuf) >= kno_htmlout_max) {
          KNO_STOP_DO_CHOICES;
          break;}
        else if (SYMBOLP(var)) {
          lispval val=kno_get(env,var,VOID);
          u8_puts(out,"\n <div class='binding'>");
          if ((val == VOID) || (val == KNO_UNBOUND))
            u8_printf(out,"<span class='varname'>%s</span> "
                      "<span class='evalsto'>⇒</span> "
                      "<span class='unbound'>UNBOUND</span>",
                      SYM_NAME(var));
          else {
            u8_printf(out,"<span class='varname'>%s</span> "
                      "<span class='evalsto'>⇒</span> ",
                      SYM_NAME(var));
            { if (CHOICEP(val))
                u8_printf(out,
                          "<span class='values'>"
                          "<span class='nvals'>(%d values)</span> ",
                          KNO_CHOICE_SIZE(val));
              {DO_CHOICES(v,val) {
                  output_value(out,v,"span","value");}}
              if (CHOICEP(val)) u8_puts(out," </span> ");}}
          u8_puts(out,"</div>");} /* class='binding' */
      }
      u8_printf(out,"\n</div>");} /* class='bindings' */
  }
  else {
    U8_STATIC_OUTPUT(pprinted,4096);
    u8_puts(out,"\n <pre class='lispobj'>\n");
    kno_pprint(&pprinted,entry,NULL,0,0,80);
    kno_emit_xmlcontent(out,pprinted.u8_outbuf);
    u8_puts(out,"\n</pre>\n");
    u8_close_output(&pprinted);}
}

KNO_EXPORT
void kno_html_backtrace(u8_output out,lispval backtrace)
{
  /* Output the backtrace */
  lispval scan=backtrace; while (PAIRP(scan)) {
    if ( (out->u8_write - out->u8_outbuf) >= kno_htmlout_max) return;
    lispval entry=KNO_CAR(scan); scan=KNO_CDR(scan);
    output_stack_frame(out,entry);}
}

/* Outputing tables to XHTML */

static void output_xhtml_table(U8_OUTPUT *out,lispval tbl,lispval keys,
                               u8_string class_name,lispval xmloidfn)
{
  u8_printf(out,"<table class='%s'>\n",class_name);
  if (OIDP(tbl))
    u8_printf(out,"<tr><th colspan='2' class='header'>%lk</th></tr>\n",tbl);
  else if (HASHTABLEP(tbl))
    u8_printf(out,"<tr><th colspan='2' class='header'>%lk</th></tr>\n",tbl);
  else u8_printf(out,"<tr><th colspan='2' class='header'>%s</th></tr>\n",
                 kno_type_names[KNO_PTR_TYPE(tbl)]);
  {
    DO_CHOICES(key,keys) {
      if ( (out->u8_write - out->u8_outbuf) >= kno_htmlout_max) {
        KNO_STOP_DO_CHOICES; return;}
      lispval _value=
        ((OIDP(tbl)) ? (kno_frame_get(tbl,key)) :
         (kno_get(tbl,key,EMPTY)));
      lispval values = kno_simplify_choice(_value);
      u8_printf(out,"  <tr><th>");
      kno_xmlout_helper(out,NULL,key,xmloidfn,NULL);
      if (EMPTYP(values))
        u8_printf(out,"</th>\n    <td class='novalues'>No values</td></tr>\n");
      else if (CHOICEP(values)) {
        int first_item = 1;
        DO_CHOICES(value,values) {
          if (first_item) {
            u8_puts(out,"</th>\n    <td><div class='value'>");
            first_item = 0;}
          else
            u8_puts(out,"        <div class='value'>");
          kno_xmlout_helper(out,NULL,value,xmloidfn,NULL);
          u8_puts(out,"</div> \n");}
        u8_printf(out,"    </td></tr>\n");}
      else {
        u8_printf(out,"</th>\n      <td class='singlevalue'>");
        kno_xmlout_helper(out,NULL,values,xmloidfn,NULL);
        u8_printf(out,"</td></tr>\n");}}}
  u8_printf(out,"</table>\n");
}

static lispval table2html_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  u8_string classname = NULL;
  U8_OUTPUT *out = u8_current_output;
  lispval xmloidfn = kno_symeval(xmloidfn_symbol,env);
  lispval tables, classarg, slotids;
  tables = kno_eval(kno_get_arg(expr,1),env);
  if (KNO_ABORTP(tables))return tables;
  else if (VOIDP(tables))
    return kno_err(kno_SyntaxError,"table2html_evalfn",NULL,expr);
  classarg = kno_eval(kno_get_arg(expr,2),env);
  if (KNO_ABORTP(classarg)) {
    kno_decref(tables); return classarg;}
  else if (STRINGP(classarg)) classname = CSTRING(classarg);
  else if ((VOIDP(classarg)) || (FALSEP(classarg))) {}
  else {
    kno_decref(tables);
    return kno_type_error(_("string"),"table2html_evalfn",classarg);}
  slotids = kno_eval(kno_get_arg(expr,3),env);
  if (KNO_ABORTP(slotids)) {
    kno_decref(tables); kno_decref(classarg); return slotids;}
  {
    DO_CHOICES(table,tables)
      if (TABLEP(table)) {
        lispval keys = ((VOIDP(slotids)) ? (kno_getkeys(table)) : (slotids));
        if (classname)
          output_xhtml_table(out,table,keys,classname,xmloidfn);
        else if (OIDP(table))
          output_xhtml_table(out,table,keys,"frame_table",xmloidfn);
        else output_xhtml_table(out,table,keys,"table_table",xmloidfn);}
      else {
        kno_decref(tables); kno_decref(classarg); kno_decref(slotids);
        return kno_type_error(_("table"),"table2html_evalfn",table);}}
  return VOID;
}

static lispval obj2html_prim(lispval obj,lispval tag)
{
  u8_string tagname = NULL, classname = NULL; u8_byte tagbuf[64];
  U8_OUTPUT *s = u8_current_output;
  if (STRINGP(tag)) {
    u8_string s = CSTRING(tag);
    u8_string dot = strchr(s,'.');
    if ((dot)&&((dot-s)>50))
      return kno_type_error("HTML tag.class","obj2html_prim",tag);
    else if (dot) {
      memcpy(tagbuf,s,dot-s); tagbuf[dot-s]='\0';
      tagname = tagbuf; classname = dot+1;}
    else tagname = s;}
  else if (SYMBOLP(tag)) tagname = SYM_NAME(tag);
  else if ((VOIDP(tag))||(FALSEP(tag))) {}
  else return kno_type_error("HTML tag.class","obj2html_prim",tag);
  output_value(s,obj,tagname,classname);
  return VOID;
}

KNO_EXPORT void kno_init_htmlout_c()
{
  lispval fdweb_module=kno_new_module("FDWEB",(0));
  lispval safe_module=kno_new_module("FDWEB",(KNO_MODULE_SAFE));
  lispval xhtml_module=kno_new_module("XHTML",KNO_MODULE_SAFE);

  lispval debug2html = kno_make_cprim2("DEBUGPAGE->HTML",debugpage2html_prim,0);
  lispval backtrace2html = kno_make_cprim2("BACKTRACE->HTML",backtrace2html_prim,0);

  kno_defn(fdweb_module,debug2html);
  kno_defn(fdweb_module,backtrace2html);

  kno_defn(safe_module,debug2html);
  kno_defn(safe_module,backtrace2html);

  kno_defn(xhtml_module,debug2html);
  kno_defn(xhtml_module,backtrace2html);

  kno_def_evalfn(xhtml_module,"TABLE->HTML","",table2html_evalfn);
  kno_idefn(xhtml_module,kno_make_cprim2("OBJ->HTML",obj2html_prim,1));

  kno_decref(debug2html);
  kno_decref(backtrace2html);

  xmloidfn_symbol = kno_intern("%XMLOID");
  id_symbol = kno_intern("%ID");
  href_symbol = kno_intern("HREF");
  class_symbol = kno_intern("CLASS");
  obj_name = kno_intern("OBJ-NAME");
  quote_symbol = kno_intern("QUOTE");
  xmltag_symbol = kno_intern("%XMLTAG");
  rawtag_symbol = kno_intern("%RAWTAG");
  browseinfo_symbol = kno_intern("BROWSEINFO");
  embedded_symbol = kno_intern("%EMBEDDED");
  error_style_symbol = kno_intern("%ERRORSTYLE");
  error_script_symbol = kno_intern("%ERRORSCRIPT");
  modules_symbol = kno_intern("%MODULES");
  xml_env_symbol = kno_intern("%XMLENV");

  moduleid_symbol = kno_intern("%MODULEID");

  kno_register_config
    ("ERRORSTYLESHEET",_("Default style sheet for web errors"),
     kno_sconfig_get,kno_sconfig_set,&error_stylesheet);

  u8_register_source_file(_FILEINFO);

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
