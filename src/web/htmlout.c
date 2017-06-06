/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
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

/* If you  edit this file, you probably also want to edit bugjar.css */
#include "backtrace_css.h"
#include "backtrace_js.h"

#include "framerd/support.h"

#include <ctype.h>

#define fast_eval(x,env) (fd_stack_eval(x,env,_stack,0))

#include <libu8/u8xfiles.h>

static void output_value(u8_output out,fdtype val,
			 u8_string eltname,
			 u8_string classname);
FD_EXPORT void fd_html_exception(u8_output s,u8_exception ex,int backtrace);

static u8_string error_stylesheet;

static fdtype xmloidfn_symbol, obj_name, id_symbol, quote_symbol;
static fdtype href_symbol, class_symbol, rawtag_symbol, browseinfo_symbol;
static fdtype embedded_symbol, estylesheet_symbol, xmltag_symbol;
static fdtype modules_symbol, xml_env_symbol;

static void start_errorpage(u8_output s,u8_exception ex)
{
  int isembedded = 0, customstylesheet = 0;
  s->u8_write = s->u8_outbuf;
  fdtype embeddedp = fd_req_get(embedded_symbol,FD_VOID);
  fdtype estylesheet = fd_req_get(estylesheet_symbol,FD_VOID);
  if ((FD_NOVOIDP(embeddedp)) || (FD_FALSEP(embeddedp))) isembedded = 1;
  if (FD_STRINGP(embeddedp)) u8_puts(s,FD_STRDATA(embeddedp));
  if (FD_STRINGP(estylesheet)) {
    u8_puts(s,FD_STRDATA(estylesheet));
    customstylesheet = 1;}
  if (isembedded==0) {
    u8_printf(s,"%s\n%s\n",DEFAULT_DOCTYPE,DEFAULT_XMLPI);
    u8_printf(s,"<html>\n<head>\n<title>");
    if (ex->u8x_cond)
      u8_printf(s,"%k",ex->u8x_cond);
    else u8_printf(s,"Unknown Exception");
    if (ex->u8x_context) u8_printf(s," @%k",ex->u8x_context);
    if (ex->u8x_details) {
      if (strlen(ex->u8x_details)<40)
        u8_printf(s," (%s)",ex->u8x_details);}
    u8_puts(s,"</title>\n");
    u8_printf(s,"\n<style type='text/css'>%s</style>\n",FD_BACKTRACE_CSS);
    u8_printf(s,"\n<script language='javascript'>\n%s\n</script>\n",
              FD_BACKTRACE_JS);
    if (customstylesheet==0)
      u8_printf(s,"<link rel='stylesheet' type='text/css' href='%s'/>\n",
                error_stylesheet);
    u8_puts(s,"</head>\n");}
  u8_puts(s,"<body id='ERRORPAGE'>\n");
}

FD_EXPORT
void fd_xhtmldebugpage(u8_output s,u8_exception ex)
{
  start_errorpage(s,ex);

  u8_puts(s,"<p class='sorry'>"
          "Sorry, there was an unexpected error processing your request"
          "</p>\n");

  fdtype backtrace = fd_exception_backtrace(ex);
  if (FD_PAIRP(backtrace)) {
    u8_puts(s,"<div class='backtrace'>\n");
    fd_html_backtrace(s,backtrace);
    u8_puts(s,"</div>\n");}

  u8_puts(s,"</body>\n</html>\n");
}

FD_EXPORT
void fd_xhtmlerrorpage(u8_output s,u8_exception ex)
{
  start_errorpage(s,ex);

  u8_puts(s,"<p class='sorry'>"
          "Sorry, there was an unexpected error processing your request"
          "</p>\n");

  fd_html_exception(s,ex,0);

  u8_puts(s,"</body>\n</html>\n");
}

static fdtype debugpage2html_prim(fdtype exception,fdtype where)
{
  u8_exception ex;
  if ((FD_VOIDP(exception))||(FD_FALSEP(exception)))
    ex = u8_current_exception;
  else if (FD_TYPEP(exception,fd_error_type)) {
    struct FD_EXCEPTION_OBJECT *xo=
      fd_consptr(struct FD_EXCEPTION_OBJECT *,exception,fd_error_type);
    ex = xo->fdex_u8ex;}
  else {
    u8_log(LOG_WARN,"debugpage2html_prim","Bad exception argument %q",exception);
    ex = u8_current_exception;}
  if ((FD_VOIDP(where))||(FD_TRUEP(where))) {
    u8_output s = u8_current_output;
    fd_xhtmldebugpage(s,ex);
    return FD_TRUE;}
  else if (FD_FALSEP(where)) {
    struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,4096);
    fd_xhtmldebugpage(&out,ex);
    return fd_init_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);}
  else return FD_FALSE;
}

static fdtype backtrace2html_prim(fdtype arg,fdtype where)
{
  fdtype backtrace=FD_VOID; u8_exception ex;
  if ((FD_VOIDP(arg))||(FD_FALSEP(arg))||(arg == FD_DEFAULT_VALUE)) {
    ex = u8_current_exception;
    if (ex) backtrace=fd_exception_backtrace(ex);}
  else if (FD_PAIRP(arg))
    backtrace=arg;
  else if (FD_TYPEP(arg,fd_error_type)) {
    struct FD_EXCEPTION_OBJECT *xo=
      fd_consptr(struct FD_EXCEPTION_OBJECT *,arg,fd_error_type);
    ex = xo->fdex_u8ex;
    if (ex) backtrace=fd_exception_backtrace(ex);}
  else return fd_err("Bad exception/backtrace","backtrace2html_prim",
                     NULL,arg);
  if (!(FD_PAIRP(backtrace)))
    return FD_VOID;
  else if ((FD_VOIDP(where))||(FD_TRUEP(where))) {
    u8_output s = u8_current_output;
    fd_html_backtrace(s,backtrace);
    return FD_TRUE;}
  else if (FD_FALSEP(where)) {
    struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,4096);
    fd_html_backtrace(&out,backtrace);
    return fd_init_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);}
  else return FD_FALSE;
}

/* Output Scheme objects, mostly as tables */

FD_EXPORT void fd_dtype2html(u8_output s,fdtype v,u8_string tag,u8_string cl)
{
  output_value(s,v,tag,cl);
}

/* XHTML error report */

static fdtype moduleid_symbol;

static int isexprp(fdtype expr)
{
  FD_DOLIST(elt,expr) {
    if ( (FD_PAIRP(elt)) || (FD_CHOICEP(elt)) ||
	 (FD_SLOTMAPP(elt)) || (FD_SCHEMAPP(elt)) ||
	 (FD_VECTORP(elt)) || (FD_CODEP(elt)))
      return 0;}
  return 1;
}

static int isoptsp(fdtype expr)
{
  if (FD_PAIRP(expr))
    if ((FD_SYMBOLP(FD_CAR(expr))) && (!(FD_PAIRP(FD_CDR(expr)))))
      return 1;
    else return isoptsp(FD_CAR(expr)) && isoptsp(FD_CDR(expr));
  else if ( (FD_CONSTANTP(expr)) || (FD_SYMBOLP(expr)) )
    return 1;
  else if ( (FD_SCHEMAPP(expr)) || (FD_SLOTMAPP(expr)) )
    return 1;
  else return 0;
}

static void output_opts(u8_output out,fdtype expr)
{
  if (FD_PAIRP(expr))
    if ( (FD_SYMBOLP(FD_CAR(expr))) && (!(FD_PAIRP(FD_CDR(expr)))) ) {
      u8_printf(out,"\n <tr><th class='optname'>%s</th>\n       ",
		FD_SYMBOL_NAME(FD_CAR(expr)));
      output_value(out,FD_CDR(expr),"td","optval");
      u8_printf(out,"</tr>");}
    else {
      output_opts(out,FD_CAR(expr));
      output_opts(out,FD_CDR(expr));}
  else if (FD_EMPTY_LISTP(expr)) {}
  else if (FD_SYMBOLP(expr))
    u8_printf(out,"\n <tr><th class='optname'>%s</th><td>%s</td></tr>",
	      FD_SYMBOL_NAME(expr),FD_SYMBOL_NAME(expr));
  else if ( (FD_SCHEMAPP(expr)) || (FD_SLOTMAPP(expr)) ) {
    fdtype keys=fd_getkeys(expr);
    FD_DO_CHOICES(key,keys) {
      fdtype optval=fd_get(expr,key,FD_VOID);
      if (FD_SYMBOLP(key))
	u8_printf(out,"\n <tr><th class='optname'>%s</th>",
		  FD_SYMBOL_NAME(expr));
      else u8_printf(out,"\n <tr><th class='optkey'>%q</th>",expr);
      output_value(out,optval,"td","optval");
      u8_printf(out,"</tr>");
      fd_decref(optval);}
    fd_decref(keys);}
}


FD_EXPORT
void fd_html_exception(u8_output s,u8_exception ex,int backtrace)
{
  fdtype irritant=fd_get_irritant(ex);
  u8_string i_string=NULL; int overflow=0;
  U8_FIXED_OUTPUT(tmp,32);
  if (!(FD_VOIDP(irritant))) {
    fd_unparse(&tmp,irritant);
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
    if ( (!(FD_VOIDP(irritant))) && (overflow) )
      u8_printf(s,"\n<pre class='irritant'>%Q</pre>",irritant);
  }
  u8_puts(s,"\n</div>\n"); /* exception */
  if (backtrace) {
    fdtype backtrace=fd_exception_backtrace(ex);
    if (FD_PAIRP(backtrace))
      fd_html_backtrace(s,backtrace);
    fd_decref(backtrace);}
}

static void output_value(u8_output out,fdtype val,
			 u8_string eltname,
			 u8_string classname)
{
  if (FD_STRINGP(val))
    if (FD_STRLEN(val)>42)
      u8_printf(out," <%s class='%s long string' title='%d characters'>%q</%s>",
		eltname,classname,FD_STRLEN(val),val,eltname);
    else u8_printf(out," <%s class='%s string' title='% characters'>%q</%s>",
		   eltname,classname,FD_STRLEN(val),val,eltname);
  else if (FD_SYMBOLP(val))
    u8_printf(out," <%s class='%s symbol'>%s</%s>",
	      eltname,classname,FD_SYMBOL_NAME(val),eltname);
  else if (FD_NUMBERP(val))
    u8_printf(out," <%s class='%s number'>%q</%s>",
	      eltname,classname,val,eltname);
  else if (FD_VECTORP(val)) {
    int len=FD_VECTOR_LENGTH(val);
    if (len<2) {
      u8_printf(out," <ol class='%s short vector'>#(",classname);
      if (len==1) output_value(out,FD_VECTOR_REF(val,0),"span","vecelt");
      u8_printf(out,")</ol>");}
    else {
      u8_printf(out," <ol class='%s vector'>#(",classname);
      int i=0; while (i<len) {
	if (i>0) u8_putc(out,' ');
	output_value(out,FD_VECTOR_REF(val,i),"li","vecelt");
	i++;}
      u8_printf(out,")</ol>");}}
  else if ( (FD_SLOTMAPP(val)) || (FD_SCHEMAPP(val)) ) {
    fdtype keys=fd_getkeys(val);
    int n_keys=FD_CHOICE_SIZE(keys);
    if (n_keys==0)
      u8_printf(out," <%s class='%s map'>#[]</%s>",eltname,classname,eltname);
    else if (n_keys==1) {
      fdtype value=fd_get(val,keys,FD_VOID);
      u8_printf(out," <%s class='%s map'>#[<span class='slotid'>%q</span> ",
		eltname,classname,keys);
      output_value(out,value,"span","slotvalue");
      u8_printf(out," ]</%s>",eltname);
      fd_decref(value);}
    else {
      u8_printf(out,"\n<div class='%s map'>",classname);
      int i=0; FD_DO_CHOICES(key,keys) {
	fdtype value=fd_get(val,key,FD_VOID);
        u8_printf(out,"\n  <div class='%s keyval keyval%d'>",classname,i);
	output_value(out,key,"span","key");
	if (FD_CHOICEP(value)) u8_puts(out," <span class='slotvals'>");
        {FD_DO_CHOICES(v,value) {
	    u8_putc(out,' ');
	    output_value(out,value,"span","slotval");}}
	if (FD_CHOICEP(value)) u8_puts(out," </span>");
	u8_printf(out,"</div>");
        fd_decref(value);
        i++;}
      u8_printf(out,"\n</div>",classname);}}
  else if (FD_PAIRP(val)) {
    u8_string tmp = fd_dtype2string(val);
    if (strlen(tmp)< 50)
      u8_printf(out,"<%s class='%s listval'>%s</%s>",
		eltname,classname,tmp,eltname);
    else if (isoptsp(val)) {
      u8_printf(out,"\n<table class='%s opts'>",classname);
      output_opts(out,val);
      u8_printf(out,"\n</table>\n",classname);}
    else if (isexprp(val)) {
      u8_printf(out,"\n<pre class='listexpr'>");
      fd_pprint(out,val,"",0,0,60);
      u8_printf(out,"\n</pre>");}
    else {
      fdtype scan=val;
      u8_printf(out," <ol class='%s list'>",classname);
      while (FD_PAIRP(scan)) {
	fdtype car=FD_CAR(val); scan=FD_CDR(scan);
	output_value(out,car,"li","listelt");}
      if (!(FD_EMPTY_LISTP(scan)))
	output_value(out,scan,"li","cdrelt");
      u8_printf(out,"\n</ol>");}
    u8_free(tmp);}
  else if (FD_CHOICEP(val)) {
    int size=FD_CHOICE_SIZE(val), i=0;
    if (size<7)
      u8_printf(out," <ul class='%s short choice'>{",classname);
    else u8_printf(out," <ul class='%s choice'>{",classname);
    FD_DO_CHOICES(elt,val) {
      if (i>0) u8_putc(out,' ');
      output_value(out,elt,"li","choicelt");
      i++;}
    u8_printf(out,"}</ul>");}
  else if (FD_PACKETP(val))
    if (FD_PACKET_LENGTH(val)>128)
      u8_printf(out," <%s class='%s long packet'>%q</%s>",
		eltname,classname,val,eltname);
    else u8_printf(out," <%s class='%s packet'>%q</%s>",
		   eltname,classname,val,eltname);
  else {
    fd_ptr_type ptrtype=FD_PTR_TYPE(val);
    if (fd_type_names[ptrtype])
      u8_printf(out," <%s class='%s %s'>%q</%s>",
		eltname,classname,fd_type_names[ptrtype],
		val,eltname);}
}

#define INTVAL(x)    ((FD_FIXNUMP(x))?(FD_INT(x)):(-1))
#define STRINGVAL(x) ((FD_STRINGP(x))?(FD_STRDATA(x)):((u8_string)"uninitialized"))

static void output_stack_frame(u8_output out,fdtype entry)
{
  if (FD_EXCEPTIONP(entry)) {
    fd_exception_object exo=
      fd_consptr(fd_exception_object,entry,fd_error_type);
    u8_exception ex = exo->fdex_u8ex;
    fd_html_exception(out,ex,0);}
  else if ((FD_VECTORP(entry)) && (FD_VECTOR_LENGTH(entry)>=7)) {
    fdtype depth=FD_VECTOR_REF(entry,0);
    fdtype type=FD_VECTOR_REF(entry,1);
    fdtype label=FD_VECTOR_REF(entry,2);
    fdtype status=FD_VECTOR_REF(entry,3);
    fdtype op=FD_VECTOR_REF(entry,4);
    fdtype args=FD_VECTOR_REF(entry,5);
    fdtype env=FD_VECTOR_REF(entry,6);
    fdtype source=FD_VECTOR_REF(entry,7);
    u8_puts(out,"<div class='stackframe'>\n");
    u8_printf(out,
              "  <div class='head'>"
              "   <span class='label'>%s</span>"
              "   <span class='depth'>%d</span>"
              "   <span class='type'>%s</span>",
              STRINGVAL(label),FD_INT(depth),STRINGVAL(type));
    if (FD_STRINGP(status))
      u8_printf(out,"\n  <p class='status'>%s</p>\n",FD_STRDATA(status));
    u8_puts(out,"</div>");
    if (FD_PAIRP(source))
      u8_printf(out,"\n  <pre class='source'>\n%Q\n</pre>");
    if (FD_FALSEP(args)) {
      if (FD_PAIRP(op)) {
        u8_puts(out,"\n  <pre class='eval expr'>\n");
        fd_pprint(out,op,NULL,0,0,100);
        u8_puts(out,"\n  </pre>");}
      else u8_printf(out,"\n  <div class='eval'>%q</div>",op);}
    else {
      u8_puts(out,"\n  <div class='call'>\n");
      output_value(out,op,"span","handler");
      int i=0, n=FD_VECTOR_LENGTH(args);
      while (i<n) {
        fdtype arg=FD_VECTOR_REF(args,i);
        output_value(out,arg,"span","arg");
        i++;}}
    if (FD_TABLEP(env)) {
      fdtype vars=fd_getkeys(env);
      u8_printf(out,"<div class='bindings'>");
      FD_DO_CHOICES(var,vars) {
        if (FD_SYMBOLP(var)) {
          fdtype val=fd_get(env,var,FD_VOID);
          u8_puts(out,"\n <div class='binding'>");
          if ((val == FD_VOID) || (val == FD_UNBOUND))
            u8_printf(out,"<span class='varname'>%s</span> "
                      "<span class='evalsto'>⇒</span> "
                      "<span class='unbound'>UNBOUND</span>",
                      FD_SYMBOL_NAME(var));
          else {
            u8_printf(out,"<span class='varname'>%s</span> "
                      "<span class='evalsto'>⇒</span> ",
                      FD_SYMBOL_NAME(var));
            { if (FD_CHOICEP(val))
                u8_printf(out,
                          "<span class='values'> "
                          "<span class='nvals'>(%d values)</span> ",
                          FD_CHOICE_SIZE(val));
              {FD_DO_CHOICES(v,val) {
                  output_value(out,v,"span","value");}}
              if (FD_CHOICEP(val)) u8_puts(out," </span> ");}}
          u8_puts(out,"</div>");}}
      u8_printf(out,"\n</div>");}}
  else {
    u8_puts(out,"\n <pre class='lispobj'>\n");
    fd_pprint(out,entry,NULL,0,0,80);
    u8_puts(out,"\n</div>\n");}
}

FD_EXPORT
void fd_html_backtrace(u8_output out,fdtype rep)
{
  fdtype backtrace=FD_EMPTY_LIST;
  /* Reverse the list */
  {FD_DOLIST(entry,rep) {
      backtrace=fd_init_pair(NULL,fd_incref(entry),backtrace);}}
  /* Output the backtrace */
  fdtype scan=backtrace; while (FD_PAIRP(scan)) {
    fdtype entry=FD_CAR(scan); scan=FD_CDR(scan);
    output_stack_frame(out,entry);}
  /* Free what you reversed above */
  fd_decref(backtrace);
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
      fdtype values = fd_simplify_choice(_value);
      u8_printf(out,"  <tr><th>");
      fd_xmlout_helper(out,NULL,key,xmloidfn,NULL);
      if (FD_EMPTY_CHOICEP(values))
        u8_printf(out,"</th>\n    <td class='novalues'>No values</td></tr>\n");
      else if (FD_CHOICEP(values)) {
        int first_item = 1;
        FD_DO_CHOICES(value,values) {
          if (first_item) {
            u8_puts(out,"</th>\n    <td><div class='value'>");
            first_item = 0;}
          else
            u8_puts(out,"        <div class='value'>");
          fd_xmlout_helper(out,NULL,value,xmloidfn,NULL);
          u8_puts(out,"</div> \n");}
        u8_printf(out,"    </td></tr>\n");}
      else {
        u8_printf(out,"</th>\n      <td class='singlevalue'>");
        fd_xmlout_helper(out,NULL,values,xmloidfn,NULL);
        u8_printf(out,"</td></tr>\n");}}}
  u8_printf(out,"</table>\n");
}

static fdtype table2html_evalfn(fdtype expr,fd_lexenv env,fd_stack _stack)
{
  u8_string classname = NULL;
  U8_OUTPUT *out = u8_current_output;
  fdtype xmloidfn = fd_symeval(xmloidfn_symbol,env);
  fdtype tables, classarg, slotids;
  tables = fd_eval(fd_get_arg(expr,1),env);
  if (FD_ABORTP(tables))return tables;
  else if (FD_VOIDP(tables))
    return fd_err(fd_SyntaxError,"table2html_evalfn",NULL,expr);
  classarg = fd_eval(fd_get_arg(expr,2),env);
  if (FD_ABORTP(classarg)) {
    fd_decref(tables); return classarg;}
  else if (FD_STRINGP(classarg)) classname = FD_STRDATA(classarg);
  else if ((FD_VOIDP(classarg)) || (FD_FALSEP(classarg))) {}
  else {
    fd_decref(tables);
    return fd_type_error(_("string"),"table2html_evalfn",classarg);}
  slotids = fd_eval(fd_get_arg(expr,3),env);
  if (FD_ABORTP(slotids)) {
    fd_decref(tables); fd_decref(classarg); return slotids;}
  {
    FD_DO_CHOICES(table,tables)
      if (FD_TABLEP(table)) {
        fdtype keys = ((FD_VOIDP(slotids)) ? (fd_getkeys(table)) : (slotids));
        if (classname)
          output_xhtml_table(out,table,keys,classname,xmloidfn);
        else if (FD_OIDP(table))
          output_xhtml_table(out,table,keys,"frame_table",xmloidfn);
        else output_xhtml_table(out,table,keys,"table_table",xmloidfn);}
      else {
        fd_decref(tables); fd_decref(classarg); fd_decref(slotids);
        return fd_type_error(_("table"),"table2html_evalfn",table);}}
  return FD_VOID;
}

static fdtype obj2html_prim(fdtype obj,fdtype tag)
{
  u8_string tagname = NULL, classname = NULL; u8_byte tagbuf[64];
  U8_OUTPUT *s = u8_current_output;
  if (FD_STRINGP(tag)) {
    u8_string s = FD_STRDATA(tag);
    u8_string dot = strchr(s,'.');
    if ((dot)&&((dot-s)>50))
      return fd_type_error("HTML tag.class","obj2html_prim",tag);
    else if (dot) {
      memcpy(tagbuf,s,dot-s); tagbuf[dot-s]='\0';
      tagname = tagbuf; classname = dot+1;}
    else tagname = s;}
  else if (FD_SYMBOLP(tag)) tagname = FD_SYMBOL_NAME(tag);
  else if ((FD_VOIDP(tag))||(FD_FALSEP(tag))) {}
  else return fd_type_error("HTML tag.class","obj2html_prim",tag);
  output_value(s,obj,tagname,classname);
  return FD_VOID;
}

FD_EXPORT void fd_init_htmlout_c()
{
  fdtype fdweb_module=fd_new_module("FDWEB",(0));
  fdtype safe_module=fd_new_module("FDWEB",(FD_MODULE_SAFE));
  fdtype xhtml_module=fd_new_module("XHTML",FD_MODULE_SAFE);

  fdtype debug2html = fd_make_cprim2("DEBUGPAGE->HTML",debugpage2html_prim,0);
  fdtype backtrace2html = fd_make_cprim2("BACKTRACE->HTML",backtrace2html_prim,0);

  fd_idefn(fdweb_module,debug2html);
  fd_idefn(fdweb_module,backtrace2html);

  fd_idefn(safe_module,debug2html);
  fd_idefn(safe_module,backtrace2html);

  fd_idefn(xhtml_module,debug2html);
  fd_idefn(xhtml_module,backtrace2html);

  fd_defspecial(xhtml_module,"TABLE->HTML",table2html_evalfn);
  fd_idefn(xhtml_module,fd_make_cprim2("OBJ->HTML",obj2html_prim,1));

  fd_decref(debug2html);
  fd_decref(backtrace2html);

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

  moduleid_symbol = fd_intern("%MODULEID");

  error_stylesheet = u8_strdup("http://static.beingmeta.com/fdjt/fdjt.css");
  fd_register_config
    ("ERRORSTYLESHEET",_("Default style sheet for web errors"),
     fd_sconfig_get,fd_sconfig_set,&error_stylesheet);

  u8_register_source_file(_FILEINFO);

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
