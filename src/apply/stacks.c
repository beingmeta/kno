/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2015-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_FCNIDS 1
#define FD_INLINE_STACKS 1
#define FD_INLINE_LEXENV 1
#define FD_INLINE_APPLY  1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/lexenv.h"
#include "framerd/apply.h"

#include <libu8/u8printf.h>
#include <libu8/u8contour.h>
#include <libu8/u8strings.h>

#include <errno.h>

#if HAVE_UNISTD_H
#include <unistd.h>
#endif

#include <stdarg.h>

#if (FD_USE_TLS)
u8_tld_key fd_stackptr_key;
#elif (U8_USE__THREAD)
__thread struct FD_STACK *fd_stackptr=NULL;
#else
struct FD_STACK *fd_stackptr=NULL;
#endif

static void summarize_stack_frame(u8_output out,struct FD_STACK *stack)
{
  if (stack->stack_label)
    u8_puts(out,stack->stack_label);
  if ( (stack->stack_status) &&
       (stack->stack_status[0]) &&
       (stack->stack_status!=stack->stack_label) ) {
    u8_printf(out,"(%s)",stack->stack_status);}
  if ((stack->stack_type) &&
      (strcmp(stack->stack_type,stack->stack_label)))
    u8_printf(out,".%s",stack->stack_type);
}

FD_EXPORT
fdtype fd_get_backtrace(struct FD_STACK *stack,fdtype rep)
{
  if (stack == NULL) stack=fd_stackptr;
  while (stack) {
    u8_string summary=NULL;
    U8_FIXED_OUTPUT(tmp,128);
    if ( (stack->stack_label) || (stack->stack_status) ) {
      summarize_stack_frame(tmpout,stack);
      summary=tmp.u8_outbuf;}
    if (stack->stack_env) {
      fdtype bindings = stack->stack_env->env_bindings;
      if ( (FD_SLOTMAPP(bindings)) || (FD_SCHEMAPP(bindings)) ) {
	rep=fd_init_pair( NULL, fd_copy(bindings), rep);}}
    if ( stack->stack_args ) {
      fdtype *args=stack->stack_args;
      int i=0, n=stack->n_args;
      fdtype tmpbuf[n+1], *write=&tmpbuf[1];
      tmpbuf[0]=stack->stack_op;
      fd_incref(stack->stack_op);
      while (i<n) {
	fdtype v=args[i];
	fd_incref(v);
	*write++=v;
	i++;}
      fdtype vec = fd_make_vector(n+1,tmpbuf);
      rep=fd_init_pair(NULL,vec,rep);}
    else if (!(FD_VOIDP(stack->stack_op)))
      rep = fd_init_pair( NULL, fd_incref(stack->stack_op), rep);
    if (summary)
      rep = fd_init_pair(NULL,fd_make_string(NULL,-1,summary),rep);
    stack=stack->stack_caller;}
  return rep;
}

FD_EXPORT
void fd_sum_backtrace(u8_output out,fdtype backtrace)
{
  if (fd_stacktracep(backtrace)) {
    fdtype scan=backtrace;
    int n=0; while (FD_PAIRP(scan)) {
      fdtype car = FD_CAR(scan);
      if (FD_STRINGP(car)) {
	if (n) u8_puts(out," ⇒ ");
	u8_putn(out,FD_STRDATA(car),FD_STRLEN(car));
	n++;}
      else if (FD_EXCEPTIONP(car)) {
	struct FD_EXCEPTION_OBJECT *exo=
	  (struct FD_EXCEPTION_OBJECT *)car;
	u8_exception ex=exo->fdex_u8ex;
	if (n) u8_puts(out," ⇒ ");
	u8_puts(out,ex->u8x_cond);
	if (ex->u8x_context) u8_printf(out,"@%s",ex->u8x_context);
	if (ex->u8x_details) u8_printf(out," (%s)",ex->u8x_details);
	if (ex->u8x_free_xdata == fd_free_exception_xdata) {
	  fdtype irritant=(fdtype)ex->u8x_xdata;
	  char buf[32]; buf[0]='\0';
	  u8_sprintf(buf,32," =%q",irritant);
	  u8_puts(out,buf);}
	n++;}
      else {}
      scan=FD_CDR(scan);}}
}

static void output_value(u8_output out,fdtype val,
			 u8_string eltname,
			 u8_string classname);

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

static int output_opts(u8_output out,fdtype expr)
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

static void output_value(u8_output out,fdtype val,
			 u8_string eltname,
			 u8_string classname)
{
  if (FD_STRINGP(val))
    if (FD_STRLEN(val)>42)
      u8_printf(out," <%s class='%s long string' title='%d characters'>%s</%s>",
		eltname,classname,
		FD_STRLEN(val),FD_STRDATA(val),
		eltname);
    else u8_printf(out," <%s class='%s string' title='% characters'>%s</%s>",
		   eltname,classname,
		   FD_STRLEN(val),FD_STRDATA(val),
		   eltname);
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
	u8_printf(out,"\n  <div class='%s keyval'>",classname);
	output_value(out,key,"span","key");
	if (FD_CHOICEP(value)) u8_puts(out," <span class='slotvals'>");
	{FD_DO_CHOICES(v,value) {
	    u8_putc(out,' ');
	    output_value(out,value,"span","slotval");}}
	if (FD_CHOICEP(value)) u8_puts(out," </span>");
	u8_printf(out,"</div>");
	fd_decref(value);}
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
      fd_pprint(out,val,"",0,0,60,1);
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

static void output_entry(u8_output out,fdtype entry)
{
  if (FD_STRINGP(entry))
    u8_printf(out,"<h2>%s</h2>",FD_STRDATA(entry));
  else if (FD_VECTORP(entry)) {
    fdtype op=FD_VECTOR_REF(entry,0);
    u8_string opname=NULL, classname="op";
    if (FD_PRIMITIVEP(op)) {
      opname = ((fd_function)op)->fcn_name;
      classname="cprim";}
    else if (FD_TYPEP(op,fd_sproc_type)) {
      opname = ((fd_function)op)->fcn_name;
      classname="lambda";}
    else if (FD_TYPEP(op,fd_ffi_type)) {
      opname = ((fd_function)op)->fcn_name;
      classname="ffi";}
    else opname=NULL;
    if (opname)
      u8_printf(out,"<div class='call'><span class='op %s'>%s</span>",
		classname,opname);
    else u8_printf(out,"<div class='call'><span class='op lisp'>%q</span>",op);
    int i=1, n=FD_VECTOR_LENGTH(entry);
    while (i<n) {
      fdtype arg=FD_VECTOR_REF(entry,i);
      output_value(out,arg,"span","arg");
      i++;}}
  else if (FD_PAIRP(entry)) {
    u8_printf(out,"<pre class='eval'>");
    fd_pprint(out,entry,"",0,0,100,1);
    u8_printf(out,"</pre>");}
  else if (FD_TABLEP(entry)) {
    fdtype vars=fd_getkeys(entry);
    fdtype unbound=FD_EMPTY_CHOICE;
    u8_printf(out,"<div class='bindings'>");
    FD_DO_CHOICES(var,vars) {
      if (FD_SYMBOLP(var)) {
	fdtype val=fd_get(entry,var,FD_VOID);
	if ((val == FD_VOID) || (val == FD_UNBOUND))
	  u8_printf(out,
		    "\n <div class='binding'><span class='varname'>%s</span> "
		    "<span class='unbound'>UNBOUND</span</div>",
		    FD_SYMBOL_NAME(var));
	else {
	  u8_printf(out,"\n <div class='binding'>"
		    "<span class='varname'>%s</span>\n ",
		    FD_SYMBOL_NAME(var));
	  if (FD_CHOICEP(val)) u8_puts(out,"<span class='values'> ");
	  {FD_DO_CHOICES(v,val) {
	      output_value(out,v,"span","value");}}
	  if (FD_CHOICEP(val)) u8_puts(out,"</span> ");
	  u8_printf(out,"</div>");}}}
    u8_printf(out,"\n</div>");}
  else if (FD_EXCEPTIONP(entry)) {
    fd_exception_object exo=
      fd_consptr(fd_exception_object,entry,fd_error_type);
    u8_exception ex = exo->fdex_u8ex;
    u8_printf(out,
	      "<div class='exception'>"
	      "<span class='condition'>%s</span> "
	      "<span class='context'>%s</span>",
	      ex->u8x_cond,ex->u8x_context);
    if (ex->u8x_details) 
      u8_printf(out,"\n<p class='details'>%s</p>",ex->u8x_details);
    fdtype irritant=fd_get_irritant(ex);
    if (!(FD_VOIDP(irritant)))
      output_value(out,irritant,"div","irritant");}
  else {}
}

FD_EXPORT
void fd_html_backtrace(u8_output out,fdtype rep)
{
  fdtype backtrace=FD_EMPTY_LIST;
  {FD_DOLIST(entry,rep) {
      backtrace=fd_init_pair(NULL,fd_incref(entry),backtrace);}}
  {FD_DOLIST(entry,backtrace) {
      output_entry(out,entry);}}
  fd_decref(backtrace);
}

void fd_init_stacks_c()
{
#if (FD_USE_TLS)
  u8_new_threadkey(&fd_stackptr_key,NULL);
#endif
  fd_stackptr=NULL;
}
