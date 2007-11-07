/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2007 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id: mime.c 2034 2007-11-05 11:46:39Z haase $";

#define U8_INLINE_IO 1

#include "fdb/dtype.h"
#include "fdb/tables.h"
#include "fdb/eval.h"
#include "fdb/ports.h"
#include "fdb/fdweb.h"

#include <libu8/xfiles.h>
#include <libu8/u8convert.h>
#include <libu8/u8netfns.h>

#include <ctype.h>

/* MAIL output operations */

static fdtype mailhost_symbol, maildomain_symbol;
static fdtype mailfrom_symbol, ctype_symbol;

static fdtype smtp_function(fdtype dest,fdtype headers,fdtype content,fdtype ctype,fdtype mailinfo)
{
  char *mailhost=NULL, *maildomain=NULL, *mailfrom=NULL;
  fdtype mailhost_spec=fd_get(mailinfo,mailhost_symbol,FD_VOID);
  fdtype maildomain_spec=fd_get(mailinfo,maildomain_symbol,FD_VOID);
  fdtype mailfrom_spec=fd_get(mailinfo,mailfrom_symbol,FD_VOID);
  fdtype header_keys=fd_getkeys(headers);
  int retval, i=0, n_headers=FD_CHOICE_SIZE(header_keys), n_to_free=0;
  struct U8_MAILHEADER *mh=u8_alloc_n(n_headers,struct U8_MAILHEADER);
  u8_string *to_free=u8_alloc_n(n_headers,u8_string);
  FD_DO_CHOICES(header,headers) {
    fdtype value=fd_get(headers,header,FD_VOID);
    if (FD_VOIDP(value)) mh[i].label=NULL;
    else if (FD_SYMBOLP(header)) mh[i].label=FD_SYMBOL_NAME(header);
    else if (FD_STRINGP(header)) mh[i].label=FD_STRDATA(header);
    else mh[i].label=NULL;
    if (FD_STRINGP(value)) mh[i].value=FD_STRDATA(value);
    else {
      u8_string data=fd_dtype2string(value);
      to_free[n_to_free++]=data;
      mh[i].value=data;}
    i++;}
  if (FD_STRINGP(mailhost_spec)) mailhost=FD_STRDATA(mailhost_spec);
  if (FD_STRINGP(maildomain_spec)) maildomain=FD_STRDATA(maildomain_spec);
  if (FD_STRINGP(mailfrom_spec)) mailfrom=FD_STRDATA(mailfrom_spec);
  retval=u8_smtp(mailhost,maildomain,mailfrom,FD_STRDATA(dest),
		 ((FD_STRINGP(ctype))?(FD_STRDATA(ctype)):(NULL)),
		 n_headers,&mh,FD_STRDATA(content));
  while (n_to_free>0) {u8_free(to_free[--n_to_free]);}
  u8_free(mh); u8_free(to_free);
  if (retval<0)
    return FD_ERROR_VALUE;
  else return FD_TRUE;
}

static fdtype mailout_handler(fdtype expr,fd_lispenv env)
{
  fdtype dest_arg=fd_get_arg(expr,1), headers_arg=fd_get_arg(expr,2), body=fd_get_body(expr,3);
  fdtype dest, headers, header_fields, result;
  int retval, i=0, n_headers, n_to_free=0; 
  struct U8_MAILHEADER *mh;
  struct U8_OUTPUT out;
  u8_string *to_free;
  dest=fd_eval(dest_arg,env); if (FD_ABORTP(dest)) return dest;
  headers=fd_eval(headers_arg,env); if (FD_ABORTP(headers)) {
    fd_decref(dest); return headers;}
  header_fields=fd_getkeys(headers);
  if (FD_ABORTP(header_fields)) {
    fd_decref(dest); fd_decref(headers); return header_fields;}
  n_headers=FD_CHOICE_SIZE(header_fields);
  mh=u8_alloc_n(n_headers,struct U8_MAILHEADER);
  to_free=u8_alloc_n(n_headers,u8_string);
  {FD_DO_CHOICES(header,header_fields) {
      fdtype value=fd_get(headers,header,FD_VOID);
      if (FD_VOIDP(value)) mh[i].label=NULL;
      else if (FD_SYMBOLP(header)) mh[i].label=FD_SYMBOL_NAME(header);
      else if (FD_STRINGP(header)) mh[i].label=FD_STRDATA(header);
      else mh[i].label=NULL;
      if (FD_STRINGP(value)) mh[i].value=FD_STRDATA(value);
      else {
	u8_string data=fd_dtype2string(value);
	to_free[n_to_free++]=data;
	mh[i].value=data;}
      i++;}}
  result=fd_printout_to(&out,body,env);
  retval=u8_smtp(NULL,NULL,NULL,FD_STRDATA(dest),NULL,
		 n_headers,&mh,out.u8_outbuf);
  while (n_to_free>0) {u8_free(to_free[--n_to_free]);}
  u8_free(mh); u8_free(to_free);
  if (retval<0) {
    fd_decref(result);
    return FD_ERROR_VALUE;}
  else return result;
}

/* Module initialization */

void fd_init_email_c()
{
  fdtype module=fd_new_module("FDWEB",(FD_MODULE_DEFAULT|FD_MODULE_SAFE));
  fdtype unsafe_module=fd_new_module("FDWEB",(FD_MODULE_DEFAULT));

  fd_idefn(unsafe_module,fd_make_cprim5("SMTP",smtp_function,3));
  fd_defspecial(unsafe_module,"MAILOUT",mailout_handler);

  fd_register_source_file(versionid);
}
