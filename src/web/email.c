/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/tables.h"
#include "framerd/eval.h"
#include "framerd/ports.h"
#include "framerd/fdweb.h"

#include <libu8/u8xfiles.h>
#include <libu8/u8convert.h>
#include <libu8/u8netfns.h>

#include <ctype.h>

/* MAIL output operations */

static fdtype mailhost_symbol, maildomain_symbol;
static fdtype mailfrom_symbol;

static u8_string mailhost_dflt = NULL, maildomain_dflt = NULL;
static u8_string mailfrom_dflt = NULL;

static void get_mailinfo(fdtype headers,u8_string *host,u8_string *domain,u8_string *from)
{
  fdtype mailhost_spec = fd_get(headers,mailhost_symbol,FD_VOID);
  fdtype maildomain_spec = fd_get(headers,maildomain_symbol,FD_VOID);
  fdtype mailfrom_spec = fd_get(headers,mailfrom_symbol,FD_VOID);
  if (FD_STRINGP(mailhost_spec)) *host = FD_STRDATA(mailhost_spec);
  if (FD_STRINGP(maildomain_spec)) *domain = FD_STRDATA(maildomain_spec);
  if (FD_STRINGP(mailfrom_spec)) *from = FD_STRDATA(mailfrom_spec);
}

static fdtype smtp_function(fdtype dest,fdtype headers,fdtype content,
                            fdtype ctype,fdtype mailinfo)
{
  u8_string mailhost = mailhost_dflt, maildomain = maildomain_dflt;
  u8_string mailfrom = mailfrom_dflt;
  fdtype header_keys = fd_getkeys(headers);
  int retval, i = 0, n_headers = FD_CHOICE_SIZE(header_keys), n_to_free = 0;
  struct U8_MAILHEADER *mh = u8_alloc_n(n_headers,struct U8_MAILHEADER);
  u8_string *to_free = u8_alloc_n(n_headers,u8_string);
  FD_DO_CHOICES(header,headers) {
    fdtype value = fd_get(headers,header,FD_VOID);
    if (FD_VOIDP(value)) mh[i].label = NULL;
    else if (FD_SYMBOLP(header)) mh[i].label = FD_SYMBOL_NAME(header);
    else if (FD_STRINGP(header)) mh[i].label = FD_STRDATA(header);
    else mh[i].label = NULL;
    if (FD_STRINGP(value)) mh[i].value = FD_STRDATA(value);
    else {
      u8_string data = fd_dtype2string(value);
      to_free[n_to_free++]=data;
      mh[i].value = data;}
    i++;}
  if (FD_VOIDP(mailinfo))
    get_mailinfo(headers,&mailhost,&maildomain,&mailfrom);
  else get_mailinfo(mailinfo,&mailhost,&maildomain,&mailfrom);
  if (mailfrom == NULL) mailfrom="framerd";
  retval = u8_smtp(mailhost,maildomain,mailfrom,FD_STRDATA(dest),
                 ((FD_STRINGP(ctype))?(FD_STRDATA(ctype)):(NULL)),
                 n_headers,&mh,FD_STRDATA(content),FD_STRLEN(content));
  while (n_to_free>0) {u8_free(to_free[--n_to_free]);}
  u8_free(mh); u8_free(to_free);
  if (retval<0)
    return FD_ERROR_VALUE;
  else return FD_TRUE;
}

static fdtype mailout_evalfn(fdtype expr,fd_lexenv env,fd_stack _stack)
{
  const u8_byte *mailhost = mailhost_dflt, *maildomain = maildomain_dflt;
  const u8_byte *mailfrom = mailfrom_dflt;
  fdtype dest_arg = fd_get_arg(expr,1), headers_arg = fd_get_arg(expr,2);
  fdtype body = fd_get_body(expr,3);
  fdtype dest, headers, header_fields, result;
  int retval, i = 0, n_headers, n_to_free = 0;
  struct U8_MAILHEADER *mh;
  struct U8_OUTPUT out;
  u8_string *to_free;
  dest = fd_eval(dest_arg,env); if (FD_ABORTP(dest)) return dest;
  if (FD_SLOTMAPP(headers_arg)) headers = fd_incref(headers_arg);
  else headers = fd_eval(headers_arg,env);
  if (FD_ABORTP(headers)) {
    fd_decref(dest); return headers;}
  get_mailinfo(headers,&mailhost,&maildomain,&mailfrom);
  if (mailfrom == NULL) mailfrom="framerd";
  header_fields = fd_getkeys(headers);
  if (FD_ABORTP(header_fields)) {
    fd_decref(dest); fd_decref(headers);
    return header_fields;}
  n_headers = FD_CHOICE_SIZE(header_fields);
  mh = u8_alloc_n(n_headers,struct U8_MAILHEADER);
  to_free = u8_alloc_n(n_headers,u8_string);
  {FD_DO_CHOICES(header,header_fields) {
      fdtype value = fd_get(headers,header,FD_VOID);
      if (FD_VOIDP(value)) mh[i].label = NULL;
      else if (FD_SYMBOLP(header)) mh[i].label = FD_SYMBOL_NAME(header);
      else if (FD_STRINGP(header)) mh[i].label = FD_STRDATA(header);
      else mh[i].label = NULL;
      if (FD_STRINGP(value)) mh[i].value = FD_STRDATA(value);
      else {
        u8_string data = fd_dtype2string(value);
        to_free[n_to_free++]=data;
        mh[i].value = data;}
      i++;}}
  U8_INIT_OUTPUT(&out,1024);
  result = fd_printout_to(&out,body,env);
  retval = u8_smtp(mailhost,maildomain,mailfrom,
                 FD_STRDATA(dest),NULL,n_headers,&mh,
                 out.u8_outbuf,out.u8_write-out.u8_outbuf);
  while (n_to_free>0) {u8_free(to_free[--n_to_free]);}
  u8_free(mh); u8_free(to_free); u8_free(out.u8_outbuf);
  fd_decref(header_fields); fd_decref(headers);
  fd_decref(dest);
  if (retval<0) {
    fd_decref(result);
    return FD_ERROR_VALUE;}
  else return result;
}

/* Module initialization */

void fd_init_email_c()
{
  fdtype unsafe_module = fd_new_module("FDWEB",(0));

  fd_idefn(unsafe_module,fd_make_cprim5("SMTP",smtp_function,3));
  fd_defspecial(unsafe_module,"MAILOUT",mailout_evalfn);

  fd_register_config("MAILHOST",_("SMTP host"),
                     fd_sconfig_get,fd_sconfig_set,&mailhost_dflt);
  fd_register_config("MAILDOMAIN",_("SMTP default domain"),
                     fd_sconfig_get,fd_sconfig_set,&maildomain_dflt);
  fd_register_config("MAILFROM",_("SMTP default from"),
                     fd_sconfig_get,fd_sconfig_set,&mailfrom_dflt);

  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
