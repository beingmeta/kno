/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1

#include "kno/knosource.h"
#include "kno/dtype.h"
#include "kno/tables.h"
#include "kno/eval.h"
#include "kno/ports.h"
#include "kno/webtools.h"

#include <libu8/u8xfiles.h>
#include <libu8/u8convert.h>
#include <libu8/u8netfns.h>

#include <ctype.h>

/* MAIL output operations */

static lispval mailhost_symbol, maildomain_symbol;
static lispval mailfrom_symbol;

static u8_string mailhost_dflt = NULL, maildomain_dflt = NULL;
static u8_string mailfrom_dflt = NULL;

static void get_mailinfo(lispval headers,u8_string *host,u8_string *domain,u8_string *from)
{
  lispval mailhost_spec = kno_get(headers,mailhost_symbol,VOID);
  lispval maildomain_spec = kno_get(headers,maildomain_symbol,VOID);
  lispval mailfrom_spec = kno_get(headers,mailfrom_symbol,VOID);
  if (STRINGP(mailhost_spec)) *host = CSTRING(mailhost_spec);
  if (STRINGP(maildomain_spec)) *domain = CSTRING(maildomain_spec);
  if (STRINGP(mailfrom_spec)) *from = CSTRING(mailfrom_spec);
}

static lispval smtp_function(lispval dest,lispval headers,lispval content,
                            lispval ctype,lispval mailinfo)
{
  u8_string mailhost = mailhost_dflt, maildomain = maildomain_dflt;
  u8_string mailfrom = mailfrom_dflt;
  lispval header_keys = kno_getkeys(headers);
  int retval, i = 0, n_headers = KNO_CHOICE_SIZE(header_keys), n_to_free = 0;
  struct U8_MAILHEADER *mh = u8_alloc_n(n_headers,struct U8_MAILHEADER);
  u8_string *to_free = u8_alloc_n(n_headers,u8_string);
  DO_CHOICES(header,headers) {
    lispval value = kno_get(headers,header,VOID);
    if (VOIDP(value)) mh[i].label = NULL;
    else if (SYMBOLP(header)) mh[i].label = SYM_NAME(header);
    else if (STRINGP(header)) mh[i].label = CSTRING(header);
    else mh[i].label = NULL;
    if (STRINGP(value)) mh[i].value = CSTRING(value);
    else {
      u8_string data = kno_lisp2string(value);
      to_free[n_to_free++]=data;
      mh[i].value = data;}
    i++;}
  if (VOIDP(mailinfo))
    get_mailinfo(headers,&mailhost,&maildomain,&mailfrom);
  else get_mailinfo(mailinfo,&mailhost,&maildomain,&mailfrom);
  if (mailfrom == NULL) mailfrom="kno";
  retval = u8_smtp(mailhost,maildomain,mailfrom,CSTRING(dest),
                 ((STRINGP(ctype))?(CSTRING(ctype)):(NULL)),
                 n_headers,&mh,CSTRING(content),STRLEN(content));
  while (n_to_free>0) {u8_free(to_free[--n_to_free]);}
  u8_free(mh); u8_free(to_free);
  if (retval<0)
    return KNO_ERROR;
  else return KNO_TRUE;
}

static lispval mailout_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  const u8_byte *mailhost = mailhost_dflt, *maildomain = maildomain_dflt;
  const u8_byte *mailfrom = mailfrom_dflt;
  lispval dest_arg = kno_get_arg(expr,1), headers_arg = kno_get_arg(expr,2);
  lispval body = kno_get_body(expr,3);
  lispval dest, headers, header_fields, result;
  int retval, i = 0, n_headers, n_to_free = 0;
  struct U8_MAILHEADER *mh;
  struct U8_OUTPUT out;
  u8_string *to_free;
  dest = kno_eval(dest_arg,env); if (KNO_ABORTP(dest)) return dest;
  if (SLOTMAPP(headers_arg)) headers = kno_incref(headers_arg);
  else headers = kno_eval(headers_arg,env);
  if (KNO_ABORTP(headers)) {
    kno_decref(dest); return headers;}
  get_mailinfo(headers,&mailhost,&maildomain,&mailfrom);
  if (mailfrom == NULL) mailfrom="kno";
  header_fields = kno_getkeys(headers);
  if (KNO_ABORTP(header_fields)) {
    kno_decref(dest); kno_decref(headers);
    return header_fields;}
  n_headers = KNO_CHOICE_SIZE(header_fields);
  mh = u8_alloc_n(n_headers,struct U8_MAILHEADER);
  to_free = u8_alloc_n(n_headers,u8_string);
  {DO_CHOICES(header,header_fields) {
      lispval value = kno_get(headers,header,VOID);
      if (VOIDP(value)) mh[i].label = NULL;
      else if (SYMBOLP(header)) mh[i].label = SYM_NAME(header);
      else if (STRINGP(header)) mh[i].label = CSTRING(header);
      else mh[i].label = NULL;
      if (STRINGP(value)) mh[i].value = CSTRING(value);
      else {
        u8_string data = kno_lisp2string(value);
        to_free[n_to_free++]=data;
        mh[i].value = data;}
      i++;}}
  U8_INIT_OUTPUT(&out,1024);
  result = kno_printout_to(&out,body,env);
  retval = u8_smtp(mailhost,maildomain,mailfrom,
                 CSTRING(dest),NULL,n_headers,&mh,
                 out.u8_outbuf,out.u8_write-out.u8_outbuf);
  while (n_to_free>0) {u8_free(to_free[--n_to_free]);}
  u8_free(mh); u8_free(to_free); u8_free(out.u8_outbuf);
  kno_decref(header_fields); kno_decref(headers);
  kno_decref(dest);
  if (retval<0) {
    kno_decref(result);
    return KNO_ERROR;}
  else return result;
}

/* Module initialization */

void kno_init_email_c()
{
  lispval unsafe_module = kno_new_cmodule("WEBTOOLS",(0),kno_init_email_c);

  kno_idefn(unsafe_module,kno_make_cprim5("SMTP",smtp_function,3));
  kno_def_evalfn(unsafe_module,"MAILOUT","",mailout_evalfn);

  kno_register_config("MAILHOST",_("SMTP host"),
                     kno_sconfig_get,kno_sconfig_set,&mailhost_dflt);
  kno_register_config("MAILDOMAIN",_("SMTP default domain"),
                     kno_sconfig_get,kno_sconfig_set,&maildomain_dflt);
  kno_register_config("MAILFROM",_("SMTP default from"),
                     kno_sconfig_get,kno_sconfig_set,&mailfrom_dflt);

  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
