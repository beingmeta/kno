/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2016 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/tables.h"
#include "framerd/eval.h"
#include "framerd/fdweb.h"
#include "framerd/ports.h"

#include <libu8/libu8io.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8streamio.h>
#include <libu8/u8netfns.h>

#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/nameser.h>
#include <resolv.h>

static int dns_initialized=0;

static fdtype dns_query(fdtype domain,fdtype class_arg,fdtype type_arg)
{
  int class=-1, type=-1, rv=-1;
  char result[256]; int result_len;

  if (FD_STRINGP(class_arg))
    class=rs_nametoclass(FD_STRDATA(class_arg),NULL);
  else if (FD_SYMBOLP(class_arg))
    class=rs_nametoclass(FD_SYMBOL_NAME(class_arg),NULL);
  else return fd_type_error("DNS class spec","dns_query",class_arg);
  if (class<0) return fd_type_error("DNS class spec","dns_query",class_arg);

  if (FD_STRINGP(type_arg))
    class=rs_nametotype(FD_STRDATA(type_arg),NULL);
  else if (FD_SYMBOLP(type_arg))
    class=rs_nametotype(FD_SYMBOL_NAME(type_arg),NULL);
  else return fd_type_error("DNS type spec","dns_query",type_arg);
  if (type<0) return fd_type_error("DNS type spec","dns_query",type_arg);

  rv=res_search(FD_STRDATA(domain),class,type,result,&result_len);

  return fdstring(result);
}

FD_EXPORT void fd_init_dns_c(void) FD_LIBINIT_FN;

FD_EXPORT void fd_init_dns_c()
{
  fdtype module;
  if (dns_initialized) return;
  dns_initialized=1;
  fd_init_fdscheme();

  module=fd_new_module("FDWEB",(0));

  res_init();

  fd_curl_type=fd_register_cons_type("CURLHANDLE");
  fd_recyclers[fd_curl_type]=recycle_curl_handle;
  fd_unparsers[fd_curl_type]=unparse_curl_handle;

  curl_global_init(CURL_GLOBAL_ALL|CURL_GLOBAL_SSL);
  init_ssl_locks();
  atexit(global_curl_cleanup);

  url_symbol=fd_intern("URL");
  content_type_symbol=fd_intern("CONTENT-TYPE");
  content_length_symbol=fd_intern("CONTENT-LENGTH");
  content_encoding_symbol=fd_intern("CONTENT-ENCODING");
  etag_symbol=fd_intern("ETAG");
  charset_symbol=fd_intern("CHARSET");
  type_symbol=fd_intern("TYPE");
  text_symbol=fd_intern("TEXT");
  content_symbol=fd_intern("%CONTENT");
  header_symbol=fd_intern("HEADER");
  referer_symbol=fd_intern("REFERER");
  method_symbol=fd_intern("METHOD");
  verbose_symbol=fd_intern("VERBOSE");
  useragent_symbol=fd_intern("USERAGENT");
  verifyhost_symbol=fd_intern("VERIFYHOST");
  verifypeer_symbol=fd_intern("VERIFYPEER");
  cainfo_symbol=fd_intern("CAINFO");
  cookie_symbol=fd_intern("COOKIE");
  cookiejar_symbol=fd_intern("COOKIEJAR");
  authinfo_symbol=fd_intern("AUTHINFO");
  basicauth_symbol=fd_intern("BASICAUTH");
  date_symbol=fd_intern("DATE");
  last_modified_symbol=fd_intern("LAST-MODIFIED");
  name_symbol=fd_intern("NAME");
  /* MAXTIME is the maximum time for a result, and TIMEOUT is the max time to
     establish a connection. */
  maxtime_symbol=fd_intern("MAXTIME");
  timeout_symbol=fd_intern("TIMEOUT");
  eurl_slotid=fd_intern("EFFECTIVE-URL");
  filetime_slotid=fd_intern("FILETIME");
  response_code_slotid=fd_intern("RESPONSE");


  FD_ADD_TO_CHOICE(text_types,text_symbol);
  decl_text_type("application/xml");
  decl_text_type("application/rss+xml");
  decl_text_type("application/atom+xml");
  decl_text_type("application/json");

  curl_defaults=fd_empty_slotmap();

  fd_defspecial(module,"URLPOSTOUT",urlpostdata_handler);

  fd_idefn(module,fd_make_cprim2("URLGET",urlget,1));
  fd_idefn(module,fd_make_cprim2("URLHEAD",urlhead,1));
  fd_idefn(module,fd_make_cprimn("URLPOST",urlpost,1));
  fd_idefn(module,fd_make_cprim4("URLPUT",urlput,2));
  fd_idefn(module,fd_make_cprim2("URLCONTENT",urlcontent,1));
  fd_idefn(module,fd_make_cprim3("URLXML",urlxml,1));
  fd_idefn(module,fd_make_cprimn("CURL/OPEN",curlopen,0));
  fd_defalias(module,"CURLOPEN","CURL/OPEN");
  fd_idefn(module,fd_make_cprim3("CURL/SETOPT!",curlsetopt,2));
  fd_defalias(module,"CURLSETOPT!","CURL/SETOPT!");
  fd_idefn(module,fd_make_cprim1x
           ("CURL/RESET!",curlreset,1,fd_curl_type,FD_VOID));
  fd_defalias(module,"CURLRESET!","CURL/RESET!");
  fd_idefn(module,fd_make_cprim1("ADD-TEXT_TYPE!",addtexttype,1));
  fd_idefn(module,fd_make_cprim1("CURL-HANDLE?",curlhandlep,1));

  fd_idefn(module,fd_make_cprim1("RESPONSE/OK?",responseokp,1));
  fd_idefn(module,fd_make_cprim1("RESPONSE/REDIRECT?",
                                 responseredirectp,1));
  fd_idefn(module,fd_make_cprim1("RESPONSE/ERROR?",responseanyerrorp,1));
  fd_idefn(module,fd_make_cprim1("RESPONSE/MYERROR?",responsemyerrorp,1));
  fd_idefn(module,fd_make_cprim1("RESPONSE/SERVERERROR?",
                                 responseservererrorp,1));
  fd_idefn(module,fd_make_cprim1("RESPONSE/UNAUTHORIZED?",
                                 responseunauthorizedp,1));
  fd_idefn(module,fd_make_cprim1("RESPONSE/FORBIDDEN?",
                                 responseforbiddenp,1));
  fd_idefn(module,fd_make_cprim1("RESPONSE/TIMEOUT?",
                                 responsetimeoutp,1));
  fd_idefn(module,fd_make_cprim1("RESPONSE/BADMETHOD?",
                                 responsebadmethodp,1));
  fd_idefn(module,fd_make_cprim1("RESPONSE/NOTFOUND?",
                                 responsenotfoundp,1));
  fd_idefn(module,fd_make_cprim1("RESPONSE/NOTFOUND?",
                                 responsenotfoundp,1));
  fd_idefn(module,fd_make_cprim1("RESPONSE/GONE?",
                                 responsegonep,1));
  fd_idefn(module,fd_make_cprim1("RESPONSE/STATUS",responsestatusprim,1));
  fd_idefn(module,fd_make_ndprim
           (fd_make_cprim3("RESPONSE/STATUS?",testresponseprim,2)));

  fd_register_config
    ("DEBUGCURL",_("Whether to debug low level CURL interaction"),
     fd_boolconfig_get,fd_boolconfig_set,&debugging_curl);


  fd_register_sourcefn(url_sourcefn);

  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
