/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2013 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1
#define FD_PROVIDE_FASTEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/tables.h"
#include "framerd/eval.h"
#include "framerd/fddb.h"
#include "framerd/ports.h"
#include "framerd/fdweb.h"
#include "framerd/support.h"

#include <libu8/libu8.h>
#include <libu8/libu8io.h>
#include <libu8/xfiles.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8streamio.h>

#include <ctype.h>

static fdtype accept_language, accept_type, accept_charset, accept_encoding;
static fdtype server_port, remote_port, request_method, status_field;
static fdtype get_method, post_method, browseinfo_symbol;
static fdtype redirect_field, sendfile_field;
static fdtype query_string, query_elts, query, http_cookie, http_referrer;
static fdtype http_headers, html_headers, cookiedata_symbol;
static fdtype outcookies_symbol, incookies_symbol, bad_cookie, text_symbol;
static fdtype doctype_slotid, xmlpi_slotid, html_attribs_slotid;
static fdtype body_attribs_slotid, body_classes_slotid, class_symbol;
static fdtype content_slotid, content_type, cgi_content_type;
static fdtype remote_user_symbol, remote_host_symbol, remote_addr_symbol;
static fdtype remote_info_symbol, remote_agent_symbol, remote_ident_symbol;
static fdtype parts_slotid, name_slotid, filename_slotid, mapurlfn_symbol;
static fdtype ipeval_symbol;

char *fd_sendfile_header=NULL; /* X-Sendfile */
static int log_cgidata=0;

static u8_condition CGIDataInconsistency="Inconsistent CGI data";

#define DEFAULT_CONTENT_TYPE \
  "Content-type: text/html; charset=utf-8;"
#define DEFAULT_DOCTYPE \
  "<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.1//EN\"\
               \"http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd\">"
#define DEFAULT_XMLPI \
  "<?xml version='1.0' charset='utf-8' ?>"

/* Utility functions */

static fdtype try_parse(u8_string buf)
{
  fdtype val=fd_parse((buf[0]==':')?(buf+1):(buf));
  if (FD_ABORTP(val)) {
    fd_decref(val); return fdtype_string(buf);}
  else return val;
}

static fdtype buf2lisp(char *buf,int isascii)
{
  if (buf[0]!=':')
    if (isascii) return fdtype_string(buf);
    else return fd_lispstring(u8_valid_copy(buf));
  else if (isascii) return try_parse(buf);
  else {
    u8_string s=u8_valid_copy(buf);
    fdtype value=try_parse(s);
    u8_free(s);
    return value;}
}

static fdtype buf2slotid(char *buf,int isascii)
{
  if (isascii) return fd_parse(buf);
  else {
    u8_string s=u8_valid_copy(buf);
    fdtype value=fd_parse(s);
    u8_free(s);
    return value;}
}

static fdtype buf2string(char *buf,int isascii)
{
  if (isascii) return fdtype_string(buf);
  else return fd_lispstring(u8_valid_copy(buf));
}

static void emit_uri_value(u8_output out,fdtype val)
{
  if (FD_STRINGP(val))
    fd_uri_output(out,FD_STRDATA(val),FD_STRLEN(val),0,NULL);
  else {
    struct U8_OUTPUT xout; u8_byte buf[256];
    U8_INIT_OUTPUT_X(&xout,256,buf,0);
    fd_unparse(&xout,val);
    u8_putn(out,"%3a",3);
    fd_uri_output(out,xout.u8_outbuf,xout.u8_outptr-xout.u8_outbuf,0,NULL);
    if (xout.u8_outbuf!=buf) u8_free(xout.u8_outbuf);}
}

FD_EXPORT
void fd_urify(u8_output out,fdtype val)
{
  emit_uri_value(out,val);
}

/* Converting cgidata */

static struct FD_PROTECTED_CGI {
  fdtype field;
  struct FD_PROTECTED_CGI *next;} *protected_cgi;
#if FD_THREADS_ENABLED
static u8_mutex protected_cgi_lock;
#endif

static int isprotected(fdtype field)
{
  struct FD_PROTECTED_CGI *scan=protected_cgi;
  while (scan) {
    if (FDTYPE_EQUAL(scan->field,field)) return 1;
    else scan=scan->next;}
  return 0;
}
static fdtype protected_cgi_get(fdtype var,void *ptr)
{
  fdtype result=FD_EMPTY_CHOICE;
  struct FD_PROTECTED_CGI *scan=protected_cgi;
  while (scan) {
    fdtype field=scan->field; fd_incref(field);
    FD_ADD_TO_CHOICE(result,field); 
    scan=scan->next;}
  return result;
}
static int protected_cgi_set(fdtype var,fdtype field,void *ptr)
{
  struct FD_PROTECTED_CGI *scan=protected_cgi, *fresh=NULL;
  u8_lock_mutex(&protected_cgi_lock);
  while (scan) {
    if (FDTYPE_EQUAL(scan->field,field)) {
      u8_unlock_mutex(&protected_cgi_lock);
      return 0;}
    else scan=scan->next;}
  fresh=u8_alloc(struct FD_PROTECTED_CGI);
  fresh->field=field; fd_incref(field);
  fresh->next=protected_cgi;
  protected_cgi=fresh;
  u8_unlock_mutex(&protected_cgi_lock);
  return 1;
}

static void convert_parse(fd_slotmap c,fdtype slotid)
{
  fdtype value=fd_slotmap_get(c,slotid,FD_VOID);
  if (!(FD_STRINGP(value))) {fd_decref(value);}
  else {
    fd_slotmap_store(c,slotid,try_parse(FD_STRDATA(value)));
    fd_decref(value);}
}

static void convert_accept(fd_slotmap c,fdtype slotid)
{
  fdtype value=fd_slotmap_get(c,slotid,FD_VOID);
  if (!(FD_STRINGP(value))) {fd_decref(value);}
  else {
    fdtype newvalue=FD_EMPTY_CHOICE, entry;
    u8_byte *data=FD_STRDATA(value), *scan=data;
    u8_byte *comma=strchr(scan,','), *semi=strstr(scan,";q=");
    while (comma) {
      if ((semi) && (semi<comma))
        entry=fd_init_pair(NULL,fd_extract_string(NULL,scan,semi),
                           fd_extract_string(NULL,semi+3,comma));
      else entry=fd_extract_string(NULL,scan,comma);
      FD_ADD_TO_CHOICE(newvalue,entry);
      scan=comma+1; comma=strchr(scan,','); semi=strstr(scan,";q=");}
    if (semi)
      entry=fd_init_pair(NULL,fd_extract_string(NULL,scan,semi),
                         fd_extract_string(NULL,semi+3,NULL));
    else entry=fd_extract_string(NULL,scan,NULL);
    FD_ADD_TO_CHOICE(newvalue,entry);
    fd_slotmap_store(c,slotid,newvalue);
    fd_decref(value); fd_decref(newvalue);}
}

/* Converting query arguments */

static fdtype post_data_slotid, form_data_string;

/* Setting this to zero might be slightly more secure, and it's the
   CONFIG option QUERYWITHPOST (boolean) */
static int parse_query_on_post=1;

static void parse_query_string(fd_slotmap c,char *data,int len);

static fdtype intern_compound(u8_string s1,u8_string s2)
{
  fdtype result=FD_VOID;
  struct U8_OUTPUT tmpout; u8_byte tmpbuf[128];
  U8_INIT_OUTPUT_X(&tmpout,128,tmpbuf,U8_STREAM_GROWS);
  u8_puts(&tmpout,s1); u8_puts(&tmpout,s2);
  result=fd_parse(tmpout.u8_outbuf);
  u8_close_output(&tmpout);
  return result;
}

static void get_form_args(fd_slotmap c)
{
  if (fd_test((fdtype)c,request_method,get_method)) {
    fdtype qval=fd_slotmap_get(c,query_string,FD_VOID);
    if (FD_STRINGP(qval))
      parse_query_string(c,FD_STRING_DATA(qval),FD_STRING_LENGTH(qval));
    fd_decref(qval);
    return;}
  else if (fd_test((fdtype)c,request_method,post_method)) {
    fd_handle_compound_mime_field((fdtype)c,cgi_content_type,FD_VOID);
    if (parse_query_on_post) {
      /* We do this first, so it won't override any post data
         which is supposedly more legitimate. */
      fdtype qval=fd_slotmap_get(c,query_string,FD_VOID);
      if (FD_STRINGP(qval))
        parse_query_string(c,FD_STRING_DATA(qval),FD_STRING_LENGTH(qval));
      fd_decref(qval);}
    if (fd_test((fdtype)c,cgi_content_type,form_data_string)) {
      fdtype postdata=fd_slotmap_get(c,post_data_slotid,FD_VOID);
      fdtype parts=FD_EMPTY_LIST;
      /* Parse the MIME data into parts */
      if (FD_STRINGP(postdata))
        parts=fd_parse_multipart_mime
          ((fdtype)c,FD_STRING_DATA(postdata),
           FD_STRING_DATA(postdata)+FD_STRING_LENGTH(postdata));
      else if (FD_PACKETP(postdata))
        parts=fd_parse_multipart_mime
          ((fdtype)c,FD_PACKET_DATA(postdata),
           FD_PACKET_DATA(postdata)+FD_PACKET_LENGTH(postdata));
      fd_add((fdtype)c,parts_slotid,parts);
      fd_decref(postdata);
      /* Convert the parts */
      {FD_DOLIST(elt,parts) {
        fdtype namestring=fd_get(elt,name_slotid,FD_VOID);
        if (FD_STRINGP(namestring)) {
          u8_string nstring=FD_STRING_DATA(namestring);
          fdtype namesym=fd_parse(nstring);
          fdtype partsym=intern_compound("__",nstring);
          fdtype ctype=fd_get(elt,content_type,FD_VOID);
          fdtype content=fd_get((fdtype)elt,content_slotid,FD_EMPTY_CHOICE);
          /* Add the part itself in the _name slotid */
          fd_add((fdtype)c,partsym,elt);
          /* Add the filename slot if it's an upload */
          if (fd_test(elt,filename_slotid,FD_VOID)) {
            fdtype filename=fd_get(elt,filename_slotid,FD_EMPTY_CHOICE);
            fd_add((fdtype)c,intern_compound(nstring,"_FILENAME"),
                   filename);
            fd_decref(filename);}
          if (fd_test(elt,content_type,FD_VOID)) {
            fdtype ctype=fd_get(elt,content_type,FD_EMPTY_CHOICE);
            fd_add((fdtype)c,intern_compound(nstring,"_TYPE"),ctype);
            fd_decref(ctype);}
          if ((FD_VOIDP(ctype)) || (fd_overlapp(ctype,text_symbol))) {
            if (FD_STRINGP(content)) {
              u8_string chars=FD_STRDATA(content); int len=FD_STRLEN(content);
              /* Remove trailing \r\n from the MIME field */
              if ((len>1) && (chars[len-1]=='\n')) {
                fdtype new_content;
                if (chars[len-2]=='\r')
                  new_content=fd_extract_string(NULL,chars,chars+len-2);
                else new_content=fd_extract_string(NULL,chars,chars+len-1);
                fd_add((fdtype)c,namesym,new_content);
                fd_decref(new_content);}
              else fd_add((fdtype)c,namesym,content);}
            else fd_add((fdtype)c,namesym,content);}
          else fd_add((fdtype)c,namesym,content);
          fd_decref(content); fd_decref(ctype);}
        fd_decref(namestring);}}
      fd_decref(parts);}
    else {
      fdtype qval=fd_slotmap_get(c,post_data_slotid,FD_VOID);
      u8_string data; unsigned int len;
      if (FD_STRINGP(qval)) {
        data=FD_STRDATA(qval); len=FD_STRING_LENGTH(qval);}
      else if (FD_PACKETP(qval)) {
        data=FD_PACKET_DATA(qval); len=FD_PACKET_LENGTH(qval);}
      else {
        fd_decref(qval); data=NULL;}
      if ((data) && (strchr(data,'=')))
        parse_query_string(c,data,len);
      fd_decref(qval);}}
}

static void parse_query_string(fd_slotmap c,char *data,int len)
{
  fdtype slotid=FD_VOID, value=FD_VOID;
  int isascii=1;
  u8_byte *scan=data, *end=scan+len;
  char *buf=u8_malloc(len+1), *write=buf;
  while (scan<end)
    if ((FD_VOIDP(slotid)) && (*scan=='=')) {
      *write++='\0';
      /* Don't store vars beginning with _ or HTTP, to avoid spoofing
         of real HTTP variables or other variables that might be used
         internally. */
      if (buf[0]=='_') slotid=FD_VOID;
      if (strncmp(buf,"HTTP",4)==0) slotid=FD_VOID;
      else slotid=buf2slotid(buf,isascii);
      if (isprotected(slotid)) slotid=FD_VOID;
      write=buf; isascii=1; scan++;}
    else if (*scan=='&') {
      *write++='\0';
      if (FD_VOIDP(slotid))
        value=buf2string(buf,isascii);
      else value=buf2lisp(buf,isascii);
      if (FD_VOIDP(slotid))
        fd_slotmap_add(c,query,value);
      else fd_slotmap_add(c,slotid,value);
      fd_decref(value); value=FD_VOID; slotid=FD_VOID;
      write=buf; isascii=1; scan++;}
    else if (*scan == '%')
      if (scan+3>end) end=scan;
      else {
        char buf[4]; int c; scan++;
        buf[0]=*scan++; buf[1]=*scan++; buf[2]='\0';
        c=strtol(buf,NULL,16);
        if (c>=0x80) isascii=0;
        *write++=c;}
    else if (*scan == '+') {*write++=' '; scan++;}
    else if (*scan<0x80) *write++=*scan++;
    else {*write++=*scan++; isascii=0;}
  if (write>buf) {
    *write++='\0';
    value=buf2lisp(buf,isascii);
    if (FD_VOIDP(slotid))
      fd_slotmap_add(c,query,value);
    else fd_slotmap_add(c,slotid,value);
    fd_decref(value); value=FD_VOID; slotid=FD_VOID;
    write=buf; isascii=1; scan++;}
  else if (!(FD_VOIDP(slotid))) {
    fdtype str=fdtype_string("");
    fd_slotmap_add(c,slotid,str);
    fd_decref(str);}
  u8_free(buf);
}

/* Converting cookie args */

static void setcookiedata(fdtype cookiedata,fdtype cgidata)
{
  /* This replaces any existing cookie data which matches cookiedata */
  fdtype cookies=
    ((FD_TABLEP(cgidata))?
     (fd_get(cgidata,outcookies_symbol,FD_EMPTY_CHOICE)):
     (fd_req_get(outcookies_symbol,FD_EMPTY_CHOICE)));
  fdtype cookievar=FD_VECTOR_REF(cookiedata,0);
  FD_DO_CHOICES(cookie,cookies) {
    if (FD_EQ(FD_VECTOR_REF(cookie,0),cookievar)) {
      if (FD_TABLEP(cgidata))
        fd_drop(cgidata,outcookies_symbol,cookie);
      else fd_req_drop(outcookies_symbol,cookie);}}
  fd_decref(cookies);
  if ((cgidata)&&(FD_TABLEP(cgidata)))
    fd_add(cgidata,outcookies_symbol,cookiedata);
  else fd_req_add(outcookies_symbol,cookiedata);
}

static void convert_cookie_arg(fd_slotmap c)
{
  fdtype qval=fd_slotmap_get(c,http_cookie,FD_VOID);
  if (!(FD_STRINGP(qval))) {fd_decref(qval); return;}
  else {
    fdtype slotid=FD_VOID, name=FD_VOID, value=FD_VOID;
    int len=FD_STRLEN(qval); int isascii=1, badcookie=0;
    u8_byte *scan=FD_STRDATA(qval), *end=scan+len;
    char *buf=u8_malloc(len), *write=buf;
    while (scan<end)
      if ((FD_VOIDP(slotid)) && (*scan=='=')) {
        /* There's an assignment going on. */
        *write++='\0';
        name=fdtype_string(buf);
        if ((buf[0]=='_')||(strncmp(buf,"HTTP",4)==0)) badcookie=1;
        else badcookie=0;
        if (isascii) slotid=fd_parse(buf);
        else {
          u8_string s=u8_valid_copy(buf);
          slotid=fd_parse(s);
          u8_free(s);}
        write=buf; isascii=1;
        scan++;}
      else if (*scan==';') {
        *write++='\0';
        if (FD_VOIDP(slotid))
          /* If there isn't a slot/symbol/etc, just store a string */
          value=buf2string(buf,isascii);
        else /* Otherwise, parse to LISP */
          value=buf2lisp(buf,isascii);
        if (FD_VOIDP(slotid))
          u8_log(LOG_WARN,_("malformed cookie"),"strange cookie syntax: \"%s\"",
                 FD_STRDATA(qval));
        else {
          fdtype cookiedata=fd_make_nvector(2,name,fd_incref(value));
          if (badcookie)
            fd_slotmap_add(c,bad_cookie,cookiedata);
          else fd_slotmap_add(c,slotid,value);
          fd_slotmap_add(c,cookiedata_symbol,cookiedata);
          setcookiedata(cookiedata,(fdtype)c);
          name=FD_VOID;
          fd_decref(cookiedata);}
        fd_decref(value); value=FD_VOID; slotid=FD_VOID;
        write=buf; isascii=1; scan++;}
      else if (*scan == '%')
        if (scan+3>end) {
          u8_log(LOG_WARN,_("malformed cookie"),"cookie ends early: \"%s\"",
                 FD_STRDATA(qval));
          end=scan;}
        else {
          char buf[4]; int c; scan++;
          buf[0]=*scan++; buf[1]=*scan++; buf[2]='\0';
          c=strtol(buf,NULL,16);
          if (c>=0x80) isascii=0;
          *write++=c;}
      else if (*scan == '+') {*write++=' '; scan++;}
      else if (*scan == ' ') scan++;
      else {
        if (*scan<0x80) isascii=0;
        *write++=*scan++; }
    if (write>buf) {
      *write++='\0';
      if (FD_VOIDP(slotid)) value=buf2string(buf,isascii);
      else value=buf2lisp(buf,isascii);
      if (FD_VOIDP(slotid))
        u8_log(LOG_WARN,_("malformed cookie"),"strange cookie syntax: \"%s\"",
                FD_STRDATA(qval));
      else {
        fdtype cookiedata=fd_make_nvector(2,name,fd_incref(value));
        if (badcookie)
          fd_slotmap_add(c,bad_cookie,cookiedata);
        else fd_slotmap_add(c,slotid,value);
        fd_slotmap_add(c,cookiedata_symbol,cookiedata);
        setcookiedata(cookiedata,(fdtype)c);
        fd_decref(cookiedata);}
      fd_decref(value); value=FD_VOID; slotid=FD_VOID;
      write=buf; isascii=1; scan++;}
    fd_slotmap_add(c,incookies_symbol,qval);
    fd_slotmap_store(c,outcookies_symbol,FD_EMPTY_CHOICE);
    fd_decref(qval);
    u8_free(buf);}
}

/* Parsing CGI data */

static fdtype cgi_prepfns=FD_EMPTY_CHOICE;
static void add_remote_info(fdtype cgidata);

FD_EXPORT int fd_parse_cgidata(fdtype data)
{
  struct FD_SLOTMAP *cgidata=FD_XSLOTMAP(data);
  convert_accept(cgidata,accept_language);
  convert_accept(cgidata,accept_type);
  convert_accept(cgidata,accept_charset);
  convert_accept(cgidata,accept_encoding);
  convert_parse(cgidata,server_port);
  convert_parse(cgidata,remote_port);
  convert_parse(cgidata,request_method);
  convert_cookie_arg(cgidata);
  get_form_args(cgidata);
  add_remote_info(data);
  {FD_DO_CHOICES(handler,cgi_prepfns) {
    if (FD_APPLICABLEP(handler)) {
      fdtype value=fd_apply(handler,1,&data);
      fd_decref(value);}
    else u8_log(LOG_WARN,"Not Applicable","Invalid CGI prep handler %q",handler);}}
  return 1;
}

static void add_remote_info(fdtype cgidata)
{
  /* This combines a bunch of different request properties into a single string */
  fdtype remote_user=fd_get(cgidata,remote_user_symbol,FD_VOID);
  fdtype remote_ident=fd_get(cgidata,remote_ident_symbol,FD_VOID);
  fdtype remote_host=fd_get(cgidata,remote_host_symbol,FD_VOID);
  fdtype remote_addr=fd_get(cgidata,remote_addr_symbol,FD_VOID);
  fdtype remote_agent=fd_get(cgidata,remote_agent_symbol,FD_VOID);
  fdtype remote_string=FD_VOID;
  struct U8_OUTPUT remote;
  U8_INIT_OUTPUT(&remote,128);
  u8_printf(&remote,"%s%s%s@%s%s%s<%s",
            ((FD_STRINGP(remote_ident)) ? (FD_STRDATA(remote_ident)) : ((u8_string)"")),
            ((FD_STRINGP(remote_ident)) ? ((u8_string)"|") : ((u8_string)"")),
            ((FD_STRINGP(remote_user)) ? (FD_STRDATA(remote_user)) : ((u8_string)"nobody")),
            ((FD_STRINGP(remote_host)) ? (FD_STRDATA(remote_host)) : ((u8_string)"")),
            ((FD_STRINGP(remote_host)) ? ((u8_string)"/") : ((u8_string)"")),
            ((FD_STRINGP(remote_addr)) ? (FD_STRDATA(remote_addr)) : ((u8_string)"noaddr")),
            ((FD_STRINGP(remote_agent)) ? (FD_STRDATA(remote_agent)) : ((u8_string)"noagent")));
  remote_string=
    fd_init_string(NULL,remote.u8_outptr-remote.u8_outbuf,
                   remote.u8_outbuf);
  fd_store(cgidata,remote_info_symbol,remote_string);
  fd_decref(remote_user); fd_decref(remote_ident); fd_decref(remote_host);
  fd_decref(remote_addr); fd_decref(remote_agent);
  fd_decref(remote_string);
}

/* Generating headers */

static fdtype do_xmlout(U8_OUTPUT *out,fdtype body,fd_lispenv env)
{
  U8_OUTPUT *prev=u8_current_output;
  u8_set_default_output(out);
  while (FD_PAIRP(body)) {
    fdtype value=fasteval(FD_CAR(body),env);
    body=FD_CDR(body);
    if (FD_ABORTP(value)) {
      u8_set_default_output(prev);
      return value;}
    else if (FD_VOIDP(value)) continue;
    else if (FD_STRINGP(value))
      u8_printf(out,"%s",FD_STRDATA(value));
    else u8_printf(out,"%q",value);
    fd_decref(value);}
  u8_set_default_output(prev);
  return FD_VOID;
}

static fdtype httpheader(fdtype expr,fd_lispenv env)
{
  U8_OUTPUT out; fdtype result;
  U8_INIT_OUTPUT(&out,64);
  result=do_xmlout(&out,fd_get_body(expr,1),env);
  if (FD_ABORTP(result)) {
    u8_free(out.u8_outbuf);
    return result;}
  else {
    fdtype header=
      fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
    fd_req_add(http_headers,header);
    fd_decref(header);
    return FD_VOID;}
}

static fdtype addhttpheader(fdtype header)
{
  fd_req_add(http_headers,header);
  return FD_VOID;
}

static fdtype htmlheader(fdtype expr,fd_lispenv env)
{
  U8_OUTPUT out; fdtype result;
  U8_INIT_OUTPUT(&out,64);
  result=do_xmlout(&out,fd_get_body(expr,1),env);
  if (FD_ABORTP(result)) {
    u8_free(out.u8_outbuf);
    return result;}
  else fd_req_push
         (html_headers,
          fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf));
  return FD_VOID;
}

/* Handling cookies */

static int handle_cookie(U8_OUTPUT *out,fdtype cgidata,fdtype cookie)
{
  int len; fdtype real_val, name;
  if (!(FD_VECTORP(cookie))) return -1;
  else len=FD_VECTOR_LENGTH(cookie);
  if (len<2) return -1;
  else name=FD_VECTOR_REF(cookie,0);
  if (!((FD_SYMBOLP(name))||(FD_STRINGP(name))||(FD_OIDP(name))))
    return -1;
  if (FD_TABLEP(cgidata)) {
    real_val=fd_get(cgidata,FD_VECTOR_REF(cookie,0),FD_VOID);
    if (FD_VOIDP(real_val)) real_val=fd_incref(FD_VECTOR_REF(cookie,1));}
  else real_val=fd_incref(FD_VECTOR_REF(cookie,1));
  if ((len>2) || (!(FD_EQ(real_val,FD_VECTOR_REF(cookie,1))))) {
    fdtype var=FD_VECTOR_REF(cookie,0);
    u8_string namestring; u8_byte *scan;
    int free_namestring=0, c;
    u8_printf(out,"Set-Cookie: ");
    if (FD_SYMBOLP(var)) namestring=FD_SYMBOL_NAME(var);
    else if (FD_STRINGP(var)) namestring=FD_STRDATA(var);
    else {
      namestring=fd_dtype2string(var);
      free_namestring=1;}
    scan=namestring; c=u8_sgetc(&scan);
    if ((c=='$')|| (c>=0x80) || (u8_isspace(c)) || (c==';') || (c==','))
      u8_printf(out,"\\u%04x",c);
    else u8_putc(out,c);
    c=u8_sgetc(&scan);
    while (c>0) {
      if ((c>=0x80) || (u8_isspace(c)) || (c==';') || (c==','))
        u8_printf(out,"\\u%04x",c);
      else u8_putc(out,c);
      c=u8_sgetc(&scan);}
    u8_puts(out,"=");
    if (free_namestring) u8_free(namestring);
    emit_uri_value(out,FD_VECTOR_REF(cookie,1));
    if (len>2) {
      fdtype domain=FD_VECTOR_REF(cookie,2);
      fdtype path=((len>3) ? (FD_VECTOR_REF(cookie,3)) : (FD_VOID));
      fdtype expires=((len>4) ? (FD_VECTOR_REF(cookie,4)) : (FD_VOID));
      fdtype secure=((len>5) ? (FD_VECTOR_REF(cookie,5)) : (FD_VOID));
      if (FD_STRINGP(domain))
        u8_printf(out,"; Domain=.%s",FD_STRDATA(domain));
      if (FD_STRINGP(path))
        u8_printf(out,"; Path=%s",FD_STRDATA(path));
      if (FD_STRINGP(expires))
        u8_printf(out,"; Expires=%s",FD_STRDATA(expires));
      else if (FD_PTR_TYPEP(expires,fd_timestamp_type)) {
        struct FD_TIMESTAMP *tstamp=(fd_timestamp)expires;
        char buf[512]; struct tm tptr;
        u8_xtime_to_tptr(&(tstamp->xtime),&tptr);
        strftime(buf,512,"%A, %d-%b-%Y %T GMT",&tptr);
        u8_printf(out,"; Expires=%s",buf);}
      if (!((FD_VOIDP(secure))||(FD_FALSEP(secure))))
        u8_printf(out,"; Secure");}
    u8_printf(out,"\r\n");}
  fd_decref(real_val);
  return 1;
}

static fdtype setcookie
  (fdtype var,fdtype val,
   fdtype domain,fdtype path,
   fdtype expires,fdtype secure)
{
  if (!((FD_SYMBOLP(var)) || (FD_STRINGP(var)) || (FD_OIDP(var))))
    return fd_type_error("symbol","setcookie",var);
  else {
    fdtype cookiedata;
    if (FD_VOIDP(domain)) domain=FD_FALSE;
    if (FD_VOIDP(path)) path=FD_FALSE;
    if (FD_VOIDP(expires)) expires=FD_FALSE;
    if ((FD_FALSEP(expires))||
        (FD_STRINGP(expires))||
        (FD_PRIM_TYPEP(expires,fd_timestamp_type)))
      fd_incref(expires);
    else if ((FD_FIXNUMP(expires))||(FD_BIGINTP(expires))) {
      long long ival=((FD_FIXNUMP(expires))?(FD_FIX2INT(expires)):
                      (fd_bigint_to_long_long((fd_bigint)expires)));
      time_t expval;
      if ((ival<0)||(ival<(24*3600*365*20))) expval=(time(NULL))+ival;
      else expval=(time_t)ival;
      expires=fd_time2timestamp(expval);}
    else return fd_type_error("timestamp","setcookie",expires);
    cookiedata=
      fd_make_nvector(6,fd_incref(var),fd_incref(val),
                      fd_incref(domain),fd_incref(path),
                      fd_incref(expires),fd_incref(secure));
    setcookiedata(cookiedata,FD_VOID);
    fd_decref(cookiedata);
    return FD_VOID;}
}

static fdtype clearcookie
  (fdtype var,fdtype domain,fdtype path,fdtype secure)
{
  if (!((FD_SYMBOLP(var)) || (FD_STRINGP(var)) || (FD_OIDP(var))))
    return fd_type_error("symbol","setcookie",var);
  else {
    fdtype cookiedata;
    fdtype val=fdtype_string("");
    time_t past=time(NULL)-(3600*24*7*50);
    fdtype expires=fd_time2timestamp(past);
    if (FD_VOIDP(domain)) domain=FD_FALSE;
    if (FD_VOIDP(path)) path=FD_FALSE;
    cookiedata=
      fd_make_nvector(6,fd_incref(var),val,
                      fd_incref(domain),fd_incref(path),
                      expires,fd_incref(secure));
    setcookiedata(cookiedata,FD_VOID);
    fd_decref(cookiedata);
    return FD_VOID;}
}

/* HTML Header functions */

static fdtype add_stylesheet(fdtype stylesheet,fdtype type)
{
  U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
  if (FD_VOIDP(type))
    u8_printf(&out,"<link rel='stylesheet' type='text/css' href='%s'/>\n",
              FD_STRDATA(stylesheet));
  else u8_printf(&out,"<link rel='stylesheet' type='%s' href='%s'/>\n",
                 FD_STRDATA(type),FD_STRDATA(stylesheet));
  fd_req_push(html_headers,
              fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf));
  return FD_VOID;
}

static fdtype add_javascript(fdtype url)
{
  U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
  u8_printf(&out,"<script language='javascript' src='%s'>\n",
            FD_STRDATA(url));
  u8_printf(&out,"  <!-- empty content for some browsers -->\n");
  u8_printf(&out,"</script>\n");
  fd_req_push(html_headers,
              fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf));
  return FD_VOID;
}

static fdtype title_handler(fdtype expr,fd_lispenv env)
{
  U8_OUTPUT out; fdtype result;
  U8_INIT_OUTPUT(&out,64);
  u8_puts(&out,"<title>");
  result=do_xmlout(&out,fd_get_body(expr,1),env);
  u8_puts(&out,"</title>\n");
  if (FD_ABORTP(result)) {
    u8_free(out.u8_outbuf);
    return result;}
  else {
    fd_req_push(html_headers,
                fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf));
    return FD_VOID;}
}

/* Generating the XML */
void fd_output_http_headers(U8_OUTPUT *out,fdtype cgidata)
{
  fdtype ctype=fd_get(cgidata,content_type,FD_VOID);
  fdtype status=fd_get(cgidata,status_field,FD_VOID);
  fdtype headers=fd_get(cgidata,http_headers,FD_EMPTY_CHOICE);
  fdtype redirect=fd_get(cgidata,redirect_field,FD_VOID);
  fdtype sendfile=fd_get(cgidata,sendfile_field,FD_VOID);
  fdtype cookies=fd_get(cgidata,outcookies_symbol,FD_EMPTY_CHOICE);
  if ((FD_STRINGP(redirect))&&(FD_VOIDP(status))) {
    status=FD_INT2DTYPE(303);}
  if (FD_FIXNUMP(status))
    u8_printf(out,"Status: %d\r\n",FD_FIX2INT(status));
  else if (FD_STRINGP(status))
    u8_printf(out,"Status: %s\r\n",FD_STRDATA(status));
  if (FD_STRINGP(ctype))
    u8_printf(out,"Content-type: %s\r\n",FD_STRDATA(ctype));
  else u8_printf(out,"%s\r\n",DEFAULT_CONTENT_TYPE);
  {FD_DO_CHOICES(header,headers)
      if (FD_STRINGP(header)) {
        u8_putn(out,FD_STRDATA(header),FD_STRLEN(header));
        u8_putn(out,"\r\n",2);}
      else if (FD_PACKETP(header)) {
        /* This handles both packets and secrets */
        u8_putn(out,FD_PACKET_DATA(header),FD_PACKET_LENGTH(header));
        u8_putn(out,"\r\n",2);}}
  {FD_DO_CHOICES(cookie,cookies)
     if (handle_cookie(out,cgidata,cookie)<0)
       u8_log(LOG_WARN,CGIDataInconsistency,"Bad cookie data: %q",cookie);}
  if (FD_STRINGP(redirect))
    u8_printf(out,"Location: %s\r\n",FD_STRDATA(redirect));
  else if ((FD_STRINGP(sendfile))&&(fd_sendfile_header)) {
    u8_log(LOG_DEBUG,"Sendfile","Using %s to pass %s",
           fd_sendfile_header,FD_STRDATA(sendfile));
    u8_printf(out,"%s: %s\r\n",fd_sendfile_header,FD_STRDATA(sendfile));}
  else {}
  fd_decref(ctype); fd_decref(status); fd_decref(headers);
  fd_decref(redirect); fd_decref(sendfile); fd_decref(cookies);
}

static void output_headers(U8_OUTPUT *out,fdtype headers)
{
  if (FD_PAIRP(headers)) {
    fdtype header=FD_CAR(headers);
    output_headers(out,FD_CDR(headers));
    u8_putn(out,FD_STRDATA(header),FD_STRLEN(header));
    u8_putn(out,"\n",1);}
}

static fdtype update_body_attribs(fdtype body_attribs,fdtype body_classes)
{
  if (!(FD_PAIRP(body_classes))) return body_attribs;
  else if (FD_PAIRP(body_attribs)) {
    fdtype scan=body_attribs, scan_classes=body_classes;
    struct U8_OUTPUT classout; fdtype class_cons=FD_VOID;
    u8_byte _classbuf[256]; int i=0;
    while (FD_PAIRP(scan)) {
      fdtype car=FD_CAR(scan);
      if ((car==class_symbol)||
          ((FD_STRINGP(car))&&(strcasecmp(FD_STRDATA(car),"class")==0))) {
        class_cons=FD_CDR(scan); break;}
      else {
        scan=FD_CDR(scan); 
        if (FD_PAIRP(scan)) scan=FD_CDR(scan);}}
    if (FD_VOIDP(class_cons)) {
      class_cons=fd_init_pair(NULL,FD_FALSE,body_attribs);
      body_attribs=fd_init_pair(NULL,class_symbol,class_cons);}
    U8_INIT_STATIC_OUTPUT_BUF(classout,sizeof(_classbuf),_classbuf);
    if (FD_STRINGP(FD_CAR(class_cons))) {
      u8_puts(&classout,FD_STRDATA(FD_CAR(class_cons)));
      u8_puts(&classout," ");}
    while (FD_PAIRP(body_classes)) {
      fdtype car=FD_CAR(body_classes);
      if (!(FD_STRINGP(car))) {}
      else {
        if (i) u8_puts(&classout," "); i++;
        u8_puts(&classout,FD_STRDATA(car));}
      body_classes=FD_CDR(body_classes);}
    {fdtype old=FD_CAR(class_cons);
      FD_RPLACA(class_cons,fd_make_string
                (NULL,classout.u8_outptr-classout.u8_outbuf,
                 classout.u8_outbuf));
      fd_decref(old);
      u8_close((u8_stream)&classout);}
    return body_attribs;}
  else return body_attribs;
}

FD_EXPORT
int fd_output_xhtml_preface(U8_OUTPUT *out,fdtype cgidata)
{
  fdtype doctype=fd_get(cgidata,doctype_slotid,FD_VOID);
  fdtype xmlpi=fd_get(cgidata,xmlpi_slotid,FD_VOID);
  fdtype html_attribs=fd_get(cgidata,html_attribs_slotid,FD_VOID);
  fdtype body_attribs=fd_get(cgidata,body_attribs_slotid,FD_VOID);
  fdtype body_classes=fd_get(cgidata,body_classes_slotid,FD_VOID);
  fdtype headers=fd_get(cgidata,html_headers,FD_VOID);
  if (FD_VOIDP(doctype)) u8_puts(out,DEFAULT_DOCTYPE);
  else if (FD_STRINGP(doctype))
    u8_putn(out,FD_STRDATA(doctype),FD_STRLEN(doctype));
  else return 0;
  u8_putc(out,'\n');
  if (FD_STRINGP(xmlpi))
    u8_putn(out,FD_STRDATA(xmlpi),FD_STRLEN(xmlpi));
  else u8_puts(out,DEFAULT_XMLPI);
  if (FD_VOIDP(html_attribs))
    u8_puts(out,"\n<html>\n<head>\n");
  else {
    fd_open_markup(out,"html",html_attribs,0);
    u8_puts(out,"\n<head>\n");}
  if (!(FD_VOIDP(headers))) {
    output_headers(out,headers);
    fd_decref(headers);}
  u8_puts(out,"</head>\n");
  if (FD_PAIRP(body_classes))
    body_attribs=update_body_attribs(body_attribs,body_classes);
  if (FD_FALSEP(body_attribs)) {}
  else if (FD_VOIDP(body_attribs)) u8_puts(out,"<body>");
  else {
    fd_open_markup(out,"body",body_attribs,0);}
  fd_decref(doctype); fd_decref(xmlpi);
  fd_decref(html_attribs);
  fd_decref(body_attribs);
  fd_decref(body_classes);
  return 1;
}

FD_EXPORT
int fd_output_xml_preface(U8_OUTPUT *out,fdtype cgidata)
{
  fdtype doctype=fd_get(cgidata,doctype_slotid,FD_VOID);
  fdtype xmlpi=fd_get(cgidata,xmlpi_slotid,FD_VOID);
  if (FD_STRINGP(xmlpi))
    u8_putn(out,FD_STRDATA(xmlpi),FD_STRLEN(xmlpi));
  else u8_puts(out,DEFAULT_XMLPI);
  u8_putc(out,'\n');
  if (FD_VOIDP(doctype)) u8_puts(out,DEFAULT_DOCTYPE);
  else if (FD_STRINGP(doctype))
    u8_putn(out,FD_STRDATA(doctype),FD_STRLEN(doctype));
  else return 0;
  u8_putc(out,'\n');
  fd_decref(doctype); fd_decref(xmlpi);
  return 1;
}

static fdtype set_body_attribs(int n,fdtype *args)
{
  if ((n==1)&&(args[0]==FD_FALSE)) {
    fd_req_store(body_attribs_slotid,FD_FALSE);
    return FD_VOID;}
  else {
    int i=n-1; while (i>=0) {
      fd_incref(args[i]);
      fd_req_push(body_attribs_slotid,args[i]);
      i--;}
    return FD_VOID;}
}

static fdtype add_body_class(fdtype classname)
{
  fd_req_push(body_classes_slotid,classname);
  return FD_VOID;
}

/* CGI Exec */

static fdtype tail_symbol;

static fdtype cgigetvar(fdtype cgidata,fdtype var)
{
  int noparse=
    ((FD_SYMBOLP(var))&&((FD_SYMBOL_NAME(var))[0]=='%'));
  fdtype name=((noparse)?(fd_intern(FD_SYMBOL_NAME(var)+1)):(var));
  fdtype val=((FD_TABLEP(cgidata))?(fd_get(cgidata,name,FD_VOID)):
              (fd_req_get(name,FD_VOID)));
  if (FD_VOIDP(val)) return val;
  else if ((noparse)&&(FD_STRINGP(val))) return val;
  else if (FD_STRINGP(val)) {
    u8_string data=FD_STRDATA(val);
    if (*data=='\0') return val;
    else if (strchr("@{#(",data[0])) {
      fdtype parsed=fd_parse_arg(data);
      if (FD_ABORTP(parsed)) {
        fd_decref(parsed); fd_clear_errors(0);
        return val;}
      else {
        fd_decref(val); return parsed;}}
    else if (isdigit(data[0])) {
      fdtype parsed=fd_parse_arg(data);
      if (FD_ABORTP(parsed)) {
        fd_decref(parsed); fd_clear_errors(0);
        return val;}
      else if (FD_NUMBERP(parsed)) {
        fd_decref(val); return parsed;}
      else {
        fd_decref(parsed); return val;}}
    else if (*data == ':')
      if (data[1]=='\0')
        return fdtype_string(data);
      else {
        fdtype arg=fd_parse(data+1);
        if (FD_ABORTP(arg)) {
          u8_log(LOG_WARN,fd_ParseArgError,"Bad colon spec arg '%s'",arg);
          fd_clear_errors(1);
          return fdtype_string(data);}
        else return arg;}
    else if (*data == '\\') {
      fdtype shorter=fdtype_string(data+1);
      fd_decref(val);
      return shorter;}
    else return val;}
  else if ((FD_CHOICEP(val))||(FD_ACHOICEP(val))) {
    fdtype result=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(v,val) {
      if (!(FD_STRINGP(v))) {
        fd_incref(v); FD_ADD_TO_CHOICE(result,v);}
      else {
        u8_string data=FD_STRDATA(v); fdtype parsed=v;
        if (*data=='\\') parsed=fdtype_string(data+1);
        else if ((*data==':')&&(data[1]=='\0')) {fd_incref(parsed);}
        else if (*data==':')
          parsed=fd_parse(data+1);
        else if ((isdigit(*data))||(*data=='+')||(*data=='-')||(*data=='.')) {
          parsed=fd_parse_arg(data);
          if (!(FD_NUMBERP(parsed))) {
            fd_decref(parsed); parsed=v; fd_incref(parsed);}}
        else if (strchr("@{#(",data[0]))
          parsed=fd_parse_arg(data);
        else fd_incref(parsed);
        if (FD_ABORTP(parsed)) {
          u8_log(LOG_WARN,fd_ParseArgError,"Bad LISP arg '%s'",data);
          fd_clear_errors(1);
          parsed=v; fd_incref(v);}
        FD_ADD_TO_CHOICE(result,parsed);}}
    fd_decref(val);
    return result;}
  else return val;
}

struct CGICALL {
  fdtype proc, cgidata;
  struct U8_OUTPUT *cgiout; int outlen;
  fdtype result;};

static int cgiexecstep(void *data)
{
  struct CGICALL *call=(struct CGICALL *)data;
  fdtype proc=call->proc, cgidata=call->cgidata;
  fdtype value;
  if (call->outlen<0)
    call->outlen=call->cgiout->u8_outptr-call->cgiout->u8_outbuf;
  else call->cgiout->u8_outptr=call->cgiout->u8_outbuf+call->outlen;
  value=fd_xapply_sproc((fd_sproc)proc,(void *)cgidata,
                        (fdtype (*)(void *,fdtype))cgigetvar);
  value=fd_finish_call(value);
  call->result=value;
  return 1;
}

#if FD_IPEVAL_ENABLED
static int use_ipeval(fdtype proc,fdtype cgidata)
{
  int retval=-1;
  struct FD_SPROC *sp=(struct FD_SPROC *)proc;
  fdtype val=fd_symeval(ipeval_symbol,sp->env);
  if ((FD_VOIDP(val))||(val==FD_UNBOUND)) retval=0;
  else {
    fdtype cgival=fd_get(cgidata,ipeval_symbol,FD_VOID);
    if (FD_VOIDP(cgival)) {
      if (FD_FALSEP(val)) retval=0; else retval=1;}
    else if (FD_FALSEP(cgival)) retval=0;
    else retval=1;
    fd_decref(cgival);}
  fd_decref(val);
  return retval;
}
#endif

FD_EXPORT fdtype fd_cgiexec(fdtype proc,fdtype cgidata)
{
  if (FD_SPROCP(proc)) {
    fdtype value=FD_VOID;
#if FD_IPEVAL_ENABLED
    int ipeval=use_ipeval(proc,cgidata);
#else
    int ipeval=0;
#endif
    if (!(ipeval)) {
      value=fd_xapply_sproc((fd_sproc)proc,(void *)cgidata,
                            (fdtype (*)(void *,fdtype))cgigetvar);
      value=fd_finish_call(value);}
#if FD_IPEVAL_ENABLED
    else {
      struct U8_OUTPUT *out=u8_current_output;
      struct CGICALL call={proc,cgidata,out,-1,FD_VOID};
      fd_ipeval_call(cgiexecstep,(void *)&call);
      value=call.result;}
#endif
    return value;}
  else return fd_apply(proc,0,NULL);
}

/* Parsing query strings */

static fdtype cgiparse(fdtype qstring)
{
  fdtype smap=fd_empty_slotmap();
  parse_query_string((fd_slotmap)smap,FD_STRDATA(qstring),FD_STRLEN(qstring));
  return smap;
}

/* Bind request and output */

static fdtype withreqout_handler(fdtype expr,fd_lispenv env)
{
  U8_OUTPUT *oldout=u8_current_output;
  U8_OUTPUT _out, *out=&_out;
  fdtype body=fd_get_body(expr,1);
  fdtype reqinfo=fd_empty_slotmap();
  fdtype result=FD_VOID;
  fdtype oldinfo=fd_push_reqinfo(reqinfo);
  U8_INIT_OUTPUT(&_out,1024);
  u8_set_default_output(out);
  {FD_DOLIST(ex,body) {
      if (FD_ABORTP(result)) {
        u8_free(_out.u8_outbuf);
        return result;}
      fd_decref(result);
      result=fd_eval(ex,env);}}
  u8_set_default_output(oldout);
  fd_output_xhtml_preface(oldout,reqinfo);
  u8_putn(oldout,_out.u8_outbuf,(_out.u8_outptr-_out.u8_outbuf));
  u8_printf(oldout,"\n</body>\n</html>\n");
  fd_decref(result);
  u8_free(_out.u8_outbuf);
  u8_flush(oldout);
  fd_use_reqinfo(oldinfo);
  return FD_VOID;
}

/* URI mapping */

FD_EXPORT
fdtype fd_mapurl(fdtype uri)
{
  fdtype mapfn=fd_req(mapurlfn_symbol);
  if (FD_APPLICABLEP(mapfn)) {
    fdtype result=fd_apply(mapfn,1,&uri);
    fd_decref(mapfn);
    return result;}
  else {
    fd_decref(mapfn);
    return FD_EMPTY_CHOICE;}
}

static fdtype mapurl(fdtype uri)
{
  fdtype result=fd_mapurl(uri);
  if (FD_ABORTP(result)) return result;
  else if (FD_STRINGP(result)) return result;
  else {
    fd_decref(result); return fd_incref(uri);}
}

/* Initialization */

static int cgiexec_initialized=0;

FD_EXPORT int sendfile_set(fdtype ignored,fdtype v,void *vptr)
{
  u8_string *ptr=vptr;
  if (FD_STRINGP(v)) {
    int bool=fd_boolstring(FD_STRDATA(v),-1);
    if (bool<0) {
      if (*ptr) u8_free(*ptr);
      *ptr=u8_strdup(FD_STRDATA(v));
      return 1;}
    else if (bool==0) {
      if (*ptr) u8_free(*ptr);
      *ptr=NULL;
      return 0;}
    else {
      if (*ptr) u8_free(*ptr);
      *ptr=u8_strdup("X-Sendfile");}
    return 1;}
  else if (FD_TRUEP(v)) {
    if (*ptr) u8_free(*ptr);
    *ptr=u8_strdup("X-Sendfile");
    return 1;}
  else if (FD_FALSEP(v)) {
    if (*ptr) u8_free(*ptr);
    *ptr=NULL;
    return 0;}
  else return fd_reterr(fd_TypeError,"fd_sconfig_set",u8_strdup(_("string")),v);
}

FD_EXPORT void fd_init_cgiexec_c()
{
  fdtype module, xhtmlout_module;
  if (cgiexec_initialized) return;
  cgiexec_initialized=1;
  fd_init_fdscheme();
  module=fd_new_module("FDWEB",(0));
  xhtmlout_module=fd_new_module("XHTML",FD_MODULE_SAFE);

#if FD_THREADS_ENABLED
  fd_init_mutex(&protected_cgi_lock);
#endif

  fd_defspecial(module,"HTTPHEADER",httpheader);
  fd_idefn(module,fd_make_cprim1("HTTPHEADER!",addhttpheader,1));
  fd_idefn(module,fd_make_cprim6("SET-COOKIE!",setcookie,2));
  fd_idefn(module,fd_make_cprim4("CLEAR-COOKIE!",clearcookie,1));
  fd_idefn(module,fd_make_cprimn("BODY!",set_body_attribs,1));
  fd_idefn(module,fd_make_cprim1x("BODYCLASS!",add_body_class,1,
                                  fd_string_type,FD_VOID));

  fd_defspecial(module,"WITH/REQUEST/OUT",withreqout_handler);
  fd_defalias(module,"WITHCGIOUT","WITH/REQUEST/OUT");

  fd_defalias2(module,"WITHCGI",fd_scheme_module,"WITH/REQUEST");
  fd_defalias2(module,"CGICALL",fd_scheme_module,"REQ/CALL");
  fd_defalias2(module,"CGIGET",fd_scheme_module,"REQ/GET");
  fd_defalias2(module,"CGIVAL",fd_scheme_module,"REQ/VAL");
  fd_defalias2(module,"CGITEST",fd_scheme_module,"REQ/TEST");
  fd_defalias2(module,"CGISET!",fd_scheme_module,"REQ/SET!");
  fd_defalias2(module,"CGIADD!",fd_scheme_module,"REQ/ADD!");
  fd_defalias2(module,"CGIADD!",fd_scheme_module,"REQ/DROP!");

  fd_idefn(module,fd_make_cprim1x("MAPURL",mapurl,1,fd_string_type,FD_VOID));

  /* fd_defspecial(module,"CGIVAR",cgivar_handler); */

  fd_idefn(module,fd_make_cprim1x
           ("CGIPARSE",cgiparse,1,fd_string_type,FD_VOID));

  fd_defspecial(xhtmlout_module,"HTMLHEADER",htmlheader);
  fd_defspecial(xhtmlout_module,"TITLE!",title_handler);
  fd_idefn(xhtmlout_module,
           fd_make_cprim2x("STYLESHEET!",add_stylesheet,1,
                           fd_string_type,FD_VOID,
                           fd_string_type,FD_VOID));
  fd_idefn(xhtmlout_module,
           fd_make_cprim1x("JAVASCRIPT!",add_javascript,1,
                           fd_string_type,FD_VOID));

  tail_symbol=fd_intern("%TAIL");
  browseinfo_symbol=fd_intern("BROWSEINFO");
  mapurlfn_symbol=fd_intern("MAPURLFN");

  accept_type=fd_intern("HTTP_ACCEPT");
  accept_language=fd_intern("HTTP_ACCEPT_LANGUAGE");
  accept_encoding=fd_intern("HTTP_ACCEPT_ENCODING");
  accept_charset=fd_intern("HTTP_ACCEPT_CHARSET");
  http_referrer=fd_intern("HTTP_REFERRER");

  http_cookie=fd_intern("HTTP_COOKIE");
  outcookies_symbol=fd_intern("_COOKIES%OUT");
  incookies_symbol=fd_intern("_COOKIES%IN");
  bad_cookie=fd_intern("_BADCOOKIES");
  cookiedata_symbol=fd_intern("_COOKIEDATA");

  server_port=fd_intern("SERVER_PORT");
  remote_port=fd_intern("REMOTE_PORT");
  request_method=fd_intern("REQUEST_METHOD");

  query_string=fd_intern("QUERY_STRING");
  query_elts=fd_intern("QELTS");
  query=fd_intern("QUERY");

  get_method=fd_intern("GET");
  post_method=fd_intern("POST");

  status_field=fd_intern("STATUS");
  redirect_field=fd_intern("_REDIRECT");
  sendfile_field=fd_intern("_SENDFILE");
  http_headers=fd_intern("HTTP-HEADERS");
  html_headers=fd_intern("HTML-HEADERS");

  content_type=fd_intern("CONTENT-TYPE");
  cgi_content_type=fd_intern("CONTENT_TYPE");
  content_slotid=fd_intern("CONTENT");

  doctype_slotid=fd_intern("DOCTYPE");
  xmlpi_slotid=fd_intern("XMLPI");
  html_attribs_slotid=fd_intern("%HTML");
  body_attribs_slotid=fd_intern("%BODY");
  body_classes_slotid=fd_intern("%BODYCLASSES");
  class_symbol=fd_intern("CLASS");

  post_data_slotid=fd_intern("POST_DATA");
  form_data_string=fdtype_string("multipart/form-data");

  filename_slotid=fd_intern("FILENAME");
  name_slotid=fd_intern("NAME");
  text_symbol=fd_intern("TEXT");
  parts_slotid=fd_intern("PARTS");

  remote_user_symbol=fd_intern("REMOTE_USER");
  remote_host_symbol=fd_intern("REMOTE_HOST");
  remote_addr_symbol=fd_intern("REMOTE_ADDR");
  remote_ident_symbol=fd_intern("REMOTE_IDENT");
  remote_agent_symbol=fd_intern("HTTP_USER_AGENT");
  remote_info_symbol=fd_intern("REMOTE_INFO");

  ipeval_symbol=fd_intern("_IPEVAL");

  fd_register_config
    ("CGIPREP",
     _("Functions to execute between parsing and responding to a CGI request"),
     fd_lconfig_get,fd_lconfig_set,&cgi_prepfns);
  fd_register_config
    ("XSENDFILE",
     _("Header for using the web server's X-SENDFILE functionality"),
     fd_sconfig_get,sendfile_set,&fd_sendfile_header);
  fd_register_config
    ("LOGCGI",_("Whether to log CGI bindings passed to FramerD"),
     fd_boolconfig_get,fd_boolconfig_set,&log_cgidata);
  fd_register_config
    ("QUERYONPOST",
     _("Whether to parse REST query args when there is POST data"),
     fd_boolconfig_get,fd_boolconfig_set,&parse_query_on_post);
  fd_register_config
    ("CGI:PROTECT",_("Fields to avoid binding directly for CGI requests"),
     protected_cgi_get,protected_cgi_set,NULL);

  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
