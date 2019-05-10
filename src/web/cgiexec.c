/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
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
#include "framerd/tables.h"
#include "framerd/eval.h"
#include "framerd/storage.h"
#include "framerd/ports.h"
#include "framerd/fdweb.h"
#include "framerd/support.h"

#include <libu8/libu8.h>
#include <libu8/libu8io.h>
#include <libu8/u8xfiles.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8streamio.h>

#define fast_eval(x,env) (_fd_fast_eval(x,env,_stack,0))

#include <ctype.h>

static lispval accept_language, accept_type, accept_charset, accept_encoding;
static lispval server_port, remote_port, request_method, request_uri, status_field;
static lispval get_method, post_method, browseinfo_symbol, params_symbol;
static lispval redirect_field, sendfile_field, xredirect_field;
static lispval query_string, query_elts, query, http_cookie, http_referrer;
static lispval http_headers, html_headers, cookiedata_symbol;
static lispval outcookies_symbol, incookies_symbol, bad_cookie;
static lispval doctype_slotid, xmlpi_slotid, html_attribs_slotid;
static lispval body_attribs_slotid, body_classes_slotid, html_classes_slotid;
static lispval class_symbol;
static lispval content_slotid, content_type, cgi_content_type;
static lispval content_length, incoming_content_length, incoming_content_type;
static lispval remote_user_symbol, remote_host_symbol, remote_addr_symbol;
static lispval remote_info_symbol, remote_agent_symbol, remote_ident_symbol;
static lispval parts_slotid, name_slotid, filename_slotid, mapurlfn_symbol;
static lispval ipeval_symbol;

char *fd_sendfile_header = NULL; /* X-Sendfile */
char *fd_xredirect_header="X-Redirect";
static int log_cgidata = 0;

static u8_condition CGIDataInconsistency="Inconsistent CGI data";

/* Utility functions */

static lispval try_parse(u8_string buf)
{
  lispval val = fd_parse((buf[0]==':')?(buf+1):(buf));
  if (FD_ABORTP(val)) {
    fd_decref(val); return lispval_string(buf);}
  else return val;
}

static lispval buf2lisp(char *buf,int isascii)
{
  if (buf[0]!=':')
    if (isascii) return lispval_string(buf);
    else return fd_lispstring(u8_valid_copy(buf));
  else if (isascii) return try_parse(buf);
  else {
    u8_string s = u8_valid_copy(buf);
    lispval value = try_parse(s);
    u8_free(s);
    return value;}
}

static lispval buf2slotid(char *buf,int isascii)
{
  if (isascii) return fd_parse(buf);
  else {
    u8_string s = u8_valid_copy(buf);
    lispval value = fd_parse(s);
    u8_free(s);
    return value;}
}

static lispval buf2string(char *buf,int isascii)
{
  if (isascii) return lispval_string(buf);
  else return fd_lispstring(u8_valid_copy(buf));
}

static void emit_uri_value(u8_output out,lispval val)
{
  if (STRINGP(val))
    fd_uri_output(out,CSTRING(val),STRLEN(val),0,NULL);
  else {
    struct U8_OUTPUT xout; u8_byte buf[256];
    U8_INIT_OUTPUT_X(&xout,256,buf,U8_FIXED_STREAM);
    fd_unparse(&xout,val);
    u8_putn(out,"%3a",3);
    fd_uri_output(out,xout.u8_outbuf,xout.u8_write-xout.u8_outbuf,0,NULL);
    if (xout.u8_outbuf!=buf) u8_free(xout.u8_outbuf);}
}

FD_EXPORT
void fd_urify(u8_output out,lispval val)
{
  emit_uri_value(out,val);
}

/* Converting cgidata */

#define MAX_PROTECTED_PARAMS 200

static lispval protected_params[MAX_PROTECTED_PARAMS];
static int n_protected_params = 0;

static u8_mutex protected_cgi_lock;

static int isprotected(lispval field)
{
  int i = 0, n = n_protected_params;
  while (i < n)
    if (protected_params[i] == field)
      return 1;
    else i++;
  return 0;
}
static lispval protected_cgi_get(lispval var,void *ptr)
{
  lispval result = EMPTY;
  int i = 0, n = n_protected_params;
  while (i < n) {
    lispval param = protected_params[i++];
    fd_incref(param);
    FD_ADD_TO_CHOICE(result,param);}
  return result;
}
static int protected_cgi_set(lispval var,lispval field,void *ptr)
{
  int i = 0, n = n_protected_params;
  while (i < n)
    if (protected_params[i] == field)
      return 0;
    else i++;
  u8_lock_mutex(&protected_cgi_lock);
  n = n_protected_params;
  while (i < n)
    if (protected_params[i] == field) {
      u8_unlock_mutex(&protected_cgi_lock);
      return 0;}
    else i++;
  if (i >= MAX_PROTECTED_PARAMS) {
    fd_seterr("TooManyProtectedParams","protected_cgi_set",NULL,var);
    u8_unlock_mutex(&protected_cgi_lock);
    return -1;}
  else {
    protected_params[n_protected_params++] = field;
    fd_incref(field);
    u8_unlock_mutex(&protected_cgi_lock);
    return 1;}
}


static void convert_parse(fd_slotmap c,lispval slotid)
{
  lispval value = fd_slotmap_get(c,slotid,VOID);
  if (!(STRINGP(value))) {fd_decref(value);}
  else {
    fd_slotmap_store(c,slotid,try_parse(CSTRING(value)));
    fd_decref(value);}
}

static void convert_accept(fd_slotmap c,lispval slotid)
{
  lispval value = fd_slotmap_get(c,slotid,VOID);
  if (!(STRINGP(value))) {fd_decref(value);}
  else {
    lispval newvalue = EMPTY, entry;
    const u8_byte *data = CSTRING(value), *scan = data;
    const u8_byte *comma = strchr(scan,','), *semi = strstr(scan,";q=");
    while (comma) {
      if ((semi) && (semi<comma))
        entry = fd_conspair(fd_substring(scan,semi),fd_substring(semi+3,comma));
      else entry = fd_substring(scan,comma);
      CHOICE_ADD(newvalue,entry);
      scan = comma+1;
      comma = strchr(scan,',');
      semi = strstr(scan,";q=");
      if (semi)
        entry = fd_conspair(fd_substring(scan,semi),fd_substring(semi+3,NULL));
      else entry = fd_substring(scan,NULL);
      CHOICE_ADD(newvalue,entry);}
    fd_slotmap_store(c,slotid,newvalue);
    fd_decref(value);
    fd_decref(newvalue);}
}

/* Converting query arguments */

static lispval post_data_slotid, multipart_form_data, www_form_urlencoded;

/* Setting this to zero might be slightly more secure, and it's the
   CONFIG option QUERYWITHPOST (boolean) */
static int parse_query_on_post = 1;

static void parse_query_string(fd_slotmap c,const char *data,int len);

static lispval intern_compound(u8_string s1,u8_string s2)
{
  lispval result = VOID;
  struct U8_OUTPUT tmpout; u8_byte tmpbuf[128];
  U8_INIT_OUTPUT_X(&tmpout,128,tmpbuf,0);
  u8_puts(&tmpout,s1); u8_puts(&tmpout,s2);
  result = fd_parse(tmpout.u8_outbuf);
  u8_close_output(&tmpout);
  return result;
}

static void get_form_args(fd_slotmap c)
{
  if ( (fd_test((lispval)c,request_method,post_method)) ||
       (fd_test((lispval)c,cgi_content_type,multipart_form_data)) ||
       (fd_test((lispval)c,cgi_content_type,www_form_urlencoded)) ) {
    fd_handle_compound_mime_field((lispval)c,cgi_content_type,VOID);
    if (parse_query_on_post) {
      /* We do this first, so it won't override any post data
         which is supposedly more legitimate. */
      lispval qval = fd_slotmap_get(c,query_string,VOID);
      if (STRINGP(qval))
        parse_query_string(c,FD_STRING_DATA(qval),FD_STRING_LENGTH(qval));
      fd_decref(qval);}
    if (fd_test((lispval)c,cgi_content_type,multipart_form_data)) {
      lispval postdata = fd_slotmap_get(c,post_data_slotid,VOID);
      lispval parts = NIL;
      /* Parse the MIME data into parts */
      if (STRINGP(postdata))
        parts = fd_parse_multipart_mime
          ((lispval)c,FD_STRING_DATA(postdata),
           FD_STRING_DATA(postdata)+FD_STRING_LENGTH(postdata));
      else if (PACKETP(postdata))
        parts = fd_parse_multipart_mime
          ((lispval)c,FD_PACKET_DATA(postdata),
           FD_PACKET_DATA(postdata)+FD_PACKET_LENGTH(postdata));
      fd_add((lispval)c,parts_slotid,parts);
      fd_decref(postdata);
      /* Convert the parts */
      {FD_DOLIST(elt,parts) {
        lispval namestring = fd_get(elt,name_slotid,VOID);
        if (STRINGP(namestring)) {
          u8_string nstring = FD_STRING_DATA(namestring);
          lispval namesym = fd_parse(nstring);
          lispval partsym = intern_compound("__",nstring);
          lispval ctype = fd_get(elt,content_type,VOID);
          lispval content = fd_get((lispval)elt,content_slotid,EMPTY);
          /* Add the part itself in the _name slotid */
          fd_add((lispval)c,partsym,elt);
          /* Add the filename slot if it's an upload */
          if (fd_test(elt,filename_slotid,VOID)) {
            lispval filename = fd_get(elt,filename_slotid,EMPTY);
            fd_add((lispval)c,intern_compound(nstring,"_FILENAME"),
                   filename);
            fd_decref(filename);}
          if (fd_test(elt,content_type,VOID)) {
            lispval ctype = fd_get(elt,content_type,EMPTY);
            fd_add((lispval)c,intern_compound(nstring,"_TYPE"),ctype);
            fd_decref(ctype);}
          if ((VOIDP(ctype)) || (fd_overlapp(ctype,FDSYM_TEXT))) {
            if (STRINGP(content)) {
              u8_string chars = CSTRING(content); int len = STRLEN(content);
              /* Remove trailing \r\n from the MIME field */
              if ((len>1) && (chars[len-1]=='\n')) {
                lispval new_content;
                if (chars[len-2]=='\r')
                  new_content = fd_substring(chars,chars+len-2);
                else new_content = fd_substring(chars,chars+len-1);
                fd_add((lispval)c,namesym,new_content);
                fd_decref(new_content);}
              else fd_add((lispval)c,namesym,content);}
            else fd_add((lispval)c,namesym,content);}
          else fd_add((lispval)c,namesym,content);
          fd_decref(content); fd_decref(ctype);}
        fd_decref(namestring);}}
      fd_decref(parts);}
    else if (fd_test((lispval)c,cgi_content_type,www_form_urlencoded)) {
      lispval qval = fd_slotmap_get(c,post_data_slotid,VOID);
      u8_string data; unsigned int len;
      if (STRINGP(qval)) {
        data = CSTRING(qval); len = FD_STRING_LENGTH(qval);}
      else if (PACKETP(qval)) {
        data = FD_PACKET_DATA(qval); len = FD_PACKET_LENGTH(qval);}
      else {
        fd_decref(qval); data = NULL;}
      if ((data) && (strchr(data,'=')))
        parse_query_string(c,data,len);
      fd_decref(qval);}
    else {}}
  else {
    lispval qval = fd_slotmap_get(c,query_string,VOID);
    if (STRINGP(qval))
      parse_query_string(c,FD_STRING_DATA(qval),FD_STRING_LENGTH(qval));
    fd_decref(qval);
    return;}
}

static void add_param(fd_slotmap req,fd_slotmap params,
                      lispval slotstring,lispval slotid,
                      lispval val)
{
  if (FD_VOIDP(slotstring))
    fd_slotmap_add(req,query,val);
  else {
    fd_slotmap_add(params,slotstring,val);
    if (!(FD_VOIDP(slotid))) {
      fd_slotmap_add(params,slotid,val);
      fd_slotmap_add(req,slotid,val);}}
}

static void parse_query_string(fd_slotmap c,const char *data,int len)
{
  int isascii = 1;
  lispval slotid = VOID, slotstring = VOID, value = VOID;
  lispval params_val = fd_slotmap_get(c,params_symbol,FD_VOID);
  fd_slotmap q = NULL;
  if (FD_SLOTMAPP(params_val))
    q = (fd_slotmap) params_val;
  else {
    params_val = fd_init_slotmap(NULL,32,NULL);
    q = (fd_slotmap) params_val;}
  const u8_byte *scan = data, *end = scan+len;
  char *buf = u8_malloc(len+1), *write = buf;
  while (scan<end)
    if ( (VOIDP(slotid)) && (*scan=='=') ) {
      *write++='\0';
      /* Don't store vars beginning with _ or HTTP, to avoid spoofing
         of real HTTP variables or other variables that might be used
         internally. */
      if ( (buf[0]=='_') ||
           (strncmp(buf,"HTTP_",5)==0) ||
           (strncmp(buf,"SERVER_",7)==0) )
        slotid = VOID;
      else slotid = buf2slotid(buf,isascii);
      fd_decref(slotstring); slotstring = VOID;
      fd_decref(value); value = VOID;
      slotstring = buf2string(buf,isascii);
      if (isprotected(slotid)) slotid = VOID;
      write = buf;
      isascii = 1;
      scan++;}
    else if ( *scan == '&' ) {
      *write++='\0';
      value = buf2string(buf,isascii);
      add_param(c,q,slotstring,slotid,value);
      fd_decref(value); value = VOID; slotid = VOID;
      write = buf;
      isascii = 1;
      scan++;}
    else if (*scan == '%')
      if (scan+3>end) end = scan;
      else {
        char buf[4]; int c; scan++;
        buf[0] = *scan++;
        buf[1] = *scan++;
        buf[2] ='\0';
        c = strtol(buf,NULL,16);
        if (c>=0x80) isascii = 0;
        *write++=c;}
    else if (*scan == '+') {
      *write++=' ';
      scan++;}
    else if (*scan<0x80) {
      *write++= *scan++;}
    else {
      *write++= *scan++;
      isascii = 0;}
  if (write>buf) {
    *write++='\0';
    value = buf2string(buf,isascii);
    add_param(c,q,slotstring,slotid,value);
    fd_decref(value); value = VOID; slotid = VOID;
    write = buf;
    isascii = 1;
    scan++;}
  else if (!(VOIDP(slotid))) {
    lispval str = lispval_string("");
    fd_slotmap_add(c,slotid,str);
    fd_decref(str);}
  if (q->n_slots) {
    fd_slotmap_add(c,params_symbol,params_val);}
  else fd_decref(params_val);
  u8_free(buf);
}

/* Converting cookie args */

static void setcookiedata(lispval cookiedata,lispval cgidata)
{
  /* This replaces any existing cookie data which matches cookiedata */
  lispval cookies=
    ((TABLEP(cgidata))?
     (fd_get(cgidata,outcookies_symbol,EMPTY)):
     (fd_req_get(outcookies_symbol,EMPTY)));
  lispval cookievar = VEC_REF(cookiedata,0);
  DO_CHOICES(cookie,cookies) {
    if (FD_EQ(VEC_REF(cookie,0),cookievar)) {
      if (TABLEP(cgidata))
        fd_drop(cgidata,outcookies_symbol,cookie);
      else fd_req_drop(outcookies_symbol,cookie);}}
  fd_decref(cookies);
  if ((cgidata)&&(TABLEP(cgidata)))
    fd_add(cgidata,outcookies_symbol,cookiedata);
  else fd_req_add(outcookies_symbol,cookiedata);
}

static void convert_cookie_arg(fd_slotmap c)
{
  lispval qval = fd_slotmap_get(c,http_cookie,VOID);
  if (!(STRINGP(qval))) {fd_decref(qval); return;}
  else {
    lispval slotid = VOID, name = VOID, value = VOID;
    int len = STRLEN(qval); int isascii = 1, badcookie = 0;
    const u8_byte *scan = CSTRING(qval), *end = scan+len;
    char *buf = u8_malloc(len), *write = buf;
    while (scan<end)
      if ((VOIDP(slotid)) && (*scan=='=')) {
        /* There's an assignment going on. */
        *write++='\0';
        name = lispval_string(buf);
        if ((buf[0]=='_')||(strncmp(buf,"HTTP",4)==0)) badcookie = 1;
        else badcookie = 0;
        if (isascii) slotid = fd_parse(buf);
        else {
          u8_string s = u8_valid_copy(buf);
          slotid = fd_parse(s);
          u8_free(s);}
        write = buf; isascii = 1;
        scan++;}
      else if (*scan==';') {
        *write++='\0';
        if (VOIDP(slotid))
          /* If there isn't a slot/symbol/etc, just store a string */
          value = buf2string(buf,isascii);
        else /* Otherwise, parse to LISP */
          value = buf2lisp(buf,isascii);
        if (VOIDP(slotid))
          u8_log(LOG_WARN,_("malformed cookie"),"strange cookie syntax: \"%s\"",
                 CSTRING(qval));
        else {
          lispval cookiedata = fd_make_nvector(2,name,fd_incref(value));
          if (badcookie)
            fd_slotmap_add(c,bad_cookie,cookiedata);
          else fd_slotmap_add(c,slotid,value);
          fd_slotmap_add(c,cookiedata_symbol,cookiedata);
          setcookiedata(cookiedata,(lispval)c);
          name = VOID;
          fd_decref(cookiedata);}
        fd_decref(value); value = VOID; slotid = VOID;
        write = buf; isascii = 1; scan++;}
      else if (*scan == '%')
        if (scan+3>end) {
          u8_log(LOG_WARN,_("malformed cookie"),"cookie ends early: \"%s\"",
                 CSTRING(qval));
          end = scan;}
        else {
          char buf[4]; int c; scan++;
          buf[0]= *scan++; buf[1]= *scan++; buf[2]='\0';
          c = strtol(buf,NULL,16);
          if (c>=0x80) isascii = 0;
          *write++=c;}
      else if (*scan == '+') {*write++=' '; scan++;}
      else if (*scan == ' ') scan++;
      else {
        if (*scan<0x80) isascii = 0;
        *write++= *scan++; }
    if (write>buf) {
      *write++='\0';
      if (VOIDP(slotid)) value = buf2string(buf,isascii);
      else value = buf2lisp(buf,isascii);
      if (VOIDP(slotid))
        u8_log(LOG_WARN,_("malformed cookie"),"strange cookie syntax: \"%s\"",
                CSTRING(qval));
      else {
        lispval cookiedata = fd_make_nvector(2,name,fd_incref(value));
        if (badcookie)
          fd_slotmap_add(c,bad_cookie,cookiedata);
        else fd_slotmap_add(c,slotid,value);
        fd_slotmap_add(c,cookiedata_symbol,cookiedata);
        setcookiedata(cookiedata,(lispval)c);
        fd_decref(cookiedata);}
      fd_decref(value); value = VOID; slotid = VOID;
      write = buf; isascii = 1; scan++;}
    fd_slotmap_add(c,incookies_symbol,qval);
    fd_slotmap_store(c,outcookies_symbol,EMPTY);
    fd_decref(qval);
    u8_free(buf);}
}

/* Parsing CGI data */

static lispval cgi_prepfns = EMPTY;
static void add_remote_info(lispval cgidata);

FD_EXPORT int fd_parse_cgidata(lispval data)
{
  struct FD_SLOTMAP *cgidata = FD_XSLOTMAP(data);
  lispval ctype = fd_get(data,content_type,VOID);
  lispval clen = fd_get(data,content_length,VOID);
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
  if (!(VOIDP(ctype))) {
    fd_slotmap_store(cgidata,incoming_content_type,ctype);
    fd_slotmap_drop(cgidata,content_type,VOID);
    fd_decref(ctype);}
  if (!(VOIDP(clen))) {
    fd_slotmap_store(cgidata,incoming_content_length,clen);
    fd_slotmap_drop(cgidata,content_length,VOID);
    fd_decref(clen);}
  {DO_CHOICES(handler,cgi_prepfns) {
    if (FD_APPLICABLEP(handler)) {
      lispval value = fd_apply(handler,1,&data);
      fd_decref(value);}
    else u8_log(LOG_WARN,"Not Applicable","Invalid CGI prep handler %q",handler);}}
  if (!(fd_slotmap_test(cgidata,FDSYM_PCTID,FD_VOID))) {
    lispval req_uri = fd_slotmap_get(cgidata,request_uri,FD_VOID);
    if (FD_STRINGP(req_uri)) fd_slotmap_store(cgidata,FDSYM_PCTID,req_uri);
    fd_decref(req_uri);}
  return 1;
}

static void add_remote_info(lispval cgidata)
{
  /* This combines a bunch of different request properties into a single string */
  lispval remote_user = fd_get(cgidata,remote_user_symbol,VOID);
  lispval remote_ident = fd_get(cgidata,remote_ident_symbol,VOID);
  lispval remote_host = fd_get(cgidata,remote_host_symbol,VOID);
  lispval remote_addr = fd_get(cgidata,remote_addr_symbol,VOID);
  lispval remote_agent = fd_get(cgidata,remote_agent_symbol,VOID);
  lispval remote_string = VOID;
  struct U8_OUTPUT remote;
  U8_INIT_OUTPUT(&remote,128);
  u8_printf(&remote,"%s%s%s@%s%s%s(%s)",
            ((STRINGP(remote_ident)) ? (CSTRING(remote_ident)) : ((u8_string)"")),
            ((STRINGP(remote_ident)) ? ((u8_string)"|") : ((u8_string)"")),
            ((STRINGP(remote_user)) ? (CSTRING(remote_user)) : ((u8_string)"nobody")),
            ((STRINGP(remote_host)) ? (CSTRING(remote_host)) : ((u8_string)"")),
            ((STRINGP(remote_host)) ? ((u8_string)"/") : ((u8_string)"")),
            ((STRINGP(remote_addr)) ? (CSTRING(remote_addr)) : ((u8_string)"noaddr")),
            ((STRINGP(remote_agent)) ? (CSTRING(remote_agent)) : ((u8_string)"noagent")));
  remote_string=
    fd_init_string(NULL,remote.u8_write-remote.u8_outbuf,
                   remote.u8_outbuf);
  fd_store(cgidata,remote_info_symbol,remote_string);
  fd_decref(remote_user); fd_decref(remote_ident); fd_decref(remote_host);
  fd_decref(remote_addr); fd_decref(remote_agent);
  fd_decref(remote_string);
}

/* Generating headers */

static lispval do_xmlout(U8_OUTPUT *out,lispval body,
                        fd_lexenv env,fd_stack _stack)
{
  U8_OUTPUT *prev = u8_current_output;
  u8_set_default_output(out);
  while (PAIRP(body)) {
    lispval value = fast_eval(FD_CAR(body),env);
    body = FD_CDR(body);
    if (FD_ABORTP(value)) {
      u8_set_default_output(prev);
      return value;}
    else if (VOIDP(value)) continue;
    else if (STRINGP(value))
      u8_printf(out,"%s",CSTRING(value));
    else u8_printf(out,"%q",value);
    fd_decref(value);}
  u8_set_default_output(prev);
  return VOID;
}

static lispval httpheader(lispval expr,fd_lexenv env,fd_stack _stack)
{
  U8_OUTPUT out; lispval result;
  U8_INIT_OUTPUT(&out,64);
  result = do_xmlout(&out,fd_get_body(expr,1),env,_stack);
  if (FD_ABORTP(result)) {
    u8_free(out.u8_outbuf);
    return result;}
  else {
    lispval header=
      fd_init_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);
    fd_req_add(http_headers,header);
    fd_decref(header);
    return VOID;}
}

static lispval addhttpheader(lispval header)
{
  fd_req_add(http_headers,header);
  return VOID;
}

static lispval htmlheader(lispval expr,fd_lexenv env,fd_stack _stack)
{
  U8_OUTPUT out; lispval header_string; lispval result;
  U8_INIT_OUTPUT(&out,64);
  result = do_xmlout(&out,fd_get_body(expr,1),env,_stack);
  if (FD_ABORTP(result)) {
    u8_free(out.u8_outbuf);
    return result;}
  header_string = fd_init_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);
  fd_req_push(html_headers,header_string);
  fd_decref(header_string);
  return VOID;
}

/* Handling cookies */

static int handle_cookie(U8_OUTPUT *out,lispval cgidata,lispval cookie)
{
  int len; lispval real_val, name;
  if (!(VECTORP(cookie))) return -1;
  else len = VEC_LEN(cookie);
  if (len<2) return -1;
  else name = VEC_REF(cookie,0);
  if (!((SYMBOLP(name))||(STRINGP(name))||(OIDP(name))))
    return -1;
  if (TABLEP(cgidata)) {
    real_val = fd_get(cgidata,VEC_REF(cookie,0),VOID);
    if (VOIDP(real_val)) real_val = fd_incref(VEC_REF(cookie,1));}
  else real_val = fd_incref(VEC_REF(cookie,1));
  if ((len>2) || (!(FD_EQ(real_val,VEC_REF(cookie,1))))) {
    lispval var = VEC_REF(cookie,0);
    u8_string namestring; const u8_byte *scan;
    int free_namestring = 0, c;
    u8_printf(out,"Set-Cookie: ");
    if (SYMBOLP(var)) namestring = SYM_NAME(var);
    else if (STRINGP(var)) namestring = CSTRING(var);
    else {
      namestring = fd_lisp2string(var);
      free_namestring = 1;}
    scan = namestring; c = u8_sgetc(&scan);
    if ((c=='$')|| (c>=0x80) || (u8_isspace(c)) || (c==';') || (c==','))
      u8_printf(out,"\\u%04x",c);
    else u8_putc(out,c);
    c = u8_sgetc(&scan);
    while (c>0) {
      if ((c>=0x80) || (u8_isspace(c)) || (c==';') || (c==','))
        u8_printf(out,"\\u%04x",c);
      else u8_putc(out,c);
      c = u8_sgetc(&scan);}
    u8_puts(out,"=");
    if (free_namestring) u8_free(namestring);
    emit_uri_value(out,VEC_REF(cookie,1));
    if (len>2) {
      lispval domain = VEC_REF(cookie,2);
      lispval path = ((len>3) ? (VEC_REF(cookie,3)) : (VOID));
      lispval expires = ((len>4) ? (VEC_REF(cookie,4)) : (VOID));
      lispval secure = ((len>5) ? (VEC_REF(cookie,5)) : (VOID));
      if (STRINGP(domain))
        u8_printf(out,"; Domain=%s",CSTRING(domain));
      if (STRINGP(path))
        u8_printf(out,"; Path=%s",CSTRING(path));
      if (STRINGP(expires))
        u8_printf(out,"; Expires=%s",CSTRING(expires));
      else if (TYPEP(expires,fd_timestamp_type)) {
        struct FD_TIMESTAMP *tstamp = (fd_timestamp)expires;
        char buf[512]; struct tm tptr;
        u8_xtime_to_tptr(&(tstamp->u8xtimeval),&tptr);
        strftime(buf,512,"%A, %d-%b-%Y %T GMT",&tptr);
        u8_printf(out,"; Expires=%s",buf);}
      if (!((VOIDP(secure))||(FALSEP(secure))))
        u8_printf(out,"; Secure");}
    u8_printf(out,"\r\n");}
  fd_decref(real_val);
  return 1;
}

static lispval setcookie
  (lispval var,lispval val,
   lispval domain,lispval path,
   lispval expires,lispval secure)
{
  if (!((SYMBOLP(var)) || (STRINGP(var)) || (OIDP(var))))
    return fd_type_error("symbol","setcookie",var);
  else {
    lispval cookiedata;
    if (VOIDP(domain)) domain = FD_FALSE;
    if (VOIDP(path)) path = FD_FALSE;
    if (VOIDP(expires)) expires = FD_FALSE;
    if ((FALSEP(expires))||
        (STRINGP(expires))||
        (TYPEP(expires,fd_timestamp_type)))
      fd_incref(expires);
    else if ((FIXNUMP(expires))||(FD_BIGINTP(expires))) {
      long long ival = ((FIXNUMP(expires))?(FIX2INT(expires)):
                      (fd_bigint_to_long_long((fd_bigint)expires)));
      time_t expval;
      if ((ival<0)||(ival<(24*3600*365*20))) expval = (time(NULL))+ival;
      else expval = (time_t)ival;
      expires = fd_time2timestamp(expval);}
    else return fd_type_error("timestamp","setcookie",expires);
    cookiedata=
      fd_make_nvector(6,fd_incref(var),fd_incref(val),
                      fd_incref(domain),fd_incref(path),
                      expires,fd_incref(secure));
    setcookiedata(cookiedata,VOID);
    fd_decref(cookiedata);
    return VOID;}
}

static lispval clearcookie
  (lispval var,lispval domain,lispval path,lispval secure)
{
  if (!((SYMBOLP(var)) || (STRINGP(var)) || (OIDP(var))))
    return fd_type_error("symbol","setcookie",var);
  else {
    lispval cookiedata;
    lispval val = lispval_string("");
    time_t past = time(NULL)-(3600*24*7*50);
    lispval expires = fd_time2timestamp(past);
    if (VOIDP(domain)) domain = FD_FALSE;
    if (VOIDP(path)) path = FD_FALSE;
    cookiedata=
      fd_make_nvector(6,fd_incref(var),val,
                      fd_incref(domain),fd_incref(path),
                      expires,fd_incref(secure));
    setcookiedata(cookiedata,VOID);
    fd_decref(cookiedata);
    return VOID;}
}

/* HTML Header functions */

static lispval add_stylesheet(lispval stylesheet,lispval type)
{
  lispval header_string = VOID;
  U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
  if (VOIDP(type))
    u8_printf(&out,"<link rel='stylesheet' type='text/css' href='%s'/>\n",
              CSTRING(stylesheet));
  else u8_printf(&out,"<link rel='stylesheet' type='%s' href='%s'/>\n",
                 CSTRING(type),CSTRING(stylesheet));
  header_string = fd_init_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);
  fd_req_push(html_headers,header_string);
  fd_decref(header_string);
  return VOID;
}

static lispval add_javascript(lispval url)
{
  lispval header_string = VOID;
  U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
  u8_printf(&out,"<script language='javascript' src='%s'>\n",
            CSTRING(url));
  u8_printf(&out,"  <!-- empty content for some browsers -->\n");
  u8_printf(&out,"</script>\n");
  header_string = fd_init_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);
  fd_req_push(html_headers,header_string);
  fd_decref(header_string);
  return VOID;
}

static lispval title_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  U8_OUTPUT out; lispval result;
  U8_INIT_OUTPUT(&out,64);
  u8_puts(&out,"<title>");
  result = do_xmlout(&out,fd_get_body(expr,1),env,_stack);
  u8_puts(&out,"</title>\n");
  if (FD_ABORTP(result)) {
    u8_free(out.u8_outbuf);
    return result;}
  else {
    lispval header_string=
      fd_init_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);
    fd_req_push(html_headers,header_string);
    fd_decref(header_string);
    return VOID;}
}

static lispval jsout_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  U8_OUTPUT *prev = u8_current_output;
  U8_OUTPUT _out, *out = &_out; lispval result = VOID;
  U8_INIT_OUTPUT(&_out,2048);
  u8_set_default_output(out);
  u8_puts(&_out,"<script language='javascript'>\n");
  {lispval body = fd_get_body(expr,1);
    FD_DOLIST(x,body) {
      if (STRINGP(x))
        u8_puts(&_out,CSTRING(x));
      else if ((SYMBOLP(x))||(PAIRP(x))||(FD_CODEP(x))) {
        result = fd_eval(x,env);
        if (FD_ABORTP(result)) break;
        else if ((VOIDP(result))||(FALSEP(result))||
                 (EMPTYP(result))) {}
        else if (STRINGP(result))
          u8_puts(&_out,CSTRING(result));
        else fd_unparse(&_out,result);
        fd_decref(result); result = VOID;}
      else fd_unparse(&_out,x);}}
  u8_puts(&_out,"\n</script>\n");
  if (FD_ABORTP(result)) {
    u8_free(_out.u8_outbuf);
    u8_set_default_output(prev);
    return result;}
  else {
    lispval header_string=
      fd_init_string(NULL,_out.u8_write-_out.u8_outbuf,_out.u8_outbuf);
    u8_set_default_output(prev);
    fd_req_push(html_headers,header_string);
    fd_decref(header_string);
    return VOID;}
}

static lispval cssout_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  U8_OUTPUT *prev = u8_current_output;
  U8_OUTPUT _out, *out = &_out;
  lispval result = VOID, body = fd_get_body(expr,1);
  U8_INIT_OUTPUT(&_out,2048);
  u8_set_default_output(out);
  u8_puts(&_out,"<style type='text/css'>\n");
  {FD_DOLIST(x,body) {
      if (STRINGP(x))
        u8_puts(&_out,CSTRING(x));
      else if ((SYMBOLP(x))||(PAIRP(x))||(FD_CODEP(x))) {
        result = fd_eval(x,env);
        if (FD_ABORTP(result)) break;
        else if ((VOIDP(result))||(FALSEP(result))||
                 (EMPTYP(result))) {}
        else if (STRINGP(result))
          u8_puts(&_out,CSTRING(result));
        else fd_unparse(&_out,result);
        fd_decref(result); result = VOID;}
      else fd_unparse(&_out,x);}}
  u8_puts(&_out,"\n</style>\n");
  if (FD_ABORTP(result)) {
    u8_free(_out.u8_outbuf);
    u8_set_default_output(prev);
    return result;}
  else {
    lispval header_string=
      fd_init_string(NULL,_out.u8_write-_out.u8_outbuf,_out.u8_outbuf);
    u8_set_default_output(prev);
    fd_req_push(html_headers,header_string);
    fd_decref(header_string);
    return VOID;}
}

/* Generating the HTTP header */
FD_EXPORT int fd_output_http_headers(U8_OUTPUT *out,lispval cgidata)
{
  lispval ctype = fd_get(cgidata,content_type,VOID);
  lispval status = fd_get(cgidata,status_field,VOID);
  lispval headers = fd_get(cgidata,http_headers,EMPTY);
  lispval redirect = fd_get(cgidata,redirect_field,VOID);
  lispval sendfile = fd_get(cgidata,sendfile_field,VOID);
  lispval xredirect = fd_get(cgidata,xredirect_field,VOID);
  lispval cookies = fd_get(cgidata,outcookies_symbol,EMPTY);
  int keep_doctype = 0, http_status = -1;
  if ((STRINGP(redirect))&&(VOIDP(status))) {
    status = FD_INT(303); http_status = 303;}
  if (FD_UINTP(status)) {
    u8_printf(out,"Status: %d\r\n",FIX2INT(status));
    http_status = FIX2INT(status);}
  else if (STRINGP(status)) {
    u8_printf(out,"Status: %s\r\n",CSTRING(status));
    http_status = atoi(CSTRING(status));}
  else http_status = 200;
  if (STRINGP(ctype))
    u8_printf(out,"Content-type: %s\r\n",CSTRING(ctype));
  else u8_printf(out,"%s\r\n",DEFAULT_CONTENT_TYPE);
  {DO_CHOICES(header,headers)
      if (STRINGP(header)) {
        u8_putn(out,CSTRING(header),STRLEN(header));
        u8_putn(out,"\r\n",2);}
      else if (PACKETP(header)) {
        /* This handles both packets and secrets */
        u8_putn(out,FD_PACKET_DATA(header),FD_PACKET_LENGTH(header));
        u8_putn(out,"\r\n",2);}}
  {DO_CHOICES(cookie,cookies)
     if (handle_cookie(out,cgidata,cookie)<0)
       u8_log(LOG_WARN,CGIDataInconsistency,"Bad cookie data: %q",cookie);}
  if (STRINGP(redirect))
    u8_printf(out,"Location: %s\r\n",CSTRING(redirect));
  else if ((STRINGP(sendfile))&&(fd_sendfile_header)) {
    u8_log(LOG_DEBUG,"Sendfile","Using %s to pass %s",
           fd_sendfile_header,CSTRING(sendfile));
    u8_printf(out,"Content-length: 0\r\n");
    u8_printf(out,"%s: %s\r\n",fd_sendfile_header,CSTRING(sendfile));}
  else if ((STRINGP(xredirect))&&(fd_xredirect_header)) {
    u8_log(LOG_DEBUG,"Xredirect","Using %s to pass %s",
           fd_xredirect_header,CSTRING(xredirect));
    u8_printf(out,"%s: %s\r\n",fd_xredirect_header,CSTRING(xredirect));}
  else keep_doctype = 1;
  if (!(keep_doctype)) fd_store(cgidata,doctype_slotid,FD_FALSE);
  fd_decref(ctype); fd_decref(status); fd_decref(headers);
  fd_decref(redirect); fd_decref(sendfile); fd_decref(cookies);
  return http_status;
}

static void output_headers(U8_OUTPUT *out,lispval headers)
{
  if (PAIRP(headers)) {
    lispval header = FD_CAR(headers);
    output_headers(out,FD_CDR(headers));
    u8_putn(out,CSTRING(header),STRLEN(header));
    u8_putn(out,"\n",1);}
}

static lispval attrib_merge_classes(lispval attribs,lispval classes)
{
  if (!(PAIRP(classes))) return attribs;
  else if (PAIRP(attribs)) {
    lispval scan = attribs;
    struct U8_OUTPUT classout; lispval class_cons = VOID;
    u8_byte _classbuf[256]; int i = 0;
    while (PAIRP(scan)) {
      lispval car = FD_CAR(scan);
      if ((car == class_symbol)||
          ((STRINGP(car))&&(strcasecmp(CSTRING(car),"class")==0))) {
        class_cons = FD_CDR(scan); break;}
      else {
        scan = FD_CDR(scan);
        if (PAIRP(scan)) scan = FD_CDR(scan);}}
    if (VOIDP(class_cons)) {
      class_cons = fd_conspair(FD_FALSE,attribs);
      attribs = fd_conspair(class_symbol,class_cons);}
    U8_INIT_STATIC_OUTPUT_BUF(classout,sizeof(_classbuf),_classbuf);
    if (STRINGP(FD_CAR(class_cons))) {
      u8_puts(&classout,CSTRING(FD_CAR(class_cons)));
      u8_puts(&classout," ");}
    while (PAIRP(classes)) {
      lispval car = FD_CAR(classes);
      if (!(STRINGP(car))) {}
      else {
        if (i) u8_puts(&classout," "); i++;
        u8_puts(&classout,CSTRING(car));}
      classes = FD_CDR(classes);}
    {lispval old = FD_CAR(class_cons);
      FD_RPLACA(class_cons,fd_make_string
                (NULL,classout.u8_write-classout.u8_outbuf,
                 classout.u8_outbuf));
      fd_decref(old);
      u8_close((u8_stream)&classout);}
    return attribs;}
  else {
    struct U8_OUTPUT classout;
    u8_byte _classbuf[256]; int i = 0;
    U8_INIT_STATIC_OUTPUT_BUF(classout,sizeof(_classbuf),_classbuf);
    {FD_DOLIST(class,classes) {
        if (STRINGP(class)) {
          if (i>0) u8_putc(&classout,' ');
          i++;
          u8_puts(&classout,CSTRING(class));}}}
    return fd_conspair(class_symbol,
                       fd_conspair(fd_stream_string(&classout),attribs));}
}

FD_EXPORT
int fd_output_xhtml_preface(U8_OUTPUT *out,lispval cgidata)
{
  lispval doctype = fd_get(cgidata,doctype_slotid,VOID);
  lispval xmlpi = fd_get(cgidata,xmlpi_slotid,VOID);
  lispval html_attribs = fd_get(cgidata,html_attribs_slotid,VOID);
  lispval html_classes = fd_get(cgidata,html_classes_slotid,VOID);
  lispval body_attribs = fd_get(cgidata,body_attribs_slotid,VOID);
  lispval body_classes = fd_get(cgidata,body_classes_slotid,VOID);
  lispval headers = fd_get(cgidata,html_headers,VOID);
  if (VOIDP(doctype)) u8_puts(out,DEFAULT_DOCTYPE);
  else if (STRINGP(doctype))
    u8_putn(out,CSTRING(doctype),STRLEN(doctype));
  else return 0;
  u8_putc(out,'\n');
  if (STRINGP(xmlpi))
    u8_putn(out,CSTRING(xmlpi),STRLEN(xmlpi));
  else u8_puts(out,DEFAULT_XMLPI);
  if (PAIRP(html_classes))
    html_attribs = attrib_merge_classes(html_attribs,html_classes);
  if (VOIDP(html_attribs))
    u8_puts(out,"\n<html>\n<head>\n");
  else {
    fd_open_markup(out,"html",html_attribs,0);
    u8_puts(out,"\n<head>\n");}
  if (!(VOIDP(headers))) {
    output_headers(out,headers);
    fd_decref(headers);}
  u8_puts(out,"</head>\n");
  if (PAIRP(body_classes))
    body_attribs = attrib_merge_classes(body_attribs,body_classes);
  if (FALSEP(body_attribs)) {}
  else if (VOIDP(body_attribs)) u8_puts(out,"<body>");
  else {
    fd_open_markup(out,"body",body_attribs,0);}
  fd_decref(doctype); fd_decref(xmlpi);
  fd_decref(html_attribs);
  fd_decref(body_attribs);
  fd_decref(body_classes);
  return 1;
}

FD_EXPORT
int fd_output_xml_preface(U8_OUTPUT *out,lispval cgidata)
{
  lispval doctype = fd_get(cgidata,doctype_slotid,VOID);
  lispval xmlpi = fd_get(cgidata,xmlpi_slotid,VOID);
  if (STRINGP(xmlpi))
    u8_putn(out,CSTRING(xmlpi),STRLEN(xmlpi));
  else u8_puts(out,DEFAULT_XMLPI);
  u8_putc(out,'\n');
  if (VOIDP(doctype)) u8_puts(out,DEFAULT_DOCTYPE);
  else if (STRINGP(doctype))
    u8_putn(out,CSTRING(doctype),STRLEN(doctype));
  else return 0;
  u8_putc(out,'\n');
  fd_decref(doctype); fd_decref(xmlpi);
  return 1;
}

static lispval set_body_attribs(int n,lispval *args)
{
  if ((n==1)&&(args[0]==FD_FALSE)) {
    fd_req_store(body_attribs_slotid,FD_FALSE);
    return VOID;}
  else {
    int i = n-1; while (i>=0) {
      lispval arg = args[i--];
      fd_req_push(body_attribs_slotid,arg);}
    return VOID;}
}

static lispval add_body_class(lispval classname)
{
  fd_req_push(body_classes_slotid,classname);
  return VOID;
}

static lispval add_html_class(lispval classname)
{
  fd_req_push(html_classes_slotid,classname);
  return VOID;
}

/* CGI Exec */

static lispval tail_symbol;

static lispval cgigetvar(lispval cgidata,lispval var)
{
  int noparse=
    ((SYMBOLP(var))&&((SYM_NAME(var))[0]=='%'));
  lispval name = ((noparse)?(fd_intern(SYM_NAME(var)+1)):(var));
  lispval val = ((TABLEP(cgidata))?(fd_get(cgidata,name,VOID)):
              (fd_req_get(name,VOID)));
  if (VOIDP(val)) return val;
  else if ((noparse)&&(STRINGP(val))) return val;
  else if (STRINGP(val)) {
    u8_string data = CSTRING(val);
    if (*data=='\0') return val;
    else if (strchr("@{#(",data[0])) {
      lispval parsed = fd_parse_arg(data);
      if (FD_ABORTP(parsed)) {
        fd_decref(parsed); fd_clear_errors(0);
        return val;}
      else {
        fd_decref(val); return parsed;}}
    else if (isdigit(data[0])) {
      lispval parsed = fd_parse_arg(data);
      if (FD_ABORTP(parsed)) {
        fd_decref(parsed); fd_clear_errors(0);
        return val;}
      else if (NUMBERP(parsed)) {
        fd_decref(val); return parsed;}
      else {
        fd_decref(parsed); return val;}}
    else if (*data == ':')
      if (data[1]=='\0')
        return lispval_string(data);
      else {
        lispval arg = fd_parse(data+1);
        if (FD_ABORTP(arg)) {
          u8_log(LOG_WARN,fd_ParseArgError,"Bad colon spec arg '%s'",arg);
          fd_clear_errors(1);
          return lispval_string(data);}
        else return arg;}
    else if (*data == '\\') {
      lispval shorter = lispval_string(data+1);
      fd_decref(val);
      return shorter;}
    else return val;}
  else if ((CHOICEP(val))||(PRECHOICEP(val))) {
    lispval result = EMPTY;
    DO_CHOICES(v,val) {
      if (!(STRINGP(v))) {
        fd_incref(v); CHOICE_ADD(result,v);}
      else {
        u8_string data = CSTRING(v); lispval parsed = v;
        if (*data=='\\') parsed = lispval_string(data+1);
        else if ((*data==':')&&(data[1]=='\0')) {fd_incref(parsed);}
        else if (*data==':')
          parsed = fd_parse(data+1);
        else if ((isdigit(*data))||(*data=='+')||(*data=='-')||(*data=='.')) {
          parsed = fd_parse_arg(data);
          if (!(NUMBERP(parsed))) {
            fd_decref(parsed); parsed = v; fd_incref(parsed);}}
        else if (strchr("@{#(",data[0]))
          parsed = fd_parse_arg(data);
        else fd_incref(parsed);
        if (FD_ABORTP(parsed)) {
          u8_log(LOG_WARN,fd_ParseArgError,"Bad LISP arg '%s'",data);
          fd_clear_errors(1);
          parsed = v; fd_incref(v);}
        CHOICE_ADD(result,parsed);}}
    fd_decref(val);
    return result;}
  else return val;
}

struct CGICALL {
  lispval proc, cgidata;
  struct U8_OUTPUT *cgiout; int outlen;
  lispval result;};

static int U8_MAYBE_UNUSED cgiexecstep(void *data)
{
  struct CGICALL *call = (struct CGICALL *)data;
  lispval proc = call->proc, cgidata = call->cgidata;
  lispval value;
  if (call->outlen<0)
    call->outlen = call->cgiout->u8_write-call->cgiout->u8_outbuf;
  else call->cgiout->u8_write = call->cgiout->u8_outbuf+call->outlen;
  value = fd_xapply_lambda((fd_lambda)proc,(void *)cgidata,
                          (lispval (*)(void *,lispval))cgigetvar);
  value = fd_finish_call(value);
  call->result = value;
  return 1;
}

#if FD_IPEVAL_ENABLED
static int use_ipeval(lispval proc,lispval cgidata)
{
  int retval = -1;
  struct FD_LAMBDA *sp = (struct FD_LAMBDA *)proc;
  lispval val = fd_symeval(ipeval_symbol,sp->env);
  if ((VOIDP(val))||(val == FD_UNBOUND)) retval = 0;
  else {
    lispval cgival = fd_get(cgidata,ipeval_symbol,VOID);
    if (VOIDP(cgival)) {
      if (FALSEP(val)) retval = 0; else retval = 1;}
    else if (FALSEP(cgival)) retval = 0;
    else retval = 1;
    fd_decref(cgival);}
  fd_decref(val);
  return retval;
}
#endif

FD_EXPORT lispval fd_cgiexec(lispval proc,lispval cgidata)
{
  if (FD_LAMBDAP(proc)) {
    lispval value = VOID;
#if FD_IPEVAL_ENABLED
    int ipeval = use_ipeval(proc,cgidata);
#else
    int ipeval = 0;
#endif
    if (!(ipeval)) {
      value = fd_xapply_lambda((fd_lambda)proc,(void *)cgidata,
                            (lispval (*)(void *,lispval))cgigetvar);
      value = fd_finish_call(value);}
#if FD_IPEVAL_ENABLED
    else {
      struct U8_OUTPUT *out = u8_current_output;
      struct CGICALL call={proc,cgidata,out,-1,VOID};
      fd_ipeval_call(cgiexecstep,(void *)&call);
      value = call.result;}
#endif
    return value;}
  else return fd_apply(proc,0,NULL);
}

/* Parsing query strings */

static lispval urldata_parse(lispval qstring)
{
  lispval smap = fd_empty_slotmap();
  parse_query_string((fd_slotmap)smap,CSTRING(qstring),STRLEN(qstring));
  return smap;
}

/* Bind request and output */

static lispval withreqout_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  U8_OUTPUT *oldout = u8_current_output;
  U8_OUTPUT _out, *out = &_out;
  lispval body = fd_get_body(expr,1);
  lispval reqinfo = fd_empty_slotmap();
  lispval result = VOID;
  lispval oldinfo = fd_push_reqinfo(reqinfo);
  U8_INIT_OUTPUT(&_out,1024);
  u8_set_default_output(out);
  {FD_DOLIST(ex,body) {
      if (FD_ABORTP(result)) {
        u8_free(_out.u8_outbuf);
        return result;}
      fd_decref(result);
      result = fd_eval(ex,env);}}
  u8_set_default_output(oldout);
  fd_output_xhtml_preface(oldout,reqinfo);
  u8_putn(oldout,_out.u8_outbuf,(_out.u8_write-_out.u8_outbuf));
  u8_printf(oldout,"\n</body>\n</html>\n");
  fd_decref(result);
  u8_free(_out.u8_outbuf);
  u8_flush(oldout);
  fd_use_reqinfo(oldinfo);
  return VOID;
}

/* URI mapping */

FD_EXPORT
lispval fd_mapurl(lispval uri)
{
  lispval mapfn = fd_req(mapurlfn_symbol);
  if (FD_APPLICABLEP(mapfn)) {
    lispval result = fd_apply(mapfn,1,&uri);
    fd_decref(mapfn);
    return result;}
  else {
    fd_decref(mapfn);
    return EMPTY;}
}

static lispval mapurl(lispval uri)
{
  lispval result = fd_mapurl(uri);
  if (FD_ABORTP(result)) return result;
  else if (STRINGP(result)) return result;
  else {
    fd_decref(result); return fd_incref(uri);}
}

/* Initialization */

static int cgiexec_initialized = 0;

FD_EXPORT int sendfile_set(lispval ignored,lispval v,void *vptr)
{
  u8_string *ptr = vptr;
  if (STRINGP(v)) {
    int bool = fd_boolstring(CSTRING(v),-1);
    if (bool<0) {
      if (*ptr) u8_free(*ptr);
      *ptr = u8_strdup(CSTRING(v));
      return 1;}
    else if (bool==0) {
      if (*ptr) u8_free(*ptr);
      *ptr = NULL;
      return 0;}
    else {
      if (*ptr) u8_free(*ptr);
      *ptr = u8_strdup("X-Sendfile");}
    return 1;}
  else if (FD_TRUEP(v)) {
    if (*ptr) u8_free(*ptr);
    *ptr = u8_strdup("X-Sendfile");
    return 1;}
  else if (FALSEP(v)) {
    if (*ptr) u8_free(*ptr);
    *ptr = NULL;
    return 0;}
  else return fd_reterr(fd_TypeError,"fd_sconfig_set",u8_strdup(_("string")),v);
}
FD_EXPORT int xredirect_set(lispval ignored,lispval v,void *vptr)
{
  u8_string *ptr = vptr;
  if (STRINGP(v)) {
    int bool = fd_boolstring(CSTRING(v),-1);
    if (bool<0) {
      if (*ptr) u8_free(*ptr);
      *ptr = u8_strdup(CSTRING(v));
      return 1;}
    else if (bool==0) {
      if (*ptr) u8_free(*ptr);
      *ptr = NULL;
      return 0;}
    else {
      if (*ptr) u8_free(*ptr);
      *ptr = u8_strdup("X-Redirect");}
    return 1;}
  else if (FD_TRUEP(v)) {
    if (*ptr) u8_free(*ptr);
    *ptr = u8_strdup("X-Sendfile");
    return 1;}
  else if (FALSEP(v)) {
    if (*ptr) u8_free(*ptr);
    *ptr = NULL;
    return 0;}
  else return fd_reterr(fd_TypeError,"fd_sconfig_set",u8_strdup(_("string")),v);
}

FD_EXPORT void fd_init_cgiexec_c()
{
  lispval module, xhtmlout_module;
  if (cgiexec_initialized) return;
  cgiexec_initialized = 1;
  fd_init_scheme();
  module = fd_new_cmodule("FDWEB",(0),fd_init_cgiexec_c);
  xhtmlout_module = fd_new_cmodule("XHTML",FD_MODULE_SAFE,fd_init_cgiexec_c);

  u8_init_mutex(&protected_cgi_lock);

  fd_def_evalfn(module,"HTTPHEADER","",httpheader);
  fd_idefn(module,fd_make_cprim1("HTTPHEADER!",addhttpheader,1));
  fd_idefn(module,fd_make_cprim6("SET-COOKIE!",setcookie,2));
  fd_idefn(module,fd_make_cprim4("CLEAR-COOKIE!",clearcookie,1));
  fd_idefn(module,fd_make_cprimn("BODY!",set_body_attribs,1));
  fd_idefn(module,fd_make_cprim1x("BODYCLASS!",add_body_class,1,
                                  fd_string_type,VOID));
  fd_idefn(module,fd_make_cprim1x("HTMLCLASS!",add_html_class,1,
                                  fd_string_type,VOID));

  fd_def_evalfn(module,"WITH/REQUEST/OUT","",withreqout_evalfn);
  fd_defalias(module,"WITHCGIOUT","WITH/REQUEST/OUT");

  fd_defalias2(module,"WITHCGI",fd_scheme_module,"WITH/REQUEST");
  fd_defalias2(module,"CGICALL",fd_scheme_module,"REQ/CALL");
  fd_defalias2(module,"CGIGET",fd_scheme_module,"REQ/GET");
  fd_defalias2(module,"CGIVAL",fd_scheme_module,"REQ/VAL");
  fd_defalias2(module,"CGITEST",fd_scheme_module,"REQ/TEST");
  fd_defalias2(module,"CGISET!",fd_scheme_module,"REQ/SET!");
  fd_defalias2(module,"CGIADD!",fd_scheme_module,"REQ/ADD!");
  fd_defalias2(module,"CGIADD!",fd_scheme_module,"REQ/DROP!");

  fd_idefn(module,fd_make_cprim1x("MAPURL",mapurl,1,fd_string_type,VOID));

  /* fd_def_evalfn(module,"CGIVAR","",cgivar_evalfn); */

  fd_idefn(module,fd_make_cprim1x
           ("URLDATA/PARSE",urldata_parse,1,fd_string_type,VOID));
  fd_defalias(module,"CGIPARSE","URLDATA/PARSE");

  fd_def_evalfn(xhtmlout_module,"HTMLHEADER","",htmlheader);
  fd_def_evalfn(xhtmlout_module,"TITLE!","",title_evalfn);
  fd_def_evalfn(xhtmlout_module,"JSOUT","",jsout_evalfn);
  fd_def_evalfn(xhtmlout_module,"CSSOUT","",cssout_evalfn);
  fd_idefn(xhtmlout_module,
           fd_make_cprim2x("STYLESHEET!",add_stylesheet,1,
                           fd_string_type,VOID,
                           fd_string_type,VOID));
  fd_idefn(xhtmlout_module,
           fd_make_cprim1x("JAVASCRIPT!",add_javascript,1,
                           fd_string_type,VOID));

  fd_idefn(xhtmlout_module,
           fd_make_cprim1x("JAVASCRIPT!",add_javascript,1,
                           fd_string_type,VOID));

  tail_symbol = fd_intern("%TAIL");
  browseinfo_symbol = fd_intern("BROWSEINFO");
  mapurlfn_symbol = fd_intern("MAPURLFN");

  accept_type = fd_intern("HTTP_ACCEPT");
  accept_language = fd_intern("HTTP_ACCEPT_LANGUAGE");
  accept_encoding = fd_intern("HTTP_ACCEPT_ENCODING");
  accept_charset = fd_intern("HTTP_ACCEPT_CHARSET");
  http_referrer = fd_intern("HTTP_REFERRER");

  http_cookie = fd_intern("HTTP_COOKIE");
  outcookies_symbol = fd_intern("_COOKIES%OUT");
  incookies_symbol = fd_intern("_COOKIES%IN");
  bad_cookie = fd_intern("_BADCOOKIES");
  cookiedata_symbol = fd_intern("_COOKIEDATA");

  server_port = fd_intern("SERVER_PORT");
  remote_port = fd_intern("REMOTE_PORT");
  request_method = fd_intern("REQUEST_METHOD");
  request_uri = fd_intern("REQUEST_URI");

  query_string = fd_intern("QUERY_STRING");
  query_elts = fd_intern("QELTS");
  query = fd_intern("QUERY");

  get_method = fd_intern("GET");
  post_method = fd_intern("POST");

  status_field = fd_intern("STATUS");
  redirect_field = fd_intern("_REDIRECT");
  sendfile_field = fd_intern("_SENDFILE");
  xredirect_field = fd_intern("_XREDIRECT");
  http_headers = fd_intern("HTTP-HEADERS");
  html_headers = fd_intern("HTML-HEADERS");

  content_type = fd_intern("CONTENT-TYPE");
  cgi_content_type = fd_intern("CONTENT_TYPE");
  incoming_content_type = fd_intern("INCOMING-CONTENT-TYPE");
  content_slotid = fd_intern("CONTENT");
  content_length = fd_intern("CONTENT-LENGTH");
  incoming_content_length = fd_intern("INCOMING-CONTENT-LENGTH");

  doctype_slotid = fd_intern("DOCTYPE");
  xmlpi_slotid = fd_intern("XMLPI");
  html_attribs_slotid = fd_intern("%HTML");
  body_attribs_slotid = fd_intern("%BODY");
  body_classes_slotid = fd_intern("%BODYCLASSES");
  html_classes_slotid = fd_intern("%HTMLCLASSES");
  class_symbol = fd_intern("CLASS");

  post_data_slotid = fd_intern("POST_DATA");
  multipart_form_data = lispval_string("multipart/form-data");
  www_form_urlencoded = lispval_string("application/x-www-form-urlencoded");

  filename_slotid = fd_intern("FILENAME");
  name_slotid = fd_intern("NAME");
  parts_slotid = fd_intern("PARTS");

  remote_user_symbol = fd_intern("REMOTE_USER");
  remote_host_symbol = fd_intern("REMOTE_HOST");
  remote_addr_symbol = fd_intern("REMOTE_ADDR");
  remote_ident_symbol = fd_intern("REMOTE_IDENT");
  remote_agent_symbol = fd_intern("HTTP_USER_AGENT");
  remote_info_symbol = fd_intern("REMOTE_INFO");

  params_symbol = fd_intern("_PARAMS");

  ipeval_symbol = fd_intern("_IPEVAL");

  protected_params[n_protected_params++] = fd_intern("STATUS");
  protected_params[n_protected_params++] = fd_intern("AUTHORIZATION");
  protected_params[n_protected_params++] = fd_intern("SCRIPT_FILENAME");
  protected_params[n_protected_params++] = fd_intern("REQUEST_METHOD");
  protected_params[n_protected_params++] = fd_intern("DOCUMENT_ROOT");

  fd_register_config
    ("CGIPREP",
     _("Functions to execute between parsing and responding to a CGI request"),
     fd_lconfig_get,fd_lconfig_set,&cgi_prepfns);
  fd_register_config
    ("XSENDFILE",
     _("Header for using the web server's X-SENDFILE functionality"),
     fd_sconfig_get,sendfile_set,&fd_sendfile_header);
  fd_register_config
    ("XREDIRECT",
     _("Header for using the web server's X-REDIRECT functionality"),
     fd_sconfig_get,xredirect_set,&fd_xredirect_header);
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
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
