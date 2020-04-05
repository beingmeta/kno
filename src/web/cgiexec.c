/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/tables.h"
#include "kno/eval.h"
#include "kno/storage.h"
#include "kno/ports.h"
#include "kno/webtools.h"
#include "kno/support.h"
#include "kno/cprims.h"


#include <libu8/libu8.h>
#include <libu8/libu8io.h>
#include <libu8/u8xfiles.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8streamio.h>

#define fast_eval(x,env) (kno_eval(x,env,kno_stackptr,0))

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

char *kno_sendfile_header = NULL; /* X-Sendfile */
char *kno_xredirect_header="X-Redirect";
static int log_cgidata = 0;

static u8_condition CGIDataInconsistency="Inconsistent CGI data";

/* Utility functions */

static lispval try_parse(u8_string buf)
{
  lispval val = kno_parse((buf[0]==':')?(buf+1):(buf));
  if (KNO_ABORTP(val)) {
    kno_decref(val); return kno_mkstring(buf);}
  else return val;
}

static lispval buf2lisp(char *buf,int isascii)
{
  if (buf[0]!=':')
    if (isascii) return kno_mkstring(buf);
    else return kno_wrapstring(u8_valid_copy(buf));
  else if (isascii) return try_parse(buf);
  else {
    u8_string s = u8_valid_copy(buf);
    lispval value = try_parse(s);
    u8_free(s);
    return value;}
}

static lispval buf2slotid(char *buf,int isascii)
{
  if (isascii) return kno_parse(buf);
  else {
    u8_string s = u8_valid_copy(buf);
    lispval value = kno_parse(s);
    u8_free(s);
    return value;}
}

static lispval buf2string(char *buf,int isascii)
{
  if (isascii) return kno_mkstring(buf);
  else return kno_wrapstring(u8_valid_copy(buf));
}

static void emit_uri_value(u8_output out,lispval val)
{
  if (STRINGP(val))
    kno_uri_output(out,CSTRING(val),STRLEN(val),0,NULL);
  else {
    struct U8_OUTPUT xout; u8_byte buf[256];
    U8_INIT_OUTPUT_X(&xout,256,buf,U8_FIXED_STREAM);
    kno_unparse(&xout,val);
    u8_putn(out,"%3a",3);
    kno_uri_output(out,xout.u8_outbuf,xout.u8_write-xout.u8_outbuf,0,NULL);
    if (xout.u8_outbuf!=buf) u8_free(xout.u8_outbuf);}
}

KNO_EXPORT
void kno_urify(u8_output out,lispval val)
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
    kno_incref(param);
    KNO_ADD_TO_CHOICE(result,param);}
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
    kno_seterr("TooManyProtectedParams","protected_cgi_set",NULL,var);
    u8_unlock_mutex(&protected_cgi_lock);
    return -1;}
  else {
    protected_params[n_protected_params++] = field;
    kno_incref(field);
    u8_unlock_mutex(&protected_cgi_lock);
    return 1;}
}


static void convert_parse(kno_slotmap c,lispval slotid)
{
  lispval value = kno_slotmap_get(c,slotid,VOID);
  if (!(STRINGP(value))) {kno_decref(value);}
  else {
    kno_slotmap_store(c,slotid,try_parse(CSTRING(value)));
    kno_decref(value);}
}

static void convert_accept(kno_slotmap c,lispval slotid)
{
  lispval value = kno_slotmap_get(c,slotid,VOID);
  if (!(STRINGP(value))) {kno_decref(value);}
  else {
    lispval newvalue = EMPTY, entry;
    const u8_byte *data = CSTRING(value), *scan = data;
    const u8_byte *comma = strchr(scan,','), *semi = strstr(scan,";q=");
    while (comma) {
      if ((semi) && (semi<comma))
        entry = kno_conspair(kno_substring(scan,semi),kno_substring(semi+3,comma));
      else entry = kno_substring(scan,comma);
      CHOICE_ADD(newvalue,entry);
      scan = comma+1;
      comma = strchr(scan,',');
      semi = strstr(scan,";q=");
      if (semi)
        entry = kno_conspair(kno_substring(scan,semi),kno_substring(semi+3,NULL));
      else entry = kno_substring(scan,NULL);
      CHOICE_ADD(newvalue,entry);}
    kno_slotmap_store(c,slotid,newvalue);
    kno_decref(value);
    kno_decref(newvalue);}
}

/* Converting query arguments */

static lispval post_data_slotid, multipart_form_data, www_form_urlencoded;

/* Setting this to zero might be slightly more secure, and it's the
   CONFIG option QUERYWITHPOST (boolean) */
static int parse_query_on_post = 1;

static void parse_query_string(kno_slotmap c,const char *data,int len);

static lispval intern_compound(u8_string s1,u8_string s2)
{
  lispval result = VOID;
  struct U8_OUTPUT tmpout; u8_byte tmpbuf[128];
  U8_INIT_OUTPUT_X(&tmpout,128,tmpbuf,0);
  u8_puts(&tmpout,s1); u8_puts(&tmpout,s2);
  result = kno_parse(tmpout.u8_outbuf);
  u8_close_output(&tmpout);
  return result;
}

static void get_form_args(kno_slotmap c)
{
  if ( (kno_test((lispval)c,request_method,post_method)) ||
       (kno_test((lispval)c,cgi_content_type,multipart_form_data)) ||
       (kno_test((lispval)c,cgi_content_type,www_form_urlencoded)) ) {
    kno_handle_compound_mime_field((lispval)c,cgi_content_type,VOID);
    if (parse_query_on_post) {
      /* We do this first, so it won't override any post data
         which is supposedly more legitimate. */
      lispval qval = kno_slotmap_get(c,query_string,VOID);
      if (STRINGP(qval))
        parse_query_string(c,KNO_STRING_DATA(qval),KNO_STRING_LENGTH(qval));
      kno_decref(qval);}
    if (kno_test((lispval)c,cgi_content_type,multipart_form_data)) {
      lispval postdata = kno_slotmap_get(c,post_data_slotid,VOID);
      lispval parts = NIL;
      /* Parse the MIME data into parts */
      if (STRINGP(postdata))
        parts = kno_parse_multipart_mime
          ((lispval)c,KNO_STRING_DATA(postdata),
           KNO_STRING_DATA(postdata)+KNO_STRING_LENGTH(postdata));
      else if (PACKETP(postdata))
        parts = kno_parse_multipart_mime
          ((lispval)c,KNO_PACKET_DATA(postdata),
           KNO_PACKET_DATA(postdata)+KNO_PACKET_LENGTH(postdata));
      kno_add((lispval)c,parts_slotid,parts);
      kno_decref(postdata);
      /* Convert the parts */
      {KNO_DOLIST(elt,parts) {
          lispval namestring = kno_get(elt,name_slotid,VOID);
          if (STRINGP(namestring)) {
            u8_string nstring = KNO_STRING_DATA(namestring);
            lispval namesym = kno_parse(nstring);
            lispval partsym = intern_compound("__",nstring);
            lispval ctype = kno_get(elt,content_type,VOID);
            lispval content = kno_get((lispval)elt,content_slotid,EMPTY);
            /* Add the part itself in the _name slotid */
            kno_add((lispval)c,partsym,elt);
            /* Add the filename slot if it's an upload */
            if (kno_test(elt,filename_slotid,VOID)) {
              lispval filename = kno_get(elt,filename_slotid,EMPTY);
              kno_add((lispval)c,intern_compound(nstring,"_filename"),
                      filename);
              kno_decref(filename);}
            if (kno_test(elt,content_type,VOID)) {
              lispval ctype = kno_get(elt,content_type,EMPTY);
              kno_add((lispval)c,intern_compound(nstring,"_type"),ctype);
              kno_decref(ctype);}
            if ((VOIDP(ctype)) || (kno_overlapp(ctype,KNOSYM_TEXT))) {
              if (STRINGP(content)) {
                u8_string chars = CSTRING(content); int len = STRLEN(content);
                /* Remove trailing \r\n from the MIME field */
                if ((len>1) && (chars[len-1]=='\n')) {
                  lispval new_content;
                  if (chars[len-2]=='\r')
                    new_content = kno_substring(chars,chars+len-2);
                  else new_content = kno_substring(chars,chars+len-1);
                  kno_add((lispval)c,namesym,new_content);
                  kno_decref(new_content);}
                else kno_add((lispval)c,namesym,content);}
              else kno_add((lispval)c,namesym,content);}
            else kno_add((lispval)c,namesym,content);
            kno_decref(content); kno_decref(ctype);}
          kno_decref(namestring);}}
      kno_decref(parts);}
    else if (kno_test((lispval)c,cgi_content_type,www_form_urlencoded)) {
      lispval qval = kno_slotmap_get(c,post_data_slotid,VOID);
      u8_string data; unsigned int len;
      if (STRINGP(qval)) {
        data = CSTRING(qval); len = KNO_STRING_LENGTH(qval);}
      else if (PACKETP(qval)) {
        data = KNO_PACKET_DATA(qval); len = KNO_PACKET_LENGTH(qval);}
      else {
        kno_decref(qval); data = NULL;}
      if ((data) && (strchr(data,'=')))
        parse_query_string(c,data,len);
      kno_decref(qval);}
    else {}}
  else {
    lispval qval = kno_slotmap_get(c,query_string,VOID);
    if (STRINGP(qval))
      parse_query_string(c,KNO_STRING_DATA(qval),KNO_STRING_LENGTH(qval));
    kno_decref(qval);
    return;}
}

static void add_param(kno_slotmap req,kno_slotmap params,
                      lispval slotstring,lispval slotid,
                      lispval val)
{
  if (KNO_VOIDP(slotstring))
    kno_slotmap_add(req,query,val);
  else {
    kno_slotmap_add(params,slotstring,val);
    if (!(KNO_VOIDP(slotid))) {
      kno_slotmap_add(params,slotid,val);
      kno_slotmap_add(req,slotid,val);}}
}

static void parse_query_string(kno_slotmap c,const char *data,int len)
{
  int isascii = 1;
  lispval slotid = VOID, slotstring = VOID, value = VOID;
  lispval params_val = kno_slotmap_get(c,params_symbol,KNO_VOID);
  kno_slotmap q = NULL;
  if (KNO_SLOTMAPP(params_val))
    q = (kno_slotmap) params_val;
  else {
    params_val = kno_init_slotmap(NULL,32,NULL);
    q = (kno_slotmap) params_val;}
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
      kno_decref(slotstring); slotstring = VOID;
      kno_decref(value); value = VOID;
      slotstring = buf2string(buf,isascii);
      if (isprotected(slotid)) slotid = VOID;
      write = buf;
      isascii = 1;
      scan++;}
    else if ( *scan == '&' ) {
      *write++='\0';
      value = buf2string(buf,isascii);
      add_param(c,q,slotstring,slotid,value);
      kno_decref(value); value = VOID; slotid = VOID;
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
    kno_decref(value); value = VOID; slotid = VOID;
    write = buf;
    isascii = 1;
    scan++;}
  else if (!(VOIDP(slotid))) {
    lispval str = kno_mkstring("");
    kno_slotmap_add(c,slotid,str);
    kno_decref(str);}
  if (q->n_slots) {
    kno_slotmap_add(c,params_symbol,params_val);}
  else kno_decref(params_val);
  u8_free(buf);
}

/* Converting cookie args */

static void setcookiedata(lispval cookiedata,lispval cgidata)
{
  /* This replaces any existing cookie data which matches cookiedata */
  lispval cookies=
    ((TABLEP(cgidata))?
     (kno_get(cgidata,outcookies_symbol,EMPTY)):
     (kno_req_get(outcookies_symbol,EMPTY)));
  lispval cookievar = VEC_REF(cookiedata,0);
  DO_CHOICES(cookie,cookies) {
    if (KNO_EQ(VEC_REF(cookie,0),cookievar)) {
      if (TABLEP(cgidata))
        kno_drop(cgidata,outcookies_symbol,cookie);
      else kno_req_drop(outcookies_symbol,cookie);}}
  kno_decref(cookies);
  if ((cgidata)&&(TABLEP(cgidata)))
    kno_add(cgidata,outcookies_symbol,cookiedata);
  else kno_req_add(outcookies_symbol,cookiedata);
}

static void convert_cookie_arg(kno_slotmap c)
{
  lispval qval = kno_slotmap_get(c,http_cookie,VOID);
  if (!(STRINGP(qval))) {kno_decref(qval); return;}
  else {
    lispval slotid = VOID, name = VOID, value = VOID;
    int len = STRLEN(qval); int isascii = 1, badcookie = 0;
    const u8_byte *scan = CSTRING(qval), *end = scan+len;
    char *buf = u8_malloc(len), *write = buf;
    while (scan<end)
      if ((VOIDP(slotid)) && (*scan=='=')) {
        /* There's an assignment going on. */
        *write++='\0';
        name = kno_mkstring(buf);
        if ((buf[0]=='_')||(strncmp(buf,"HTTP",4)==0)) badcookie = 1;
        else badcookie = 0;
        if (isascii) slotid = kno_parse(buf);
        else {
          u8_string s = u8_valid_copy(buf);
          slotid = kno_parse(s);
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
          lispval cookiedata = kno_make_nvector(2,name,kno_incref(value));
          if (badcookie)
            kno_slotmap_add(c,bad_cookie,cookiedata);
          else kno_slotmap_add(c,slotid,value);
          kno_slotmap_add(c,cookiedata_symbol,cookiedata);
          setcookiedata(cookiedata,(lispval)c);
          name = VOID;
          kno_decref(cookiedata);}
        kno_decref(value); value = VOID; slotid = VOID;
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
        lispval cookiedata = kno_make_nvector(2,name,kno_incref(value));
        if (badcookie)
          kno_slotmap_add(c,bad_cookie,cookiedata);
        else kno_slotmap_add(c,slotid,value);
        kno_slotmap_add(c,cookiedata_symbol,cookiedata);
        setcookiedata(cookiedata,(lispval)c);
        kno_decref(cookiedata);}
      kno_decref(value); value = VOID; slotid = VOID;
      write = buf; isascii = 1; scan++;}
    kno_slotmap_add(c,incookies_symbol,qval);
    kno_slotmap_store(c,outcookies_symbol,EMPTY);
    kno_decref(qval);
    u8_free(buf);}
}

/* Parsing CGI data */

static lispval cgi_prepfns = EMPTY;
static void add_remote_info(lispval cgidata);

KNO_EXPORT int kno_parse_cgidata(lispval data)
{
  struct KNO_SLOTMAP *cgidata = KNO_XSLOTMAP(data);
  lispval ctype = kno_get(data,content_type,VOID);
  lispval clen = kno_get(data,content_length,VOID);
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
    kno_slotmap_store(cgidata,incoming_content_type,ctype);
    kno_slotmap_drop(cgidata,content_type,VOID);
    kno_decref(ctype);}
  if (!(VOIDP(clen))) {
    kno_slotmap_store(cgidata,incoming_content_length,clen);
    kno_slotmap_drop(cgidata,content_length,VOID);
    kno_decref(clen);}
  {DO_CHOICES(handler,cgi_prepfns) {
      if (KNO_APPLICABLEP(handler)) {
        lispval value = kno_apply(handler,1,&data);
        kno_decref(value);}
      else u8_log(LOG_WARN,"Not Applicable","Invalid CGI prep handler %q",handler);}}
  if (!(kno_slotmap_test(cgidata,KNOSYM_PCTID,KNO_VOID))) {
    lispval req_uri = kno_slotmap_get(cgidata,request_uri,KNO_VOID);
    if (KNO_STRINGP(req_uri)) kno_slotmap_store(cgidata,KNOSYM_PCTID,req_uri);
    kno_decref(req_uri);}
  return 1;
}

static void add_remote_info(lispval cgidata)
{
  /* This combines a bunch of different request properties into a single string */
  lispval remote_user = kno_get(cgidata,remote_user_symbol,VOID);
  lispval remote_ident = kno_get(cgidata,remote_ident_symbol,VOID);
  lispval remote_host = kno_get(cgidata,remote_host_symbol,VOID);
  lispval remote_addr = kno_get(cgidata,remote_addr_symbol,VOID);
  lispval remote_agent = kno_get(cgidata,remote_agent_symbol,VOID);
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
    kno_init_string(NULL,remote.u8_write-remote.u8_outbuf,
                    remote.u8_outbuf);
  kno_store(cgidata,remote_info_symbol,remote_string);
  kno_decref(remote_user); kno_decref(remote_ident); kno_decref(remote_host);
  kno_decref(remote_addr); kno_decref(remote_agent);
  kno_decref(remote_string);
}

/* Generating headers */

static lispval do_xmlout(U8_OUTPUT *out,lispval body,
                         kno_lexenv env,kno_stack _stack)
{
  U8_OUTPUT *prev = u8_current_output;
  u8_set_default_output(out);
  while (PAIRP(body)) {
    lispval value = fast_eval(KNO_CAR(body),env);
    body = KNO_CDR(body);
    if (KNO_ABORTP(value)) {
      u8_set_default_output(prev);
      return value;}
    else if (VOIDP(value)) continue;
    else if (STRINGP(value))
      u8_printf(out,"%s",CSTRING(value));
    else u8_printf(out,"%q",value);
    kno_decref(value);}
  u8_set_default_output(prev);
  return VOID;
}

static lispval httpheader(lispval expr,kno_lexenv env,kno_stack _stack)
{
  U8_OUTPUT out; lispval result;
  U8_INIT_OUTPUT(&out,64);
  result = do_xmlout(&out,kno_get_body(expr,1),env,_stack);
  if (KNO_ABORTP(result)) {
    u8_free(out.u8_outbuf);
    return result;}
  else {
    lispval header=
      kno_init_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);
    kno_req_add(http_headers,header);
    kno_decref(header);
    return VOID;}
}

DEFPRIM1("httpheader!",addhttpheader,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
         "`(HTTPHEADER! *arg0*)` **undocumented**",
         kno_any_type,KNO_VOID);
static lispval addhttpheader(lispval header)
{
  kno_req_add(http_headers,header);
  return VOID;
}

static lispval htmlheader(lispval expr,kno_lexenv env,kno_stack _stack)
{
  U8_OUTPUT out; lispval header_string; lispval result;
  U8_INIT_OUTPUT(&out,64);
  result = do_xmlout(&out,kno_get_body(expr,1),env,_stack);
  if (KNO_ABORTP(result)) {
    u8_free(out.u8_outbuf);
    return result;}
  header_string = kno_init_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);
  kno_req_push(html_headers,header_string);
  kno_decref(header_string);
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
    real_val = kno_get(cgidata,VEC_REF(cookie,0),VOID);
    if (VOIDP(real_val)) real_val = kno_incref(VEC_REF(cookie,1));}
  else real_val = kno_incref(VEC_REF(cookie,1));
  if ((len>2) || (!(KNO_EQ(real_val,VEC_REF(cookie,1))))) {
    lispval var = VEC_REF(cookie,0);
    u8_string namestring; const u8_byte *scan;
    int free_namestring = 0, c;
    u8_printf(out,"Set-Cookie: ");
    if (SYMBOLP(var)) namestring = SYM_NAME(var);
    else if (STRINGP(var)) namestring = CSTRING(var);
    else {
      namestring = kno_lisp2string(var);
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
      else if (TYPEP(expires,kno_timestamp_type)) {
        struct KNO_TIMESTAMP *tstamp = (kno_timestamp)expires;
        char buf[512]; struct tm tptr;
        u8_xtime_to_tptr(&(tstamp->u8xtimeval),&tptr);
        strftime(buf,512,"%A, %d-%b-%Y %T GMT",&tptr);
        u8_printf(out,"; Expires=%s",buf);}
      if (!((VOIDP(secure))||(FALSEP(secure))))
        u8_printf(out,"; Secure");}
    u8_printf(out,"\r\n");}
  kno_decref(real_val);
  return 1;
}

DEFPRIM6("set-cookie!",setcookie,KNO_MAX_ARGS(6)|KNO_MIN_ARGS(2),
         "`(SET-COOKIE! *arg0* *arg1* [*arg2*] [*arg3*] [*arg4*] [*arg5*])` **undocumented**",
         kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
         kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
         kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval setcookie
(lispval var,lispval val,
 lispval domain,lispval path,
 lispval expires,lispval secure)
{
  if (!((SYMBOLP(var)) || (STRINGP(var)) || (OIDP(var))))
    return kno_type_error("symbol","setcookie",var);
  else {
    lispval cookiedata;
    if (VOIDP(domain)) domain = KNO_FALSE;
    if (VOIDP(path)) path = KNO_FALSE;
    if (VOIDP(expires)) expires = KNO_FALSE;
    if ((FALSEP(expires))||
        (STRINGP(expires))||
        (TYPEP(expires,kno_timestamp_type)))
      kno_incref(expires);
    else if ((FIXNUMP(expires))||(KNO_BIGINTP(expires))) {
      long long ival = ((FIXNUMP(expires))?(FIX2INT(expires)):
                        (kno_bigint_to_long_long((kno_bigint)expires)));
      time_t expval;
      if ((ival<0)||(ival<(24*3600*365*20))) expval = (time(NULL))+ival;
      else expval = (time_t)ival;
      expires = kno_time2timestamp(expval);}
    else return kno_type_error("timestamp","setcookie",expires);
    cookiedata=
      kno_make_nvector(6,kno_incref(var),kno_incref(val),
                       kno_incref(domain),kno_incref(path),
                       expires,kno_incref(secure));
    setcookiedata(cookiedata,VOID);
    kno_decref(cookiedata);
    return VOID;}
}

DEFPRIM4("clear-cookie!",clearcookie,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(1),
         "`(CLEAR-COOKIE! *arg0* [*arg1*] [*arg2*] [*arg3*])` **undocumented**",
         kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
         kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval clearcookie
(lispval var,lispval domain,lispval path,lispval secure)
{
  if (!((SYMBOLP(var)) || (STRINGP(var)) || (OIDP(var))))
    return kno_type_error("symbol","setcookie",var);
  else {
    lispval cookiedata;
    lispval val = kno_mkstring("");
    time_t past = time(NULL)-(3600*24*7*50);
    lispval expires = kno_time2timestamp(past);
    if (VOIDP(domain)) domain = KNO_FALSE;
    if (VOIDP(path)) path = KNO_FALSE;
    cookiedata=
      kno_make_nvector(6,kno_incref(var),val,
                       kno_incref(domain),kno_incref(path),
                       expires,kno_incref(secure));
    setcookiedata(cookiedata,VOID);
    kno_decref(cookiedata);
    return VOID;}
}

/* HTML Header functions */

DEFPRIM2("stylesheet!",add_stylesheet,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
         "`(STYLESHEET! *arg0* [*arg1*])` **undocumented**",
         kno_string_type,KNO_VOID,kno_string_type,KNO_VOID);
static lispval add_stylesheet(lispval stylesheet,lispval type)
{
  lispval header_string = VOID;
  U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
  if (VOIDP(type))
    u8_printf(&out,"<link rel='stylesheet' type='text/css' href='%s'/>\n",
              CSTRING(stylesheet));
  else u8_printf(&out,"<link rel='stylesheet' type='%s' href='%s'/>\n",
                 CSTRING(type),CSTRING(stylesheet));
  header_string = kno_init_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);
  kno_req_push(html_headers,header_string);
  kno_decref(header_string);
  return VOID;
}

DEFPRIM1("javascript!",add_javascript,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
         "`(JAVASCRIPT! *arg0*)` **undocumented**",
         kno_string_type,KNO_VOID);
static lispval add_javascript(lispval url)
{
  lispval header_string = VOID;
  U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
  u8_printf(&out,"<script language='javascript' src='%s'>\n",
            CSTRING(url));
  u8_printf(&out,"  <!-- empty content for some browsers -->\n");
  u8_printf(&out,"</script>\n");
  header_string = kno_init_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);
  kno_req_push(html_headers,header_string);
  kno_decref(header_string);
  return VOID;
}

static lispval title_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  U8_OUTPUT out; lispval result;
  U8_INIT_OUTPUT(&out,64);
  u8_puts(&out,"<title>");
  result = do_xmlout(&out,kno_get_body(expr,1),env,_stack);
  u8_puts(&out,"</title>\n");
  if (KNO_ABORTP(result)) {
    u8_free(out.u8_outbuf);
    return result;}
  else {
    lispval header_string=
      kno_init_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);
    kno_req_push(html_headers,header_string);
    kno_decref(header_string);
    return VOID;}
}

static lispval jsout_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  U8_OUTPUT *prev = u8_current_output;
  U8_OUTPUT _out, *out = &_out; lispval result = VOID;
  U8_INIT_OUTPUT(&_out,2048);
  u8_set_default_output(out);
  u8_puts(&_out,"<script language='javascript'>\n");
  {lispval body = kno_get_body(expr,1);
    KNO_DOLIST(x,body) {
      if (STRINGP(x))
        u8_puts(&_out,CSTRING(x));
      else if ((SYMBOLP(x))||(PAIRP(x))) {
	result = kno_eval(x,env,_stack,0);
        if (KNO_ABORTP(result)) break;
        else if ((VOIDP(result))||(FALSEP(result))||
                 (EMPTYP(result))) {}
        else if (STRINGP(result))
          u8_puts(&_out,CSTRING(result));
        else kno_unparse(&_out,result);
        kno_decref(result); result = VOID;}
      else kno_unparse(&_out,x);}}
  u8_puts(&_out,"\n</script>\n");
  if (KNO_ABORTP(result)) {
    u8_free(_out.u8_outbuf);
    u8_set_default_output(prev);
    return result;}
  else {
    lispval header_string=
      kno_init_string(NULL,_out.u8_write-_out.u8_outbuf,_out.u8_outbuf);
    u8_set_default_output(prev);
    kno_req_push(html_headers,header_string);
    kno_decref(header_string);
    return VOID;}
}

static lispval cssout_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  U8_OUTPUT *prev = u8_current_output;
  U8_OUTPUT _out, *out = &_out;
  lispval result = VOID, body = kno_get_body(expr,1);
  U8_INIT_OUTPUT(&_out,2048);
  u8_set_default_output(out);
  u8_puts(&_out,"<style type='text/css'>\n");
  {KNO_DOLIST(x,body) {
      if (STRINGP(x))
        u8_puts(&_out,CSTRING(x));
      else if ((SYMBOLP(x))||(PAIRP(x))) {
	result = kno_eval(x,env,_stack,0);
        if (KNO_ABORTP(result)) break;
        else if ((VOIDP(result))||(FALSEP(result))||
                 (EMPTYP(result))) {}
        else if (STRINGP(result))
          u8_puts(&_out,CSTRING(result));
        else kno_unparse(&_out,result);
        kno_decref(result); result = VOID;}
      else kno_unparse(&_out,x);}}
  u8_puts(&_out,"\n</style>\n");
  if (KNO_ABORTP(result)) {
    u8_free(_out.u8_outbuf);
    u8_set_default_output(prev);
    return result;}
  else {
    lispval header_string=
      kno_init_string(NULL,_out.u8_write-_out.u8_outbuf,_out.u8_outbuf);
    u8_set_default_output(prev);
    kno_req_push(html_headers,header_string);
    kno_decref(header_string);
    return VOID;}
}

/* Generating the HTTP header */
KNO_EXPORT int kno_output_http_headers(U8_OUTPUT *out,lispval cgidata)
{
  lispval ctype = kno_get(cgidata,content_type,VOID);
  lispval status = kno_get(cgidata,status_field,VOID);
  lispval headers = kno_get(cgidata,http_headers,EMPTY);
  lispval redirect = kno_get(cgidata,redirect_field,VOID);
  lispval sendfile = kno_get(cgidata,sendfile_field,VOID);
  lispval xredirect = kno_get(cgidata,xredirect_field,VOID);
  lispval cookies = kno_get(cgidata,outcookies_symbol,EMPTY);
  int keep_doctype = 0, http_status = -1;
  if ((STRINGP(redirect))&&(VOIDP(status))) {
    status = KNO_INT(303); http_status = 303;}
  if (KNO_UINTP(status)) {
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
        u8_putn(out,KNO_PACKET_DATA(header),KNO_PACKET_LENGTH(header));
        u8_putn(out,"\r\n",2);}}
  {DO_CHOICES(cookie,cookies)
      if (handle_cookie(out,cgidata,cookie)<0)
        u8_log(LOG_WARN,CGIDataInconsistency,"Bad cookie data: %q",cookie);}
  if (STRINGP(redirect))
    u8_printf(out,"Location: %s\r\n",CSTRING(redirect));
  else if ((STRINGP(sendfile))&&(kno_sendfile_header)) {
    u8_log(LOG_DEBUG,"Sendfile","Using %s to pass %s",
           kno_sendfile_header,CSTRING(sendfile));
    u8_printf(out,"Content-length: 0\r\n");
    u8_printf(out,"%s: %s\r\n",kno_sendfile_header,CSTRING(sendfile));}
  else if ((STRINGP(xredirect))&&(kno_xredirect_header)) {
    u8_log(LOG_DEBUG,"Xredirect","Using %s to pass %s",
           kno_xredirect_header,CSTRING(xredirect));
    u8_printf(out,"%s: %s\r\n",kno_xredirect_header,CSTRING(xredirect));}
  else keep_doctype = 1;
  if (!(keep_doctype)) kno_store(cgidata,doctype_slotid,KNO_FALSE);
  kno_decref(ctype); kno_decref(status); kno_decref(headers);
  kno_decref(redirect); kno_decref(sendfile); kno_decref(cookies);
  return http_status;
}

static void output_headers(U8_OUTPUT *out,lispval headers)
{
  if (PAIRP(headers)) {
    lispval header = KNO_CAR(headers);
    output_headers(out,KNO_CDR(headers));
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
      lispval car = KNO_CAR(scan);
      if ((car == class_symbol)||
          ((STRINGP(car))&&(strcasecmp(CSTRING(car),"class")==0))) {
        class_cons = KNO_CDR(scan); break;}
      else {
        scan = KNO_CDR(scan);
        if (PAIRP(scan)) scan = KNO_CDR(scan);}}
    if (VOIDP(class_cons)) {
      class_cons = kno_conspair(KNO_FALSE,attribs);
      attribs = kno_conspair(class_symbol,class_cons);}
    U8_INIT_STATIC_OUTPUT_BUF(classout,sizeof(_classbuf),_classbuf);
    if (STRINGP(KNO_CAR(class_cons))) {
      u8_puts(&classout,CSTRING(KNO_CAR(class_cons)));
      u8_puts(&classout," ");}
    while (PAIRP(classes)) {
      lispval car = KNO_CAR(classes);
      if (!(STRINGP(car))) {}
      else {
        if (i) u8_puts(&classout," "); i++;
        u8_puts(&classout,CSTRING(car));}
      classes = KNO_CDR(classes);}
    {lispval old = KNO_CAR(class_cons);
      KNO_RPLACA(class_cons,kno_make_string
                 (NULL,classout.u8_write-classout.u8_outbuf,
                  classout.u8_outbuf));
      kno_decref(old);
      u8_close((u8_stream)&classout);}
    return attribs;}
  else {
    struct U8_OUTPUT classout;
    u8_byte _classbuf[256]; int i = 0;
    U8_INIT_STATIC_OUTPUT_BUF(classout,sizeof(_classbuf),_classbuf);
    {KNO_DOLIST(class,classes) {
        if (STRINGP(class)) {
          if (i>0) u8_putc(&classout,' ');
          i++;
          u8_puts(&classout,CSTRING(class));}}}
    return kno_conspair(class_symbol,
                        kno_conspair(kno_stream_string(&classout),attribs));}
}

KNO_EXPORT
int kno_output_xhtml_preface(U8_OUTPUT *out,lispval cgidata)
{
  lispval doctype = kno_get(cgidata,doctype_slotid,VOID);
  lispval xmlpi = kno_get(cgidata,xmlpi_slotid,VOID);
  lispval html_attribs = kno_get(cgidata,html_attribs_slotid,VOID);
  lispval html_classes = kno_get(cgidata,html_classes_slotid,VOID);
  lispval body_attribs = kno_get(cgidata,body_attribs_slotid,VOID);
  lispval body_classes = kno_get(cgidata,body_classes_slotid,VOID);
  lispval headers = kno_get(cgidata,html_headers,VOID);
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
    kno_open_markup(out,"html",html_attribs,0);
    u8_puts(out,"\n<head>\n");}
  if (!(VOIDP(headers))) {
    output_headers(out,headers);
    kno_decref(headers);}
  u8_puts(out,"</head>\n");
  if (PAIRP(body_classes))
    body_attribs = attrib_merge_classes(body_attribs,body_classes);
  if (FALSEP(body_attribs)) {}
  else if (VOIDP(body_attribs)) u8_puts(out,"<body>");
  else {
    kno_open_markup(out,"body",body_attribs,0);}
  kno_decref(doctype); kno_decref(xmlpi);
  kno_decref(html_attribs);
  kno_decref(body_attribs);
  kno_decref(body_classes);
  return 1;
}

KNO_EXPORT
int kno_output_xml_preface(U8_OUTPUT *out,lispval cgidata)
{
  lispval doctype = kno_get(cgidata,doctype_slotid,VOID);
  lispval xmlpi = kno_get(cgidata,xmlpi_slotid,VOID);
  if (STRINGP(xmlpi))
    u8_putn(out,CSTRING(xmlpi),STRLEN(xmlpi));
  else u8_puts(out,DEFAULT_XMLPI);
  u8_putc(out,'\n');
  if (VOIDP(doctype)) u8_puts(out,DEFAULT_DOCTYPE);
  else if (STRINGP(doctype))
    u8_putn(out,CSTRING(doctype),STRLEN(doctype));
  else return 0;
  u8_putc(out,'\n');
  kno_decref(doctype); kno_decref(xmlpi);
  return 1;
}

DEFPRIM("body!",set_body_attribs,KNO_VAR_ARGS|KNO_MIN_ARGS(1),
        "`(BODY! *arg0* *args...*)` **undocumented**");
static lispval set_body_attribs(int n,kno_argvec args)
{
  if ((n==1)&&(args[0]==KNO_FALSE)) {
    kno_req_store(body_attribs_slotid,KNO_FALSE);
    return VOID;}
  else {
    int i = n-1; while (i>=0) {
      lispval arg = args[i--];
      kno_req_push(body_attribs_slotid,arg);}
    return VOID;}
}

DEFPRIM1("bodyclass!",add_body_class,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
         "`(BODYCLASS! *arg0*)` **undocumented**",
         kno_string_type,KNO_VOID);
static lispval add_body_class(lispval classname)
{
  kno_req_push(body_classes_slotid,classname);
  return VOID;
}

DEFPRIM1("htmlclass!",add_html_class,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
         "`(HTMLCLASS! *arg0*)` **undocumented**",
         kno_string_type,KNO_VOID);
static lispval add_html_class(lispval classname)
{
  kno_req_push(html_classes_slotid,classname);
  return VOID;
}

/* CGI Exec */

static lispval tail_symbol;

static lispval cgigetvar(lispval cgidata,lispval var)
{
  int noparse=
    ((SYMBOLP(var))&&((SYM_NAME(var))[0]=='%'));
  lispval name = ((noparse)?(kno_intern(SYM_NAME(var)+1)):(var));
  lispval val = ((TABLEP(cgidata))?(kno_get(cgidata,name,VOID)):
                 (kno_req_get(name,VOID)));
  if (VOIDP(val)) return val;
  else if ((noparse)&&(STRINGP(val))) return val;
  else if (STRINGP(val)) {
    u8_string data = CSTRING(val);
    if (*data=='\0') return val;
    else if (strchr("@{#(",data[0])) {
      lispval parsed = kno_parse_arg(data);
      if (KNO_ABORTP(parsed)) {
        kno_decref(parsed); kno_clear_errors(0);
        return val;}
      else {
        kno_decref(val); return parsed;}}
    else if (isdigit(data[0])) {
      lispval parsed = kno_parse_arg(data);
      if (KNO_ABORTP(parsed)) {
        kno_decref(parsed); kno_clear_errors(0);
        return val;}
      else if (NUMBERP(parsed)) {
        kno_decref(val); return parsed;}
      else {
        kno_decref(parsed); return val;}}
    else if (*data == ':')
      if (data[1]=='\0')
        return kno_mkstring(data);
      else {
        lispval arg = kno_parse(data+1);
        if (KNO_ABORTP(arg)) {
          u8_log(LOG_WARN,kno_ParseArgError,"Bad colon spec arg '%s'",arg);
          kno_clear_errors(1);
          return kno_mkstring(data);}
        else return arg;}
    else if (*data == '\\') {
      lispval shorter = kno_mkstring(data+1);
      kno_decref(val);
      return shorter;}
    else return val;}
  else if ((CHOICEP(val))||(PRECHOICEP(val))) {
    lispval result = EMPTY;
    DO_CHOICES(v,val) {
      if (!(STRINGP(v))) {
        kno_incref(v); CHOICE_ADD(result,v);}
      else {
        u8_string data = CSTRING(v); lispval parsed = v;
        if (*data=='\\') parsed = kno_mkstring(data+1);
        else if ((*data==':')&&(data[1]=='\0')) {kno_incref(parsed);}
        else if (*data==':')
          parsed = kno_parse(data+1);
        else if ((isdigit(*data))||(*data=='+')||(*data=='-')||(*data=='.')) {
          parsed = kno_parse_arg(data);
          if (!(NUMBERP(parsed))) {
            kno_decref(parsed); parsed = v; kno_incref(parsed);}}
        else if (strchr("@{#(",data[0]))
          parsed = kno_parse_arg(data);
        else kno_incref(parsed);
        if (KNO_ABORTP(parsed)) {
          u8_log(LOG_WARN,kno_ParseArgError,"Bad LISP arg '%s'",data);
          kno_clear_errors(1);
          parsed = v; kno_incref(v);}
        CHOICE_ADD(result,parsed);}}
    kno_decref(val);
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
  value = kno_xapply_lambda((kno_lambda)proc,(void *)cgidata,
                            (lispval (*)(void *,lispval))cgigetvar);
  call->result = value;
  return 1;
}

#if KNO_IPEVAL_ENABLED
static int use_ipeval(lispval proc,lispval cgidata)
{
  int retval = -1;
  struct KNO_LAMBDA *sp = (struct KNO_LAMBDA *)proc;
  lispval val = kno_symeval(ipeval_symbol,sp->env);
  if ((VOIDP(val))||(val == KNO_UNBOUND)) retval = 0;
  else {
    lispval cgival = kno_get(cgidata,ipeval_symbol,VOID);
    if (VOIDP(cgival)) {
      if (FALSEP(val)) retval = 0; else retval = 1;}
    else if (FALSEP(cgival)) retval = 0;
    else retval = 1;
    kno_decref(cgival);}
  kno_decref(val);
  return retval;
}
#endif

KNO_EXPORT lispval kno_cgiexec(lispval proc,lispval cgidata)
{
  if (KNO_LAMBDAP(proc)) {
    lispval value = VOID;
#if KNO_IPEVAL_ENABLED
    int ipeval = use_ipeval(proc,cgidata);
#else
    int ipeval = 0;
#endif
    if (!(ipeval))
      value = kno_xapply_lambda((kno_lambda)proc,(void *)cgidata,
				(lispval (*)(void *,lispval))cgigetvar);
#if KNO_IPEVAL_ENABLED
    else {
      struct U8_OUTPUT *out = u8_current_output;
      struct CGICALL call={proc,cgidata,out,-1,VOID};
      kno_ipeval_call(cgiexecstep,(void *)&call);
      value = call.result;}
#endif
    return value;}
  else return kno_apply(proc,0,NULL);
}

/* Parsing query strings */

DEFPRIM1("urldata/parse",urldata_parse,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
         "`(URLDATA/PARSE *arg0*)` **undocumented**",
         kno_string_type,KNO_VOID);
static lispval urldata_parse(lispval qstring)
{
  lispval smap = kno_empty_slotmap();
  parse_query_string((kno_slotmap)smap,CSTRING(qstring),STRLEN(qstring));
  return smap;
}

/* Bind request and output */

static lispval withreqout_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  U8_OUTPUT *oldout = u8_current_output;
  U8_OUTPUT _out, *out = &_out;
  lispval body = kno_get_body(expr,1);
  lispval reqinfo = kno_empty_slotmap();
  lispval result = VOID;
  lispval oldinfo = kno_push_reqinfo(reqinfo);
  U8_INIT_OUTPUT(&_out,1024);
  u8_set_default_output(out);
  {KNO_DOLIST(ex,body) {
      if (KNO_ABORTP(result)) {
        u8_free(_out.u8_outbuf);
        return result;}
      kno_decref(result);
      result = kno_eval(ex,env,_stack,0);}}
  u8_set_default_output(oldout);
  kno_output_xhtml_preface(oldout,reqinfo);
  u8_putn(oldout,_out.u8_outbuf,(_out.u8_write-_out.u8_outbuf));
  u8_printf(oldout,"\n</body>\n</html>\n");
  kno_decref(result);
  u8_free(_out.u8_outbuf);
  u8_flush(oldout);
  kno_use_reqinfo(oldinfo);
  return VOID;
}

/* URI mapping */

KNO_EXPORT
lispval kno_mapurl(lispval uri)
{
  lispval mapfn = kno_req(mapurlfn_symbol);
  if (KNO_APPLICABLEP(mapfn)) {
    lispval result = kno_apply(mapfn,1,&uri);
    kno_decref(mapfn);
    return result;}
  else {
    kno_decref(mapfn);
    return EMPTY;}
}

DEFPRIM1("mapurl",mapurl,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
         "`(MAPURL *arg0*)` **undocumented**",
         kno_string_type,KNO_VOID);
static lispval mapurl(lispval uri)
{
  lispval result = kno_mapurl(uri);
  if (KNO_ABORTP(result)) return result;
  else if (STRINGP(result)) return result;
  else {
    kno_decref(result); return kno_incref(uri);}
}

/* Initialization */

static int cgiexec_initialized = 0;

KNO_EXPORT int sendfile_set(lispval ignored,lispval v,void *vptr)
{
  u8_string *ptr = vptr;
  if (STRINGP(v)) {
    int bool = kno_boolstring(CSTRING(v),-1);
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
  else if (KNO_TRUEP(v)) {
    if (*ptr) u8_free(*ptr);
    *ptr = u8_strdup("X-Sendfile");
    return 1;}
  else if (FALSEP(v)) {
    if (*ptr) u8_free(*ptr);
    *ptr = NULL;
    return 0;}
  else return kno_reterr(kno_TypeError,"kno_sconfig_set",u8_strdup(_("string")),v);
}
KNO_EXPORT int xredirect_set(lispval ignored,lispval v,void *vptr)
{
  u8_string *ptr = vptr;
  if (STRINGP(v)) {
    int bool = kno_boolstring(CSTRING(v),-1);
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
  else if (KNO_TRUEP(v)) {
    if (*ptr) u8_free(*ptr);
    *ptr = u8_strdup("X-Sendfile");
    return 1;}
  else if (FALSEP(v)) {
    if (*ptr) u8_free(*ptr);
    *ptr = NULL;
    return 0;}
  else return kno_reterr(kno_TypeError,"kno_sconfig_set",u8_strdup(_("string")),v);
}

static lispval webtools_module, xhtml_module;

KNO_EXPORT void kno_init_cgiexec_c()
{
  lispval module;
  if (cgiexec_initialized) return;
  cgiexec_initialized = 1;
  kno_init_scheme();
  webtools_module = module = kno_new_cmodule("webtools",0,kno_init_cgiexec_c);
  xhtml_module = kno_new_cmodule("xhtml",0,kno_init_cgiexec_c);

  u8_init_mutex(&protected_cgi_lock);

  kno_def_evalfn(module,"HTTPHEADER",httpheader,
		 "*undocumented*");

  link_local_cprims();

  kno_def_evalfn(module,"WITH/REQUEST/OUT",withreqout_evalfn,
		 "*undocumented*");
  kno_defalias(module,"WITHCGIOUT","WITH/REQUEST/OUT");

  kno_defalias2(module,"WITHCGI",kno_scheme_module,"WITH/REQUEST");
  kno_defalias2(module,"CGICALL",kno_scheme_module,"REQ/CALL");
  kno_defalias2(module,"CGIGET",kno_scheme_module,"REQ/GET");
  kno_defalias2(module,"CGIVAL",kno_scheme_module,"REQ/VAL");
  kno_defalias2(module,"CGITEST",kno_scheme_module,"REQ/TEST");
  kno_defalias2(module,"CGISET!",kno_scheme_module,"REQ/SET!");
  kno_defalias2(module,"CGIADD!",kno_scheme_module,"REQ/ADD!");
  kno_defalias2(module,"CGIADD!",kno_scheme_module,"REQ/DROP!");


  /* kno_def_evalfn(module,"CGIVAR",cgivar_evalfn,
     "*undocumented*"); */

  kno_def_evalfn(xhtml_module,"HTMLHEADER",htmlheader,
		 "*undocumented*");
  kno_def_evalfn(xhtml_module,"TITLE!",title_evalfn,
		 "*undocumented*");
  kno_def_evalfn(xhtml_module,"JSOUT",jsout_evalfn,
		 "*undocumented*");
  kno_def_evalfn(xhtml_module,"CSSOUT",cssout_evalfn,
		 "*undocumented*");

  tail_symbol = kno_intern("%tail");
  browseinfo_symbol = kno_intern("browseinfo");
  mapurlfn_symbol = kno_intern("mapurlfn");

  accept_type = kno_intern("http_accept");
  accept_language = kno_intern("http_accept_language");
  accept_encoding = kno_intern("http_accept_encoding");
  accept_charset = kno_intern("http_accept_charset");
  http_referrer = kno_intern("http_referrer");

  http_cookie = kno_intern("http_cookie");
  outcookies_symbol = kno_intern("_cookies%out");
  incookies_symbol = kno_intern("_cookies%in");
  bad_cookie = kno_intern("_badcookies");
  cookiedata_symbol = kno_intern("_cookiedata");

  server_port = kno_intern("server_port");
  remote_port = kno_intern("remote_port");
  request_method = kno_intern("request_method");
  request_uri = kno_intern("request_uri");

  query_string = kno_intern("query_string");
  query_elts = kno_intern("qelts");
  query = kno_intern("query");

  get_method = kno_intern("get");
  post_method = kno_intern("post");

  status_field = kno_intern("status");
  redirect_field = kno_intern("_redirect");
  sendfile_field = kno_intern("_sendfile");
  xredirect_field = kno_intern("_xredirect");
  http_headers = kno_intern("http-headers");
  html_headers = kno_intern("html-headers");

  content_type = kno_intern("content-type");
  cgi_content_type = kno_intern("content_type");
  incoming_content_type = kno_intern("incoming-content-type");
  content_slotid = kno_intern("content");
  content_length = kno_intern("content-length");
  incoming_content_length = kno_intern("incoming-content-length");

  doctype_slotid = kno_intern("doctype");
  xmlpi_slotid = kno_intern("xmlpi");
  html_attribs_slotid = kno_intern("%html");
  body_attribs_slotid = kno_intern("%body");
  body_classes_slotid = kno_intern("%bodyclasses");
  html_classes_slotid = kno_intern("%htmlclasses");
  class_symbol = kno_intern("class");

  post_data_slotid = kno_intern("post_data");
  multipart_form_data = kno_mkstring("multipart/form-data");
  www_form_urlencoded = kno_mkstring("application/x-www-form-urlencoded");

  filename_slotid = kno_intern("filename");
  name_slotid = kno_intern("name");
  parts_slotid = kno_intern("parts");

  remote_user_symbol = kno_intern("remote_user");
  remote_host_symbol = kno_intern("remote_host");
  remote_addr_symbol = kno_intern("remote_addr");
  remote_ident_symbol = kno_intern("remote_ident");
  remote_agent_symbol = kno_intern("http_user_agent");
  remote_info_symbol = kno_intern("remote_info");

  params_symbol = kno_intern("_params");

  ipeval_symbol = kno_intern("_ipeval");

  protected_params[n_protected_params++] = kno_intern("status");
  protected_params[n_protected_params++] = kno_intern("authorization");
  protected_params[n_protected_params++] = kno_intern("script_filename");
  protected_params[n_protected_params++] = kno_intern("request_method");
  protected_params[n_protected_params++] = kno_intern("document_root");

  kno_register_config
    ("CGIPREP",
     _("Functions to execute between parsing and responding to a CGI request"),
     kno_lconfig_get,kno_lconfig_set,&cgi_prepfns);
  kno_register_config
    ("XSENDFILE",
     _("Header for using the web server's X-SENDFILE functionality"),
     kno_sconfig_get,sendfile_set,&kno_sendfile_header);
  kno_register_config
    ("XREDIRECT",
     _("Header for using the web server's X-REDIRECT functionality"),
     kno_sconfig_get,xredirect_set,&kno_xredirect_header);
  kno_register_config
    ("LOGCGI",_("Whether to log CGI bindings passed to Kno"),
     kno_boolconfig_get,kno_boolconfig_set,&log_cgidata);
  kno_register_config
    ("QUERYONPOST",
     _("Whether to parse REST query args when there is POST data"),
     kno_boolconfig_get,kno_boolconfig_set,&parse_query_on_post);
  kno_register_config
    ("CGI:PROTECT",_("Fields to avoid binding directly for CGI requests"),
     protected_cgi_get,protected_cgi_set,NULL);

  u8_register_source_file(_FILEINFO);
}



static void link_local_cprims()
{
  KNO_LINK_PRIM("mapurl",mapurl,1,webtools_module);
  KNO_LINK_PRIM("urldata/parse",urldata_parse,1,webtools_module);
  KNO_LINK_PRIM("htmlclass!",add_html_class,1,webtools_module);
  KNO_LINK_PRIM("bodyclass!",add_body_class,1,webtools_module);
  KNO_LINK_VARARGS("body!",set_body_attribs,webtools_module);
  KNO_LINK_PRIM("javascript!",add_javascript,1,xhtml_module);
  KNO_LINK_PRIM("stylesheet!",add_stylesheet,2,xhtml_module);
  KNO_LINK_PRIM("clear-cookie!",clearcookie,4,webtools_module);
  KNO_LINK_PRIM("set-cookie!",setcookie,6,webtools_module);
  KNO_LINK_PRIM("httpheader!",addhttpheader,1,webtools_module);

  KNO_LINK_ALIAS("cgiparse",urldata_parse,webtools_module);
}
