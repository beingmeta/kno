/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2009 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define U8_INLINE_IO 1
#define FD_PROVIDE_FASTEVAL 1

#include "fdb/dtype.h"
#include "fdb/tables.h"
#include "fdb/eval.h"
#include "fdb/ports.h"
#include "fdb/fdweb.h"
#include "fdb/support.h"

#include <libu8/xfiles.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8streamio.h>

#include <ctype.h>

static fdtype accept_language, accept_type, accept_charset, accept_encoding;
static fdtype server_port, remote_port, request_method, status_field;
static fdtype get_method, post_method, cgidata_symbol, browseinfo_symbol;
static fdtype query_string, query_elts, query, http_cookie, http_referrer;
static fdtype http_headers, html_headers, cookies_symbol, text_symbol;
static fdtype doctype_slotid, xmlpi_slotid, body_attribs_slotid;
static fdtype content_slotid, content_type, cgi_content_type;
static fdtype remote_user_symbol, remote_host_symbol, remote_addr_symbol;
static fdtype remote_info_symbol, remote_agent_symbol, remote_ident_symbol;
static fdtype parts_slotid, name_slotid, filename_slotid, mapurlfn_symbol;

static int log_cgidata=0;

static u8_condition CGIData="CGIDATA";
static u8_condition CGIDataInconsistency="Inconsistent CGI data";

#define DEFAULT_CONTENT_TYPE \
  "Content-type: text/html; charset=utf-8;"
#define DEFAULT_DOCTYPE \
  "<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.1//EN\"\
               \"http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd\">"
#define DEFAULT_XMLPI \
  "<?xml version='1.0' charset='utf-8' ?>"

/* Utility functions */

static fdtype get_cgidata()
{
  fdtype table=fd_thread_get(cgidata_symbol);
  if ((!table) || (!(FD_TABLEP(table)))) {
    table=fd_init_slotmap(NULL,0,NULL);
    fd_thread_set(cgidata_symbol,table);
    return table;}
  else return table;
}

static fdtype try_parse(u8_string buf)
{
  fdtype val=fd_parse(buf);
  if (FD_ABORTP(val)) {
    fd_decref(val); return fdtype_string(buf);}
  else return val;
}

static fdtype buf2lisp(char *buf,int isascii)
{
  if (buf[0]!=':')
    if (isascii) return fdtype_string(buf);
    else return fd_init_string(NULL,-1,u8_valid_copy(buf));
  else if (isascii) return try_parse(buf+1);
  else {
    u8_string s=u8_valid_copy(buf+1);
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
  else return fd_init_string(NULL,-1,u8_valid_copy(buf));
}

static void emit_uri_string(u8_output out,u8_string string)
{
  char *scan=string;
  while (*scan) {
    if (*scan=='+') u8_putc(out,' ');
    else if (isalnum(*scan)) u8_putc(out,*scan);
    else u8_printf(out,"%%%02x",*scan);
    scan++;}
}

static void emit_uri_value(u8_output out,fdtype val)
{
  if (FD_STRINGP(val)) emit_uri_string(out,FD_STRDATA(val));
  else {
    u8_string as_string=fd_dtype2string(val);
    u8_puts(out,"%3a"); emit_uri_string(out,as_string);
    u8_free(as_string);}
}

FD_EXPORT fdtype fd_get_cgidata()
{
  return get_cgidata();
}

FD_EXPORT
void fd_urify(u8_output out,fdtype val)
{
  emit_uri_value(out,val);
}

/* Converting cgidata */

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
	  fdtype _namesym=intern_compound("_",nstring);
	  fdtype ctype=fd_get(elt,content_type,FD_VOID);
	  fdtype content=fd_get((fdtype)elt,content_slotid,FD_EMPTY_CHOICE);
	  /* Add the part itself in the _name slotid */
	  fd_add((fdtype)c,_namesym,elt);
	  /* Add the filename slot if it's an upload */
	  if (fd_test(elt,filename_slotid,FD_VOID)) {
	    fdtype filename=fd_get(elt,filename_slotid,FD_EMPTY_CHOICE);
	    fd_add((fdtype)c,intern_compound(nstring,"_FILENAME"),
		   filename);
	    fd_decref(filename);}
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
      *write++='\0'; slotid=buf2slotid(buf,isascii);
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

static void setcookiedata(fdtype cgidata,fdtype cookiedata)
{
  fdtype cookies=fd_get(cgidata,cookies_symbol,FD_EMPTY_CHOICE);
  fdtype cookievar=FD_VECTOR_REF(cookiedata,0);
  FD_DO_CHOICES(cookie,cookies)
    if (FD_EQ(FD_VECTOR_REF(cookie,0),cookievar))
      fd_drop(cgidata,cookies_symbol,cookie);
  fd_decref(cookies);
  fd_add(cgidata,cookies_symbol,cookiedata);
}

static void convert_cookie_arg(fd_slotmap c)
{
  fdtype qval=fd_slotmap_get(c,http_cookie,FD_VOID);
  if (!(FD_STRINGP(qval))) {fd_decref(qval); return;}
  else {
    fdtype slotid=FD_VOID, value=FD_VOID;
    int len=FD_STRLEN(qval); int isascii=1;
    u8_byte *scan=FD_STRDATA(qval), *end=scan+len;
    char *buf=u8_malloc(len), *write=buf;
    while (scan<end)
      if ((FD_VOIDP(slotid)) && (*scan=='=')) {
	*write++='\0';
	if (isascii) slotid=fd_parse(buf);
	else {
	  u8_string s=u8_valid_copy(buf);
	  slotid=fd_parse(s); u8_free(s);}
	write=buf; isascii=1; scan++;}
      else if (*scan==';') {
	*write++='\0';
	if (FD_VOIDP(slotid)) value=buf2string(buf,isascii);
	else value=buf2lisp(buf,isascii);
	if (FD_VOIDP(slotid))
	  u8_log(LOG_WARN,_("malformed cookie"),"strange cookie syntax: \"%s\"",
		  FD_STRDATA(qval));
	else {
	  fdtype cookiedata=fd_make_vector(2,slotid,fd_incref(value));
	  fd_slotmap_add(c,slotid,value);
	  setcookiedata((fdtype)c,cookiedata);
	  fd_decref(cookiedata);}
	fd_decref(value); value=FD_VOID; slotid=FD_VOID;
	write=buf; isascii=1; scan++;}
      else if (*scan == '%') 
	if (scan+3>=end) end=scan;
	else {
	  char buf[4]; int c; scan++;
	  buf[0]=*scan++; buf[1]=*scan++; buf[2]='\0';
	  c=strtol(buf,NULL,16);
	  if (c>=0x80) isascii=0;
	  *write++=c;}
      else if (*scan == '+') {*write++=' '; scan++;}
      else if (*scan == ' ') scan++;
      else if (*scan<0x80) *write++=*scan++;
      else {*write++=*scan++; isascii=0;}
    if (write>buf) {
      *write++='\0';
      if (FD_VOIDP(slotid)) value=buf2string(buf,isascii);
      else value=buf2lisp(buf,isascii);
      if (FD_VOIDP(slotid))
	u8_log(LOG_WARN,_("malformed cookie"),"strange cookie syntax: \"%s\"",
		FD_STRDATA(qval));
      else {
	fdtype cookiedata=fd_make_vector(2,slotid,fd_incref(value));
	fd_slotmap_add(c,slotid,value);
	setcookiedata((fdtype)c,cookiedata);
	fd_decref(cookiedata);}
      fd_decref(value); value=FD_VOID; slotid=FD_VOID;
      write=buf; isascii=1; scan++;}
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
  fdtype remote_info_string=FD_VOID;
  struct U8_OUTPUT remote_info;
  U8_INIT_OUTPUT(&remote_info,128);
  u8_printf(&remote_info,"%s%s%s@%s%s%s<%s",
	    ((FD_STRINGP(remote_ident)) ? (FD_STRDATA(remote_ident)) : ((u8_string)"")),
	    ((FD_STRINGP(remote_ident)) ? ((u8_string)"|") : ((u8_string)"")),
	    ((FD_STRINGP(remote_user)) ? (FD_STRDATA(remote_user)) : ((u8_string)"nobody")),
	    ((FD_STRINGP(remote_host)) ? (FD_STRDATA(remote_host)) : ((u8_string)"")),
	    ((FD_STRINGP(remote_host)) ? ((u8_string)"/") : ((u8_string)"")),
	    ((FD_STRINGP(remote_addr)) ? (FD_STRDATA(remote_addr)) : ((u8_string)"noaddr")),
	    ((FD_STRINGP(remote_agent)) ? (FD_STRDATA(remote_agent)) : ((u8_string)"noagent")));
  remote_info_string=fd_init_string(NULL,remote_info.u8_outptr-remote_info.u8_outbuf,remote_info.u8_outbuf);
  fd_store(cgidata,remote_info_symbol,remote_info_string);
  fd_decref(remote_info_string);
}


/* Generating headers */

static fdtype do_xmlout(U8_OUTPUT *out,fdtype body,fd_lispenv env)
{
  U8_OUTPUT *prev;
  prev=fd_get_default_output();
  fd_set_default_output(out);
  while (FD_PAIRP(body)) {
    fdtype value=fasteval(FD_CAR(body),env);
    body=FD_CDR(body);
    if (FD_ABORTP(value)) {
      fd_set_default_output(prev);
      return value;}
    else if (FD_VOIDP(value)) continue;
    else if (FD_STRINGP(value))
      u8_printf(out,"%s",FD_STRDATA(value));
    else u8_printf(out,"%q",value);
    fd_decref(value);}
  fd_set_default_output(prev);
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
    fdtype cgidata=fd_thread_get(cgidata_symbol);
    if (FD_TABLEP(cgidata)) {
      fdtype header=fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
      fd_add(cgidata,http_headers,header);
      fd_decref(header);}
    else {
      U8_OUTPUT *port=fd_get_default_output();
      u8_printf(port,"http>> %s\n",out.u8_outbuf);
      u8_free(out.u8_outbuf);}
    fd_decref(cgidata);
    return FD_VOID;}
}
	  
static fdtype htmlheader(fdtype expr,fd_lispenv env)
{
  U8_OUTPUT out; fdtype result;
  U8_INIT_OUTPUT(&out,64);
  result=do_xmlout(&out,fd_get_body(expr,1),env);
  if (FD_ABORTP(result)) {
    u8_free(out.u8_outbuf);
    return result;}
  else {
    fdtype cgidata=fd_thread_get(cgidata_symbol);
    if (FD_TABLEP(cgidata)) {
      fdtype header=fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
      fdtype current=fd_get(cgidata,html_headers,FD_EMPTY_LIST);
      fdtype new=fd_init_pair(NULL,header,current);
      fd_store(cgidata,html_headers,new);
      fd_decref(new);}
    else {
      U8_OUTPUT *port=fd_get_default_output();
      u8_printf(port,"http>> %s\n",out.u8_outbuf);
      u8_free(out.u8_outbuf);}
    fd_decref(cgidata);
    return FD_VOID;}
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
    emit_uri_value(out,FD_VECTOR_REF(cookie,1)); u8_puts(out,";");
    if (len>2) {
      fdtype domain=FD_VECTOR_REF(cookie,2);
      fdtype path=((len>3) ? (FD_VECTOR_REF(cookie,3)) : (FD_VOID));
      fdtype expires=((len>4) ? (FD_VECTOR_REF(cookie,4)) : (FD_VOID));
      fdtype secure=((len>5) ? (FD_VECTOR_REF(cookie,5)) : (FD_VOID));
      if (FD_STRINGP(domain))
	u8_printf(out," domain=.%s;",FD_STRDATA(domain));
      if (FD_STRINGP(expires))
	u8_printf(out," expires=%s;",FD_STRDATA(expires));
      else if (FD_PTR_TYPEP(expires,fd_timestamp_type)) {
	struct FD_TIMESTAMP *tstamp=(fd_timestamp)expires;
	char buf[512]; struct tm tptr;
	u8_xtime_to_tptr(&(tstamp->xtime),&tptr);
	strftime(buf,512,"%A, %d-%b-%Y %T GMT",&tptr);
	u8_printf(out," expires=%s;",buf);}
      if (FD_STRINGP(path))
	u8_printf(out," path=%s",FD_STRDATA(path));
      if (!((FD_VOIDP(secure))||(FD_FALSEP(secure))))
	u8_printf(out," secure");}
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
    fdtype cookiedata, cgidata=fd_thread_get(cgidata_symbol);
    if (FD_VOIDP(domain)) domain=FD_FALSE;
    if (FD_VOIDP(path)) path=FD_FALSE;
    if (FD_VOIDP(expires)) expires=FD_FALSE;
    cookiedata=
      fd_make_vector(5,fd_incref(var),fd_incref(val),
		     fd_incref(domain),fd_incref(path),
		     fd_incref(expires),fd_incref(secure));
    if (FD_VOIDP(cgidata)) {
      u8_output out=fd_get_default_output();
      handle_cookie(out,FD_VOID,cookiedata);
      fd_decref(cookiedata);
      return FD_VOID;}
    setcookiedata(cgidata,cookiedata);
    fd_decref(cookiedata); fd_decref(cgidata);
    return FD_VOID;}
}

/* HTML Header functions */

static fdtype add_stylesheet(fdtype stylesheet,fdtype type)
{
  fdtype cgidata=fd_thread_get(cgidata_symbol);
  U8_OUTPUT out, *port;
  if (FD_VOIDP(cgidata))
    port=fd_get_default_output();
  else {U8_INIT_OUTPUT(&out,64); port=&out;}
  if (FD_VOIDP(type))
    u8_printf(port,"<link rel='stylesheet' type='text/css' href='%s'/>\n",
	      FD_STRDATA(stylesheet));
  else u8_printf(port,"<link rel='stylesheet' type='%s' href='%s'/>\n",
		 FD_STRDATA(type),FD_STRDATA(stylesheet));
  if (FD_VOIDP(cgidata)) u8_flush(port);
  else {
    fdtype header=fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
    fdtype current=fd_get(cgidata,html_headers,FD_EMPTY_LIST);
    fdtype new=fd_init_pair(NULL,header,current);
    fd_store(cgidata,html_headers,new);
    fd_decref(cgidata);
    fd_decref(new);}
  return FD_VOID;
}

static fdtype add_javascript(fdtype url)
{
  fdtype cgidata=fd_thread_get(cgidata_symbol);
  U8_OUTPUT out, *port; 
  if (FD_VOIDP(cgidata))
    port=fd_get_default_output();
  else {U8_INIT_OUTPUT(&out,64); port=&out;}
  u8_printf(port,"<script language='javascript' src='%s'>\n",
	    FD_STRDATA(url));
  u8_printf(port,"  <!-- empty content for some browsers -->\n");
  u8_printf(port,"</script>\n");
  if (FD_VOIDP(cgidata)) u8_flush(port);
  else {
    fdtype header=fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
    fdtype current=fd_get(cgidata,html_headers,FD_EMPTY_LIST);
    fdtype new=fd_init_pair(NULL,header,current);
    fd_store(cgidata,html_headers,new);
    fd_decref(new);}
  fd_decref(cgidata);
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
    fdtype cgidata=fd_thread_get(cgidata_symbol);
    if (FD_TABLEP(cgidata)) {
      fdtype header=fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
      fdtype current=fd_get(cgidata,html_headers,FD_EMPTY_LIST);
      fdtype new=fd_init_pair(NULL,header,current);
      fd_store(cgidata,html_headers,new);
      fd_decref(new);}
    else {
      U8_OUTPUT *port=fd_get_default_output();
      u8_printf(port,"http>> %s\n",out.u8_outbuf);
      u8_free(out.u8_outbuf);}
    fd_decref(cgidata);
    fd_decref(result);
    return FD_VOID;}
}      

/* Generating the XML */
void fd_output_http_headers(U8_OUTPUT *out,fdtype cgidata)
{
  fdtype ctype=fd_get(cgidata,content_type,FD_VOID);
  fdtype status=fd_get(cgidata,status_field,FD_VOID);
  fdtype headers=fd_get(cgidata,http_headers,FD_EMPTY_CHOICE);
  fdtype cookies=fd_get(cgidata,cookies_symbol,FD_EMPTY_CHOICE);
#if 0
  if (FD_VOIDP(status)) 
    u8_printf(out,"HTTP/1.1 200 Success\r\n");
  else if (FD_FIXNUMP(status)) 
    u8_printf(out,"HTTP/1.1 %d Return\r\n",FD_FIX2INT(status));
  else if (FD_STRINGP(status))
    u8_printf(out,"HTTP/1.1 500 %s\r\n",FD_STRDATA(status));
  else if ((FD_PAIRP(status)) &&
	   (FD_FIXNUMP(FD_CAR(status))) &&
	   (FD_STRINGP(FD_CDR(status))))
    u8_printf(out,"HTTP/1.1 %d %s\r\n",
	      FD_FIX2INT(FD_CAR(status)),
	      FD_STRDATA(FD_CDR(status)));
  else u8_printf(out,"HTTP/1.1 500 Bad STATUS data\r\n");
#else
  if (FD_FIXNUMP(status))
    u8_printf(out,"Status: %d\r\n",FD_FIX2INT(status));
  else if (FD_STRINGP(status))
    u8_printf(out,"Status: %s\r\n",FD_STRDATA(status));
#endif
  if (FD_STRINGP(ctype))
    u8_printf(out,"Content-type: %s\r\n",FD_STRDATA(ctype));
  else u8_printf(out,"%s\r\n",DEFAULT_CONTENT_TYPE);
  {FD_DO_CHOICES(header,headers)
     if (FD_STRINGP(header)) {
       u8_putn(out,FD_STRDATA(header),FD_STRLEN(header));
       u8_putn(out,"\r\n",2);}}
  {FD_DO_CHOICES(cookie,cookies)
     if (handle_cookie(out,cgidata,cookie)<0)
       u8_log(LOG_WARN,CGIDataInconsistency,"Bad cookie data: %q",cookie);}
  u8_putn(out,"\r\n",2);
  fd_decref(ctype); fd_decref(headers); fd_decref(cookies);
}

static void output_headers(U8_OUTPUT *out,fdtype headers)
{
  if (FD_PAIRP(headers)) {
    fdtype header=FD_CAR(headers);
    output_headers(out,FD_CDR(headers));
    u8_putn(out,FD_STRDATA(header),FD_STRLEN(header));
    u8_putn(out,"\n",1);}
}

FD_EXPORT
int fd_output_xhtml_preface(U8_OUTPUT *out,fdtype cgidata)
{
  fdtype doctype=fd_get(cgidata,doctype_slotid,FD_VOID);
  fdtype xmlpi=fd_get(cgidata,xmlpi_slotid,FD_VOID);
  fdtype body_attribs=fd_get(cgidata,body_attribs_slotid,FD_VOID);
  if (FD_VOIDP(doctype)) u8_puts(out,DEFAULT_DOCTYPE);
  else if (FD_STRINGP(doctype))
    u8_putn(out,FD_STRDATA(doctype),FD_STRLEN(doctype));
  else return 0;
  u8_putc(out,'\n');
  if (FD_STRINGP(xmlpi))
    u8_putn(out,FD_STRDATA(xmlpi),FD_STRLEN(xmlpi));
  else u8_puts(out,DEFAULT_XMLPI);
  u8_puts(out,"\n<html>\n<head>\n");
  {
    fdtype headers=fd_get(cgidata,html_headers,FD_EMPTY_LIST);
    output_headers(out,headers);
    fd_decref(headers);}
  u8_puts(out,"</head>\n");
  if (FD_FALSEP(body_attribs)) {}
  else if (FD_VOIDP(body_attribs)) u8_puts(out,"<body>");
  else {
    fd_open_markup(out,"body",body_attribs,0);}
  fd_decref(doctype); fd_decref(xmlpi); fd_decref(body_attribs);
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
  fdtype attribs=FD_EMPTY_LIST; int i=n-1;
  fdtype table=get_cgidata();
  if (n==1) {
    fd_store(table,body_attribs_slotid,args[0]);
    fd_decref(table);
    return FD_VOID;}
  while (i>=0) {
    attribs=fd_init_pair(NULL,fd_incref(args[i]),attribs); i--;}
  fd_store(table,body_attribs_slotid,attribs);
  fd_decref(table);
  fd_decref(attribs);
  return FD_VOID;
}

/* CGI Exec */

static fdtype tail_symbol, cgidata_symbol;

static fdtype cgigetvar(fdtype cgidata,fdtype var)
{
  fdtype val=fd_get(cgidata,var,FD_VOID);
  if (FD_VOIDP(val))
    if (FD_EQ(var,tail_symbol)) return fd_incref(cgidata);
    else return val;
  else if (FD_STRINGP(val)) {
    u8_string data=FD_STRDATA(val);
    if (*data=='\0') return val;
    else if (strchr("@{#(",data[0])) {
      fdtype parsed=fd_parse_arg(data);
      fd_decref(val); return parsed;}
    else if (isdigit(data[0])) {
      fdtype parsed=fd_parse_arg(data);
      if (FD_NUMBERP(parsed)) {
	fd_decref(val); return parsed;}
      else {
	fd_decref(parsed); return val;}}
    else if (*data == ':') return fd_parse(data+1);
    else if (*data == '\\') {
      fdtype shorter=fdtype_string(data+1);
      fd_decref(val);
      return shorter;}
    else return val;}
  else return val;
}

FD_EXPORT fdtype fd_cgiexec(fdtype proc,fdtype cgidata,int threadbind)
{
  fdtype value;
  if (threadbind) {
    fd_thread_set(cgidata_symbol,cgidata);
    fd_thread_set(browseinfo_symbol,FD_EMPTY_CHOICE);}
  if (FD_PTR_TYPEP(proc,fd_sproc_type))
    value=
      fd_xapply_sproc((fd_sproc)proc,(void *)cgidata,
		      (fdtype (*)(void *,fdtype))cgigetvar);
  else value=fd_apply(proc,0,NULL);
  value=fd_finish_call(value);
  if (threadbind) {
    fd_thread_set(cgidata_symbol,FD_VOID);
    fd_thread_set(browseinfo_symbol,FD_VOID);}
  if (log_cgidata) {
    fdtype keys=fd_getkeys(cgidata);
    FD_DO_CHOICES(key,keys) {
      fdtype value=fd_get(cgidata,key,FD_VOID);
      if (!(FD_VOIDP(value)))
	u8_log(LOG_DEBUG,CGIData,"%q = %q",key,value);
      fd_decref(value);}
    fd_decref(keys);}
  return value;
}

static fdtype cgicall(fdtype proc)
{
  fdtype cgidata=fd_thread_get(cgidata_symbol), value=FD_VOID;
  if (FD_PTR_TYPEP(proc,fd_sproc_type)) 
    value=
      fd_xapply_sproc((fd_sproc)proc,(void *)cgidata,
		      (fdtype (*)(void *,fdtype))cgigetvar);
  else if (FD_APPLICABLEP(proc))
    value=fd_apply(proc,0,NULL);
  else value=fd_type_error("applicable","cgicall",proc);
  fd_decref(cgidata);
  return value;
}

static fdtype cgiget(fdtype var,fdtype dflt)
{
  fdtype table=get_cgidata(), val;
  if (FD_STRINGP(var)) var=fd_intern(FD_STRDATA(var));
  val=fd_get(table,var,FD_VOID); fd_decref(table);
  if (FD_VOIDP(val))
    if (FD_VOIDP(dflt))
      return FD_EMPTY_CHOICE;
    else return fd_incref(dflt);
  else return val;
}

static fdtype cgival(fdtype var,fdtype dflt)
{
  fdtype table=get_cgidata(), val;
  if (FD_STRINGP(var)) var=fd_intern(FD_STRDATA(var));
  val=fd_get(table,var,FD_VOID); fd_decref(table);
  if (FD_VOIDP(val))
    if (FD_VOIDP(dflt))
      return FD_EMPTY_CHOICE;
    else return fd_incref(dflt);
  else if (FD_STRINGP(val)) {
    fdtype parsed=fd_parse_arg(FD_STRDATA(val));
    fd_decref(val);
    return parsed;}
  else if (FD_CHOICEP(val)) {
    fdtype result=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(v,val)
      if (FD_STRINGP(v)) {
	fdtype parsed=fd_parse_arg(FD_STRDATA(v));
	FD_ADD_TO_CHOICE(result,parsed);}
      else {fd_incref(v); FD_ADD_TO_CHOICE(result,v);}
    fd_decref(val);
    return result;}
  else return val;
}

static fdtype cgitest(fdtype vars,fdtype val)
{
  fdtype table=get_cgidata(), result=FD_FALSE;
  if (FD_TABLEP(table)) {
    FD_DO_CHOICES(var,vars) {
      if (FD_STRINGP(var)) var=fd_intern(FD_STRDATA(var));
      if (fd_test(table,var,val)) result=FD_TRUE;}}
  fd_decref(table);
  return result;
}

static fdtype cgiset(fdtype vars,fdtype value)
{
  fdtype table=get_cgidata();
  {FD_DO_CHOICES(var,vars) {
    if (FD_STRINGP(var)) var=fd_intern(FD_STRDATA(var));
    fd_store(table,var,value);}}
  fd_decref(table);
  return FD_VOID;
}

static fdtype cgiadd(fdtype var,fdtype value)
{
  fdtype table=get_cgidata();
  if (FD_STRINGP(var)) var=fd_intern(FD_STRDATA(var));
  fd_add(table,var,value);
  fd_decref(table);
  return FD_VOID;
}

static fdtype cgidrop(fdtype var,fdtype value)
{
  fdtype table=get_cgidata();
  if (FD_STRINGP(var)) var=fd_intern(FD_STRDATA(var));
  fd_drop(table,var,value);
  fd_decref(table);
  return FD_VOID;
}

static fdtype cgivar_handler(fdtype expr,fd_lispenv env)
{
  fdtype table=get_cgidata();
  FD_DOLIST(var,fd_get_body(expr,1)) {
    if (FD_TABLEP(table)) {
      fdtype val=fd_get(table,var,FD_VOID);
      fd_bind_value(var,val,env);}
    else fd_bind_value(var,FD_VOID,env);}
  fd_decref(table);
  return FD_VOID;
}

static fdtype withreq_handler(fdtype expr,fd_lispenv env)
{
  fdtype old_table=fd_thread_get(cgidata_symbol);
  fdtype new_table=fd_eval(fd_get_arg(expr,1),env);
  fdtype body=fd_get_body(expr,1);
  fdtype result=FD_VOID;
  if (!(FD_TABLEP(new_table))) {
    fd_decref(new_table); new_table=fd_init_slotmap(NULL,0,NULL);}
  fd_thread_set(cgidata_symbol,new_table);
  {FD_DOLIST(ex,body) {
      if (FD_ABORTP(result)) return result;
      fd_decref(result);
      result=fd_eval(ex,env);}}
  fd_thread_set(cgidata_symbol,old_table);
  fd_decref(new_table);
  return result;
}

static fdtype withreqout_handler(fdtype expr,fd_lispenv env)
{
  fdtype old_table=fd_thread_get(cgidata_symbol);
  fdtype new_table=fd_eval(fd_get_arg(expr,1),env);
  U8_OUTPUT *oldout=fd_get_default_output();
  U8_OUTPUT _out, *out=&_out;
  fdtype body=fd_get_body(expr,1);
  fdtype result=FD_VOID;
  if (FD_ABORTP(new_table)) return new_table;
  if (!(FD_TABLEP(new_table))) {
    fd_decref(new_table); new_table=fd_init_slotmap(NULL,0,NULL);}
  U8_INIT_OUTPUT(&_out,1024);
  fd_set_default_output(out);
  fd_thread_set(cgidata_symbol,new_table);
  {FD_DOLIST(ex,body) {
      if (FD_ABORTP(result)) {
	u8_free(_out.u8_outbuf);
	return result;}
      fd_decref(result);
      result=fd_eval(ex,env);}}
  fd_set_default_output(oldout);
  fd_output_xhtml_preface(oldout,new_table);
  u8_putn(oldout,_out.u8_outbuf,(_out.u8_outptr-_out.u8_outbuf));
  u8_printf(oldout,"\n</body>\n</html>\n");
  fd_thread_set(cgidata_symbol,old_table);
  fd_decref(new_table);
  fd_decref(result);
  u8_free(_out.u8_outbuf);
  u8_flush(oldout);
  return FD_VOID;
}

/* URI mapping */

FD_EXPORT
fdtype fd_mapurl(fdtype uri)
{
  fdtype cgidata=get_cgidata();
  if (fd_test(cgidata,mapurlfn_symbol,FD_VOID)) {
    fdtype mapfn=fd_get(cgidata,mapurlfn_symbol,FD_VOID);
    fd_decref(cgidata);
    if (FD_APPLICABLEP(mapfn))
      return fd_apply(mapfn,1,&uri);
    else return FD_EMPTY_CHOICE;}
  else {
    fd_decref(cgidata);
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

FD_EXPORT void fd_init_cgiexec_c()
{
  fdtype module, xhtmlout_module;
  if (cgiexec_initialized) return;
  cgiexec_initialized=1;
  fd_init_fdscheme();
  module=fd_new_module("FDWEB",(0));
  xhtmlout_module=fd_new_module("XHTML",FD_MODULE_SAFE);

  fd_defspecial(module,"HTTPHEADER",httpheader);
  fd_idefn(module,fd_make_cprim6("SET-COOKIE!",setcookie,2));
  fd_idefn(module,fd_make_cprimn("BODY!",set_body_attribs,1));

  fd_idefn(module,fd_make_cprim1("CGICALL",cgicall,1));
  fd_idefn(module,fd_make_cprim2("CGIGET",cgiget,1));
  fd_idefn(module,fd_make_cprim2("CGIVAL",cgival,1));
  fd_idefn(module,fd_make_ndprim(fd_make_cprim2("CGITEST",cgitest,1)));
  fd_idefn(module,fd_make_ndprim(fd_make_cprim2("CGISET!",cgiset,2)));
  fd_idefn(module,fd_make_cprim2("CGIADD!",cgiadd,2));
  fd_idefn(module,fd_make_cprim2("CGIDROP!",cgidrop,1));
  fd_idefn(module,fd_make_cprim1x("MAPURL",mapurl,1,fd_string_type,FD_VOID));
  fd_defspecial(module,"CGIVAR",cgivar_handler);
  
  fd_defspecial(module,"WITHREQ",withreq_handler);
  fd_defspecial(module,"WITHREQOUT",withreqout_handler);
  fd_defalias(module,"WITHCGI","WITHREQ");
  fd_defalias(module,"WITHCGIOUT","WITHREQOUT");

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
  cgidata_symbol=fd_intern("CGIDATA");
  browseinfo_symbol=fd_intern("BROWSEINFO");
  mapurlfn_symbol=fd_intern("MAPURLFN");

  accept_type=fd_intern("HTTP_ACCEPT");
  accept_language=fd_intern("HTTP_ACCEPT_LANGUAGE");
  accept_encoding=fd_intern("HTTP_ACCEPT_ENCODING");
  accept_charset=fd_intern("HTTP_ACCEPT_CHARSET");
  http_referrer=fd_intern("HTTP_REFERRER");
  
  http_cookie=fd_intern("HTTP_COOKIE");
  cookies_symbol=fd_intern("COOKIES");

  server_port=fd_intern("SERVER_PORT");
  remote_port=fd_intern("REMOTE_PORT");
  request_method=fd_intern("REQUEST_METHOD");

  query_string=fd_intern("QUERY_STRING");
  query_elts=fd_intern("QELTS");
  query=fd_intern("QUERY");

  get_method=fd_intern("GET");
  post_method=fd_intern("POST");

  status_field=fd_intern("STATUS");
  http_headers=fd_intern("HTTP-HEADERS");
  html_headers=fd_intern("HTML-HEADERS");

  content_type=fd_intern("CONTENT-TYPE");
  cgi_content_type=fd_intern("CONTENT_TYPE");
  content_slotid=fd_intern("CONTENT");

  doctype_slotid=fd_intern("DOCTYPE");
  xmlpi_slotid=fd_intern("XMLPI");
  body_attribs_slotid=fd_intern("%BODY");

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

  fd_register_config
    ("CGIPREP",
     _("Functions to execute between parsing and responding to a CGI request"),
     fd_lconfig_get,fd_lconfig_set,&cgi_prepfns);
  fd_register_config
    ("LOGCGI",_("Whether to log CGI bindings passed to FramerD"),
     fd_boolconfig_get,fd_boolconfig_set,&log_cgidata);
  fd_register_config
    ("QUERYONPOST",
     _("Whether to parse REST query args when there is POST data"),
     fd_boolconfig_get,fd_boolconfig_set,&parse_query_on_post);
  
  fd_register_source_file(versionid);
}
