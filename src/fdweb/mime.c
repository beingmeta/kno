/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2007 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

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

static fd_exception NoMultiPartSeparator=_("Multipart MIME document has no separator");

static fdtype content_type_slotid, headers_slotid, content_disposition_slotid, content_slotid;
static fdtype charset_slotid, encoding_slotid, separator_slotid;
static fdtype multipart_symbol, text_symbol, preamble_slotid, parts_slotid;

static fdtype parse_fieldname(char *start,char *end)
{
  U8_OUTPUT out; char buf[128], *scan=start; fdtype fieldid;
  U8_INIT_OUTPUT_BUF(&out,128,buf);
  while (scan<end) {
    int c=*scan++; u8_putc(&out,toupper(c));}
  fieldid=fd_intern(out.u8_outbuf);
  u8_close((u8_stream)&out);
  return fieldid;
}

u8_byte *parse_headers(fdtype s,u8_byte *start,u8_byte *end)
{
  U8_OUTPUT hstream;
  u8_byte *hstart=start;
  U8_INIT_OUTPUT(&hstream,1024);
  while (hstart<end) {
    fdtype slotid;
    u8_byte *colon=strchr(hstart,':'), *vstart;
    if (colon) {
      slotid=parse_fieldname(hstart,colon);
      vstart=colon+1; while (isspace(*vstart)) vstart++;}
    else {slotid=headers_slotid; vstart=hstart;}
    hstream.u8_outptr=hstream.u8_outbuf;
    while (1) {
      u8_byte *line_end=strchr(vstart,'\n');
      if (line_end>end) line_end=end;
      if (line_end[-1]=='\r')
	u8_putn(&hstream,vstart,(line_end-vstart)-1);
      else u8_putn(&hstream,vstart,(line_end-vstart));
      if ((line_end) && ((line_end[1]==' ') || (line_end[1]=='\t'))) vstart=line_end+1;
      else {
	fdtype slotval=
	  fd_init_string(NULL,-1,u8_mime_convert
			 (hstream.u8_outbuf,hstream.u8_outptr));
	fd_add(s,slotid,slotval); fd_decref(slotval); hstart=line_end+1;
	break;}}
    if (*hstart=='\n') return hstart+1;
    else if ((*hstart=='\r') && (hstart[1]=='\n')) return hstart+2;}
  return end;
}

static void handle_parameters(fdtype fields,u8_byte *data)
{
  u8_byte *scan=data, *start=scan;
  int c=u8_sgetc(&scan);
  while (u8_isspace(c)) {start=scan; c=u8_sgetc(&scan);}
  while (c>0) {
    u8_byte *equals=strchr(start,'=');
    if (equals==NULL) start=strchr(start,';');
    else {
      u8_byte *vstart=equals+1, *vend;
      fdtype slotid, slotval;
      if (*vstart=='"') {vstart++; vend=strchr(vstart,'"');}
      else if (*vstart=='\'') {vstart++; vend=strchr(vstart,'\'');}
      else vend=strchr(vstart,';');
      slotid=parse_fieldname(start,equals);
      slotval=fd_extract_string(NULL,vstart,vend);
      fd_store(fields,slotid,slotval); fd_decref(slotval);
      if (vend==NULL) start=vend;
      else if (*vend==';') start=vend+1;
      else {start=strchr(vend,';'); if (start) start++;}}
    if (start==NULL) c=-1;
    else {
      scan=start; c=u8_sgetc(&scan);
      while (u8_isspace(c)) {start=scan; c=u8_sgetc(&scan);}}}
}

FD_EXPORT
fdtype fd_handle_compound_mime_field(fdtype fields,fdtype slotid,fdtype orig_slotid)
{
  fdtype value=fd_get(fields,slotid,FD_VOID);
  if (FD_VOIDP(value)) return FD_VOID;
  else if (!(FD_STRINGP(value))) {
    fdtype err=fd_err(fd_TypeError,"fd_handle_compound_mime_field",_("string"),value);
    fd_decref(value); return err;}
  else {
    fdtype major_type=FD_VOID;
    u8_string data=FD_STRDATA(value), start=data, end, scan;
    if (FD_SYMBOLP(orig_slotid)) fd_store(fields,orig_slotid,value);
    if ((end=(strchr(start,';')))) { 
      fdtype segval=fd_extract_string(NULL,start,end);
      fd_store(fields,slotid,segval); fd_decref(segval);}
    if ((scan=strchr(start,'/')) && ((end==NULL) || (scan<end))) {
      major_type=parse_fieldname(start,scan);
      fd_add(fields,slotid,major_type);}
    if (end) handle_parameters(fields,end+1);
    fd_decref(value);
    return major_type;}
}

static fdtype convert_data(char *start,char *end,fdtype dataenc,int could_be_string)
{
  char *data; int len;
  /* First do any conversion you need to do. */
  if (FD_STRINGP(dataenc))
    if (strcasecmp(FD_STRDATA(dataenc),"quoted-printable")==0) 
      data=u8_read_quoted_printable(start,end,&len); 
    else if (strcasecmp(FD_STRDATA(dataenc),"base64")==0) 
      data=u8_read_base64(start,end,&len);
    else {
      len=end-start; data=u8_malloc(len); memcpy(data,start,len);}
  else {
    len=end-start; data=u8_malloc(len); memcpy(data,start,len);}
  if ((could_be_string) && (!(FD_STRINGP(dataenc))) && (len<50000)) {
    /* If it might be a string, check if it's ASCII without NULs.
       If so, return a string, otherwise return a packet. */
    int i=0; while (i<len)
      if (data[i]<=0)
	return fd_init_packet(NULL,len,data);
      else i++;
    return fd_init_string(NULL,len,data);}
  else return fd_init_packet(NULL,len,data);
}

static fdtype convert_text
  (char *start,char *end,fdtype dataenc,fdtype charenc)
{
  int len; u8_encoding encoding;
  u8_byte *data, *scan, *data_end;
  struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,1024);
  if (FD_STRINGP(dataenc))
    if (strcasecmp(FD_STRDATA(dataenc),"quoted-printable")==0) 
      data=u8_read_quoted_printable(start,end,&len); 
    else if (strcasecmp(FD_STRDATA(dataenc),"base64")==0) 
      data=u8_read_base64(start,end,&len); 
    else {
      len=end-start; data=u8_malloc(len); memcpy(data,start,len);}
  else {
    len=end-start; data=u8_malloc(len); memcpy(data,start,len);}
  if (FD_STRINGP(charenc))
    encoding=u8_get_encoding(FD_STRDATA(charenc));
  else encoding=NULL;
  scan=data; data_end=data+len;
  u8_convert(encoding,1,&out,&scan,data_end);
  return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
}

static fdtype convert_content(char *start,char *end,fdtype majtype,fdtype dataenc,fdtype charenc)
{
  if (end==NULL) end=start+strlen(start);
  if ((FD_STRINGP(charenc)) || (FD_EQ(majtype,text_symbol)))
    return convert_text(start,end,dataenc,charenc);
  else return convert_data(start,end,dataenc,FD_VOIDP(majtype));
}

FD_EXPORT
fdtype fd_parse_multipart_mime(fdtype slotmap,char *start,char *end)
{
  char *scan=start, *boundary; int boundary_len;
  fdtype charenc, dataenc;
  fdtype majtype=fd_handle_compound_mime_field
    (slotmap,content_type_slotid,FD_VOID);
  fdtype parts=FD_EMPTY_LIST;
  fdtype sepval=fd_get(slotmap,separator_slotid,FD_VOID);
  if (!(FD_STRINGP(sepval))) {
    fd_decref(charenc); fd_decref(dataenc); 
    return fd_err(NoMultiPartSeparator,"fd_parse_mime",NULL,slotmap);}
  fd_handle_compound_mime_field(slotmap,content_disposition_slotid,FD_VOID);
  boundary_len=FD_STRLEN(sepval)+2; boundary=u8_malloc(boundary_len+1);
  strcpy(boundary,"--"); strcat(boundary,FD_STRDATA(sepval));
  start=scan; scan=strstr(start,boundary);
  if (scan==NULL) {
    fd_store(slotmap,preamble_slotid,
	     convert_content(start,scan,majtype,dataenc,charenc));
    fd_store(slotmap,parts_slotid,FD_EMPTY_LIST);}
  else {
    fdtype *point=&parts;
    if (scan>start)
      fd_store(slotmap,preamble_slotid,
	       convert_content(scan,end,majtype,dataenc,charenc));
    start=scan+boundary_len;
    while (start<end) {
      fdtype new_pair;
      if (strncmp(start,"--",2)==0) break;
      else if (start[0]=='\n') start++;
      else if ((start[0]=='\r') && (start[1]=='\n')) start=start+2;
      scan=strstr(start,boundary);
      if (scan)
	new_pair=fd_init_pair(NULL,fd_parse_mime(start,scan),FD_EMPTY_LIST);
      else new_pair=
	     fd_init_pair(NULL,fd_parse_mime(start,start+strlen(start)),
			  FD_EMPTY_LIST);
      *point=new_pair; point=&(FD_CDR(new_pair));
      if (scan==NULL)  break; else start=scan+boundary_len;}
    fd_store(slotmap,parts_slotid,parts);}
  return parts;
}

FD_EXPORT
fdtype fd_parse_mime(char *start,char *end)
{
  fdtype slotmap=fd_init_slotmap(NULL,0,NULL);
  char *scan=parse_headers(slotmap,start,end);
  fdtype majtype=fd_handle_compound_mime_field(slotmap,content_type_slotid,FD_VOID);
  fdtype charenc, dataenc;
  fd_handle_compound_mime_field(slotmap,content_disposition_slotid,FD_VOID);
  if (FD_ABORTP(majtype)) 
    return majtype;
  charenc=fd_get(slotmap,charset_slotid,FD_VOID);
  dataenc=fd_get(slotmap,encoding_slotid,FD_VOID);
  if (fd_test(slotmap,content_type_slotid,multipart_symbol)) {
    fdtype parts=fd_parse_multipart_mime(slotmap,scan,end);
    fd_decref(parts); return slotmap;}
  else {
    fdtype content=convert_content(scan,end,majtype,dataenc,charenc);
    fd_store(slotmap,content_slotid,content);
    fd_decref(content);}
  fd_decref(charenc); fd_decref(dataenc);
  return slotmap;
}

static fdtype parse_mime_data(fdtype arg)
{
  if (FD_PACKETP(arg))
    return fd_parse_mime(FD_PACKET_DATA(arg),
			 FD_PACKET_DATA(arg)+FD_PACKET_LENGTH(arg));
  else if (FD_STRINGP(arg))
    return fd_parse_mime(FD_STRDATA(arg),
			 FD_STRDATA(arg)+FD_STRLEN(arg));
  else return fd_type_error(_("mime data"),"parse_mime_data",arg);
}


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

void fd_init_mime_c()
{
  fdtype module=fd_new_module("FDWEB",(FD_MODULE_DEFAULT|FD_MODULE_SAFE));
  fdtype unsafe_module=fd_new_module("FDWEB",(FD_MODULE_DEFAULT));
  fd_idefn(module,fd_make_cprim1("PARSE-MIME",parse_mime_data,1));

  fd_idefn(unsafe_module,fd_make_cprim5("SMTP",smtp_function,3));
  fd_defspecial(unsafe_module,"MAILOUT",mailout_handler);

  content_slotid=fd_intern("CONTENT");
  charset_slotid=fd_intern("CHARSET");
  encoding_slotid=fd_intern("CONTENT-TRANSFER-ENCODING");
  content_type_slotid=fd_intern("CONTENT-TYPE");
  content_disposition_slotid=fd_intern("CONTENT-DISPOSITION");
  separator_slotid=fd_intern("BOUNDARY");
  multipart_symbol=fd_intern("MULTIPART");
  headers_slotid=fd_intern("HEADERS");
  text_symbol=fd_intern("TEXT");
  preamble_slotid=fd_intern("PREAMBLE");
  parts_slotid=fd_intern("PARTS");

  fd_register_source_file(versionid);
}
