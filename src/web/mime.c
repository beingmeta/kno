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

static u8_condition NoMultiPartSeparator=_("Multipart MIME document has no separator");

static lispval content_type_slotid, headers_slotid, content_disposition_slotid, content_slotid;
static lispval charset_slotid, encoding_slotid, separator_slotid;
static lispval multipart_symbol, preamble_slotid, parts_slotid;

static lispval parse_fieldname(u8_string start,u8_string end)
{
  U8_OUTPUT out; char buf[128];
  u8_string scan = start;
  lispval fieldid;
  U8_INIT_STATIC_OUTPUT_BUF(out,128,buf);
  while (scan<end) {
    int c = *scan++; u8_putc(&out,toupper(c));}
  fieldid = kno_intern(out.u8_outbuf);
  u8_close((u8_stream)&out);
  return fieldid;
}

const u8_byte *parse_headers(lispval s,const u8_byte *start,const u8_byte *end)
{
  U8_OUTPUT hstream;
  const u8_byte *hstart = start;
  U8_INIT_OUTPUT(&hstream,1024);
  while (hstart<end) {
    lispval slotid;
    const u8_byte *colon = strchr(hstart,':'), *vstart;
    if (colon) {
      slotid = parse_fieldname(hstart,colon);
      vstart = colon+1; while (isspace(*vstart)) vstart++;}
    else {slotid = headers_slotid; vstart = hstart;}
    hstream.u8_write = hstream.u8_outbuf;
    while (1) {
      const u8_byte *line_end = strchr(vstart,'\n');
      if (line_end>end) line_end = end;
      if (line_end[-1]=='\r')
        u8_putn(&hstream,vstart,(line_end-vstart)-1);
      else u8_putn(&hstream,vstart,(line_end-vstart));
      if ((line_end) && ((line_end[1]==' ') || (line_end[1]=='\t')))
        vstart = line_end+1;
      else {
        lispval slotval=
          kno_lispstring(u8_mime_convert
                         (hstream.u8_outbuf,hstream.u8_write));
        kno_add(s,slotid,slotval); kno_decref(slotval); hstart = line_end+1;
        break;}}
    if (*hstart=='\n') return hstart+1;
    else if ((*hstart=='\r') && (hstart[1]=='\n')) return hstart+2;}
  return end;
}

static void handle_parameters(lispval fields,const u8_byte *data)
{
  const u8_byte *scan = data, *start = scan;
  int c = u8_sgetc(&scan);
  while (u8_isspace(c)) {start = scan; c = u8_sgetc(&scan);}
  while (c>0) {
    u8_byte *equals = strchr(start,'=');
    if (equals == NULL) start = strchr(start,';');
    else {
      u8_byte *vstart = equals+1, *vend;
      lispval slotid, slotval;
      if (*vstart=='"') {vstart++; vend = strchr(vstart,'"');}
      else if (*vstart=='\'') {vstart++; vend = strchr(vstart,'\'');}
      else vend = strchr(vstart,';');
      slotid = parse_fieldname(start,equals);
      slotval = kno_substring(vstart,vend);
      kno_store(fields,slotid,slotval); kno_decref(slotval);
      if (vend == NULL) start = vend;
      else if (*vend==';') start = vend+1;
      else {start = strchr(vend,';'); if (start) start++;}}
    if (start == NULL) c = -1;
    else {
      scan = start; c = u8_sgetc(&scan);
      while (u8_isspace(c)) {start = scan; c = u8_sgetc(&scan);}}}
}

KNO_EXPORT
lispval kno_handle_compound_mime_field(lispval fields,lispval slotid,lispval orig_slotid)
{
  lispval value = kno_get(fields,slotid,VOID);
  if (VOIDP(value)) return VOID;
  else if (!(STRINGP(value))) {
    lispval err = kno_err(kno_TypeError,"kno_handle_compound_mime_field",_("string"),value);
    kno_decref(value); return err;}
  else {
    lispval major_type = VOID;
    u8_string data = CSTRING(value), start = data, end, scan;
    if (SYMBOLP(orig_slotid)) kno_store(fields,orig_slotid,value);
    if ((end = (strchr(start,';')))) {
      lispval segval = kno_substring(start,end);
      kno_store(fields,slotid,segval); kno_decref(segval);}
    if ((scan = strchr(start,'/')) && ((end == NULL) || (scan<end))) {
      major_type = parse_fieldname(start,scan);
      kno_add(fields,slotid,major_type);}
    if (end) handle_parameters(fields,end+1);
    kno_decref(value);
    return major_type;}
}

static lispval convert_data(const char *start,const char *end,
                           lispval dataenc,int could_be_string)
{
  lispval result = VOID; char *data; int len;
  /* First do any conversion you need to do. */
  if (STRINGP(dataenc))
    if (strcasecmp(CSTRING(dataenc),"quoted-printable")==0)
      data = u8_read_quoted_printable(start,end,&len);
    else if (strcasecmp(CSTRING(dataenc),"base64")==0)
      data = u8_read_base64(start,end,&len);
    else {
      len = end-start; data = u8_malloc(len); memcpy(data,start,len);}
  else {
    len = end-start; data = u8_malloc(len); memcpy(data,start,len);}
  if ((could_be_string) && (!(STRINGP(dataenc))) &&
      (len<50000) && ((len==0)||(u8_validate(data,len))))
    result = kno_make_string(NULL,len,data);
  else result = kno_make_packet(NULL,len,data);
  u8_free(data);
  return result;
}

static lispval convert_text(const char *start,const char *end,
                           lispval dataenc,lispval charenc)
{
  int len; u8_encoding encoding;
  const u8_byte *data, *scan, *data_end;
  struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,1024);
  if (STRINGP(dataenc)) {
    if (strcasecmp(CSTRING(dataenc),"quoted-printable")==0)
      data = u8_read_quoted_printable(start,end,&len);
    else if (strcasecmp(CSTRING(dataenc),"base64")==0)
      data = u8_read_base64(start,end,&len);
    else {
      u8_byte *buf;
      len = end-start; buf = u8_malloc(end-start);
      memcpy(buf,start,len);
      data = buf;}}
  else {
    u8_byte *buf;
    len = end-start; buf = u8_malloc(len);
    memcpy(buf,start,len);
    data = buf;}
  if (STRINGP(charenc))
    encoding = u8_get_encoding(CSTRING(charenc));
  else encoding = NULL;
  scan = data; data_end = data+len;
  u8_convert(encoding,1,&out,&scan,data_end);
  return kno_stream2string(&out);
}

static lispval convert_content(const char *start,const char *end,
                              lispval majtype,lispval dataenc,lispval charenc)
{
  if (end == NULL) end = start+strlen(start);
  if ((STRINGP(charenc)) || (KNO_EQ(majtype,FDSYM_TEXT)))
    return convert_text(start,end,dataenc,charenc);
  else return convert_data(start,end,dataenc,VOIDP(majtype));
}

static const char *find_boundary(const char *boundary,const char *scan,
                                 size_t len,size_t blen,
                                 int at_start)
{
  char *next;
  if ((at_start)&&(len>(blen-2))&&
      (memcmp(scan,boundary+2,blen-2)==0))
    return scan;
  else while ((len>blen)&&(next = memchr(scan,'\n',len-blen)))
         if ((next[-1]=='\r')&&(memcmp(next-1,boundary,blen)==0))
           return next-1;
         else {len = len-((next+1)-scan); scan = (next+1);}
  return NULL;
}

KNO_EXPORT
lispval kno_parse_multipart_mime(lispval slotmap,const char *start,const char *end)
{
  const char *scan = start; char *boundary; int boundary_len;
  lispval charenc, dataenc;
  lispval majtype = kno_handle_compound_mime_field
    (slotmap,content_type_slotid,VOID);
  lispval parts = NIL;
  lispval sepval = kno_get(slotmap,separator_slotid,VOID);
  if (!(STRINGP(sepval))) {
    return kno_err(NoMultiPartSeparator,"kno_parse_mime",NULL,slotmap);}
  kno_handle_compound_mime_field(slotmap,content_disposition_slotid,VOID);
  charenc = kno_get(slotmap,charset_slotid,VOID);
  dataenc = kno_get(slotmap,encoding_slotid,VOID);
  boundary = u8_malloc(STRLEN(sepval)+5);
  strcpy(boundary,"\r\n--"); strcat(boundary,CSTRING(sepval));
  boundary_len = STRLEN(sepval)+4;
  start = scan; scan = find_boundary(boundary,start,end-start,boundary_len,1);
  if (scan == NULL) {
    kno_store(slotmap,preamble_slotid,
             convert_content(start,scan,majtype,dataenc,charenc));
    kno_store(slotmap,parts_slotid,NIL);}
  else {
    lispval *point = &parts;
    if (scan>start)
      kno_store(slotmap,preamble_slotid,
               convert_content(scan,end,majtype,dataenc,charenc));
    start = scan+boundary_len;
    while (start<end) {
      lispval new_pair;
      if (strncmp(start,"--",2)==0) break;
      /* Ignore the opening CRLF of the encapsulation */
      else if ((start[0]=='\r')&&(start[1]=='\n'))
        start = start+2;
      else if (start[0]=='\n')
        start = start+1;
      /* Find the end of the encapsluation */
      scan = find_boundary(boundary,start,end-start,boundary_len,0);
      if (scan)
        new_pair = kno_conspair(kno_parse_mime(start,scan),NIL);
      else new_pair=
             kno_conspair(kno_parse_mime(start,end),NIL);
      *point = new_pair; point = &(KNO_CDR(new_pair));
      if (scan == NULL)  break;
      else start = scan+boundary_len;}
    kno_store(slotmap,parts_slotid,parts);}
  return parts;
}

KNO_EXPORT
lispval kno_parse_mime(const char *start,const char *end)
{
  lispval slotmap = kno_empty_slotmap();
  const char *scan = parse_headers(slotmap,start,end);
  lispval majtype = kno_handle_compound_mime_field
    (slotmap,content_type_slotid,VOID);
  lispval charenc, dataenc;
  kno_handle_compound_mime_field(slotmap,content_disposition_slotid,VOID);
  if (KNO_ABORTP(majtype))
    return majtype;
  charenc = kno_get(slotmap,charset_slotid,VOID);
  dataenc = kno_get(slotmap,encoding_slotid,VOID);
  if (kno_test(slotmap,content_type_slotid,multipart_symbol)) {
    lispval parts = kno_parse_multipart_mime(slotmap,scan,end);
    kno_decref(parts); return slotmap;}
  else {
    lispval content = convert_content(scan,end,majtype,dataenc,charenc);
    kno_store(slotmap,content_slotid,content);
    kno_decref(content);}
  kno_decref(charenc); kno_decref(dataenc);
  return slotmap;
}

static lispval parse_mime_data(lispval arg)
{
  if (PACKETP(arg))
    return kno_parse_mime(KNO_PACKET_DATA(arg),
                         KNO_PACKET_DATA(arg)+KNO_PACKET_LENGTH(arg));
  else if (STRINGP(arg))
    return kno_parse_mime(CSTRING(arg),
                         CSTRING(arg)+STRLEN(arg));
  else return kno_type_error(_("mime data"),"parse_mime_data",arg);
}


/* Module initialization */

void kno_init_mime_c()
{
  lispval module = kno_new_module("WEBTOOLS",(KNO_MODULE_SAFE));
  kno_idefn(module,kno_make_cprim1("PARSE-MIME",parse_mime_data,1));

  content_slotid = kno_intern("content");
  charset_slotid = kno_intern("charset");
  encoding_slotid = kno_intern("content-transfer-encoding");
  content_type_slotid = kno_intern("content-type");
  content_disposition_slotid = kno_intern("content-disposition");
  separator_slotid = kno_intern("boundary");
  multipart_symbol = kno_intern("multipart");
  headers_slotid = kno_intern("headers");
  preamble_slotid = kno_intern("preamble");
  parts_slotid = kno_intern("parts");

  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
