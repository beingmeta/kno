/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
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
#include <libu8/stringfns.h>

static fdtype scheme_symbol, hostname_symbol, portno_symbol, userinfo_symbol;
static fdtype name_symbol, path_symbol, pathstring_symbol;
static fdtype query_symbol, fragment_symbol;

/* Inheritance */

static void assign_substring
  (fdtype frame,fdtype slotid,u8_string start,u8_string end)
{
  fdtype value=fd_extract_string(NULL,start,end);
  fd_store(frame,slotid,value);
  fd_decref(value);
}

static u8_string breakup_path(fdtype f,u8_string start)
{
  fdtype path=FD_EMPTY_LIST, *tail=&path, name;
  u8_string scan=start; int c=u8_sgetc(&scan);
  U8_OUTPUT eltout; U8_INIT_OUTPUT(&eltout,128);
  while (1)
    if (c=='%') {
      int inschar;
      char buf[3]; buf[0]=*scan++; buf[1]=*scan++; buf[2]='\0';
      inschar=strtol(buf,NULL,16);
      u8_putc(&eltout,inschar);
      c=u8_sgetc(&scan);}
    else if ((c<0) || (c=='?') || (c=='#') || (c=='/') || (c==';')) {
      fdtype eltstring=fd_extract_string(NULL,eltout.u8_outbuf,eltout.u8_outptr);
      if (c=='/') {
	fdtype eltpair=fd_init_pair(NULL,eltstring,FD_EMPTY_LIST);
	*tail=eltpair; tail=&(FD_CDR(eltpair));
	c=u8_sgetc(&scan); eltout.u8_outptr=eltout.u8_outbuf; continue;}
      else {
	fd_add(f,name_symbol,eltstring); fd_decref(eltstring);}
      fd_add(f,path_symbol,path);
      fd_decref(path);
      u8_close(&eltout); 
      if (c<0) return scan; else return scan-1;}
    else {
      u8_putc(&eltout,c);
      c=u8_sgetc(&scan);}
}

static fdtype handle_path_end(fdtype f,u8_string start,u8_string path_end)
{
  assign_substring(f,pathstring_symbol,start,path_end);
  if (path_end==NULL) {}
  else if (*path_end=='?') {
    u8_string qstart=path_end+1, hashmark=strchr(qstart,'#');
    assign_substring(f,query_symbol,qstart,hashmark);
    if (hashmark) assign_substring(f,fragment_symbol,hashmark+1,NULL);}
  else if (*path_end=='#')
    assign_substring(f,fragment_symbol,path_end+1,NULL);
}

/* Merging URIs */

static int simple_inherit(fdtype slotid,fdtype to,fdtype from)
{
  fdtype value=fd_get(to,slotid,FD_EMPTY_CHOICE);
  if (FD_ABORTP(value)) return fd_interr(value);
  else if (!(FD_EMPTY_CHOICEP(value))) {
    fd_decref(value); return 0;}
  else {
    fdtype inherited=fd_get(from,slotid,FD_EMPTY_CHOICE);
    if (FD_ABORTP(inherited))
      return fd_interr(inherited);
    else if (FD_EMPTY_CHOICEP(inherited)) return 0;
    else if (fd_store(to,slotid,inherited)<0) {
      fd_decref(inherited); return -1;}
    else {
      fd_decref(inherited); return 1;}}
}

static int uri_merge(fdtype uri,fdtype base)
{
  fdtype path=fd_get(uri,path_symbol,FD_EMPTY_CHOICE);
  if (simple_inherit(scheme_symbol,uri,base)<0) return -1;
  if (simple_inherit(userinfo_symbol,uri,base)<0) return -1;
  if (simple_inherit(hostname_symbol,uri,base)<0) return -1;
  if (simple_inherit(portno_symbol,uri,base)<0) return -1;
  if (FD_EMPTY_CHOICEP(path)) {
    fdtype base_path=fd_get(base,path_symbol,FD_EMPTY_CHOICE), pathstring;
    if (FD_ABORTP(base_path)) return base_path;
    else pathstring=fd_get(uri,pathstring_symbol,FD_EMPTY_CHOICE);
    if (FD_ABORTP(pathstring)) {
      fd_decref(base_path); return pathstring;}
    if ((FD_EXISTSP(base_path)) && (FD_STRINGP(pathstring))) {
      u8_string path_end;
      U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
      {FD_DOLIST(elt,base_path)
	 if (FD_STRINGP(elt))
	   u8_printf(&out,"%s/",FD_STRDATA(elt));}
      u8_puts(&out,FD_STRDATA(pathstring));
      path_end=breakup_path(uri,out.u8_outbuf);
      handle_path_end(uri,out.u8_outbuf,path_end);
      u8_free(out.u8_outbuf);
      return 1;}}
  else return 0;
}

/* Parsing URIs */

static int guess_portno(char *string,int n)
{
  /* We could do getservbyname, but let's not. */
  if (*string=='\0') return 80;
  else if (strncmp(string,"http",n)==0) return 80;
  else if (strncmp(string,"https",n)==0) return 443;
  else if (strncmp(string,"gopher",n)==0) return 70;
  else if (strncmp(string,"news",n)==0) return 119;
  else return -1;
}

FD_EXPORT
fdtype fd_parse_uri(u8_string uri,fdtype base)
{
  fdtype f=fd_init_slotmap(NULL,0,NULL,NULL);
  u8_string start=uri, colon, slash, path_end;
  int default_portno;
  colon=strchr(start,':'); slash=strchr(uri,'/');
  if ((colon) && ((slash==NULL) || (colon<slash)))  {
    default_portno=guess_portno(start,colon-start);
    assign_substring(f,scheme_symbol,start,colon);
    start=colon+1;}
  if (default_portno<0) {
    fd_add(f,pathstring_symbol,fdtype_string(start));
    return f;}
  if ((start[0]=='/') && (start[1]=='/')) {
    u8_string atsign=strchr(start,'@');
    u8_string colon=strchr(start,':');
    u8_string slash=strchr(start+2,'/');
    start=start+2;
    if (atsign>slash) atsign=NULL;
    if ((atsign) && (colon) && (colon<atsign))
      colon=strchr(atsign,':');
    if (colon>slash) colon=NULL;
    if (atsign) {
      assign_substring(f,userinfo_symbol,start,atsign);
      start=atsign+2;}
    if (colon)
      assign_substring(f,hostname_symbol,start,colon);
    else assign_substring(f,hostname_symbol,start,slash);
    if ((colon) && (slash) && (slash>colon) && (slash-colon<64))  {
      char buf[64]; long int portno;
      strncpy(buf,colon+1,slash-colon); buf[slash-colon]='\0';
      portno=strtol(buf,NULL,10);
      fd_add(f,portno_symbol,FD_INT2DTYPE(portno));}
    else fd_add(f,portno_symbol,FD_INT2DTYPE(default_portno));
    if (slash) start=slash+1; else start=NULL;}
  if (start==NULL) 
    return f;
  else if ((start==uri) && (FD_TABLEP(base))) {
    assign_substring(f,pathstring_symbol,start,NULL);
    uri_merge(f,base);}
  else {
    path_end=breakup_path(f,start);
    if (*path_end) handle_path_end(f,start,path_end);}
  return f;
}

static fdtype parseuri(fdtype uri,fdtype base)
{
  return fd_parse_uri(FD_STRDATA(uri),base);
}

static fdtype mergeuris(fdtype uri,fdtype base)
{
  if (!(FD_TABLEP(uri)))
    return fd_type_error(_("table"),"mergeuris",uri);
  else if (!(FD_TABLEP(uri)))
    return fd_type_error(_("table"),"mergeuris",base);
  else {
    fdtype copy=fd_deep_copy(uri);
    if (uri_merge(copy,base)<0) {
      fd_decref(copy); return fd_erreify();}
    else return copy;}
}

static void uri_output(u8_output out,u8_string s,char *escape)
{
  while (*s)
    if ((*s>=0x80) || (*s=='+') || (*s=='%') || (strchr(escape,*s))) {
      char buf[8]; sprintf(buf,"%%%02x",*s);
      u8_puts(out,buf); s++;}
    else if (*s==' ') {u8_putc(out,'+'); s++;}
    else {u8_putc(out,*s); s++;}
}

#define URI_ESCAPES "?#/'=<>:;&"

static fdtype unparseuri(fdtype uri,fdtype noencode)
{
  int do_uriencode=1;
  if (FD_VOIDP(noencode)) {}
  else if (FD_TRUEP(noencode)) do_uriencode=0;
  if (FD_TABLEP(uri)) {
    struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,128);
    fdtype scheme=fd_get(uri,scheme_symbol,FD_EMPTY_CHOICE);
    fdtype userinfo=fd_get(uri,userinfo_symbol,FD_EMPTY_CHOICE);
    fdtype hostname=fd_get(uri,hostname_symbol,FD_EMPTY_CHOICE);
    fdtype port=fd_get(uri,portno_symbol,FD_EMPTY_CHOICE);
    fdtype pathstring=fd_get(uri,pathstring_symbol,FD_EMPTY_CHOICE);
    fdtype path=fd_get(uri,path_symbol,FD_EMPTY_CHOICE);
    fdtype name=fd_get(uri,name_symbol,FD_EMPTY_CHOICE);
    fdtype query=fd_get(uri,query_symbol,FD_EMPTY_CHOICE);
    fdtype fragment=fd_get(uri,query_symbol,FD_EMPTY_CHOICE);
    if (FD_STRINGP(scheme))
      u8_printf(&out,"%s:",FD_STRDATA(scheme));
    else u8_puts(&out,"http:");
    if (FD_STRINGP(hostname)) {
      if (FD_STRINGP(userinfo))
	u8_printf(&out,"//%s@%s",FD_STRDATA(userinfo),FD_STRDATA(hostname));
      else u8_printf(&out,"//%s",FD_STRDATA(hostname));
      if (FD_FIXNUMP(port))
	if (((FD_FIX2INT(port))==80) &&
	    ((!(FD_STRINGP(scheme))) ||
	     (strcmp(FD_STRING_DATA(scheme),"http")==0)))
	  u8_printf(&out,"/");
	else u8_printf(&out,":%d/",FD_FIX2INT(port));
      else u8_printf(&out,"/");}
    if (FD_PAIRP(path)) {
      FD_DOLIST(elt,path) {
	if (FD_STRINGP(elt)) {
	  if (do_uriencode)
	    uri_output(&out,FD_STRDATA(elt),URI_ESCAPES);
	  else u8_puts(&out,FD_STRDATA(elt));
	  u8_putc(&out,'/');}}}
    else if (FD_STRINGP(pathstring))
      u8_puts(&out,FD_STRDATA(pathstring));
    else {}
    if (FD_STRINGP(name))
      if (do_uriencode)
	uri_output(&out,FD_STRDATA(name),URI_ESCAPES);
      else u8_puts(&out,FD_STRDATA(name));
    if (FD_STRINGP(query)) {
      u8_putc(&out,'?');
      if (do_uriencode)
	uri_output(&out,FD_STRDATA(query),URI_ESCAPES);
      else u8_puts(&out,FD_STRDATA(query));}
    if (FD_STRINGP(fragment)) {
      u8_putc(&out,'#');
      if (do_uriencode)
	uri_output(&out,FD_STRDATA(fragment),URI_ESCAPES);
      else u8_puts(&out,FD_STRDATA(fragment));}
    fd_decref(scheme);
    fd_decref(userinfo); fd_decref(hostname); fd_decref(port);
    fd_decref(pathstring);  fd_decref(path); fd_decref(name);
    fd_decref(fragment); fd_decref(query);
    return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);}
  else return fd_type_error(_("table"),"unparseuri",uri);
}

FD_EXPORT void fd_uri_output(u8_output out,u8_string uri,char *escape)
{
  uri_output(out,uri,escape);
}

FD_EXPORT void fd_init_urifns_c()
{
  fdtype module=fd_new_module("FDWEB",(FD_MODULE_DEFAULT));

  scheme_symbol=fd_intern("SCHEME");
  userinfo_symbol=fd_intern("USERINFO");
  hostname_symbol=fd_intern("HOSTNAME");
  portno_symbol=fd_intern("PORT");
  name_symbol=fd_intern("NAME");
  path_symbol=fd_intern("PATH");
  pathstring_symbol=fd_intern("PATHSTRING");
  query_symbol=fd_intern("QUERY");
  fragment_symbol=fd_intern("FRAGMENT");

  fd_idefn(module,fd_make_cprim2x("PARSEURI",parseuri,1,
				  fd_string_type,FD_VOID,
				  -1,FD_VOID));
  fd_idefn(module,fd_make_cprim2("MERGEURIS",mergeuris,2));
  fd_idefn(module,fd_make_cprim2("UNPARSEURI",unparseuri,1));

  fd_register_source_file(versionid);
}

