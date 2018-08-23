/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
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
#include <libu8/u8stringfns.h>
#include <libu8/u8pathfns.h>

#include <ctype.h>

static lispval scheme_symbol, hostname_symbol, portno_symbol, userinfo_symbol;
static lispval name_symbol, path_symbol, pathstring_symbol;
static lispval query_symbol, fragment_symbol, colonize_symbol;

/* Inheritance */

static void assign_substring
  (lispval frame,lispval slotid,u8_string start,u8_string end)
{
  lispval value = fd_substring(start,end);
  fd_store(frame,slotid,value);
  fd_decref(value);
}

static u8_string breakup_path(lispval f,u8_string start)
{
  lispval path = NIL, *tail = &path;
  u8_string scan = start; int c = u8_sgetc(&scan);
  U8_OUTPUT eltout; U8_INIT_OUTPUT(&eltout,128);
  while (1)
    if (c=='%') {
      int inschar;
      char buf[3]; buf[0]= *scan++; buf[1]= *scan++; buf[2]='\0';
      inschar = strtol(buf,NULL,16);
      u8_putc(&eltout,inschar);
      c = u8_sgetc(&scan);}
    else if ((c<0) || (c=='?') || (c=='#') || (c=='/') || (c==';')) {
      lispval eltstring = fd_substring(eltout.u8_outbuf,eltout.u8_write);
      if (c=='/') {
        lispval eltpair = fd_conspair(eltstring,NIL);
        *tail = eltpair; tail = &(FD_CDR(eltpair));
        c = u8_sgetc(&scan); eltout.u8_write = eltout.u8_outbuf; continue;}
      else {
        fd_add(f,name_symbol,eltstring); fd_decref(eltstring);}
      fd_add(f,path_symbol,path);
      fd_decref(path);
      u8_close((u8_stream)&eltout);
      if (c<0) return scan; else return scan-1;}
    else {
      u8_putc(&eltout,c);
      c = u8_sgetc(&scan);}
}

static void handle_path_end(lispval f,u8_string start,u8_string path_end)
{
  assign_substring(f,pathstring_symbol,start,path_end);
  if (path_end == NULL) {}
  else if (*path_end=='?') {
    u8_string qstart = path_end+1, hashmark = strchr(qstart,'#');
    assign_substring(f,query_symbol,qstart,hashmark);
    if (hashmark) assign_substring(f,fragment_symbol,hashmark+1,NULL);}
  else if (*path_end=='#')
    assign_substring(f,fragment_symbol,path_end+1,NULL);
}

/* Merging URIs */

static int simple_inherit(lispval slotid,lispval to,lispval from)
{
  lispval value = fd_get(to,slotid,EMPTY);
  if (FD_ABORTP(value)) return fd_interr(value);
  else if (!(EMPTYP(value))) {
    fd_decref(value); return 0;}
  else {
    lispval inherited = fd_get(from,slotid,EMPTY);
    if (FD_ABORTP(inherited))
      return fd_interr(inherited);
    else if (EMPTYP(inherited)) return 0;
    else if (fd_store(to,slotid,inherited)<0) {
      fd_decref(inherited); return -1;}
    else {
      fd_decref(inherited); return 1;}}
}

static int uri_merge(lispval uri,lispval base)
{
  lispval path = fd_get(uri,path_symbol,EMPTY);
  if (simple_inherit(scheme_symbol,uri,base)<0) return -1;
  if (simple_inherit(userinfo_symbol,uri,base)<0) return -1;
  if (simple_inherit(hostname_symbol,uri,base)<0) return -1;
  if (simple_inherit(portno_symbol,uri,base)<0) return -1;
  if (EMPTYP(path)) {
    lispval base_path = fd_get(base,path_symbol,EMPTY), pathstring;
    if (FD_ABORTP(base_path)) return base_path;
    else pathstring = fd_get(uri,pathstring_symbol,EMPTY);
    if (FD_ABORTP(pathstring)) {
      fd_decref(base_path); return pathstring;}
    if ((FD_EXISTSP(base_path)) && (STRINGP(pathstring))) {
      u8_string path_end;
      U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
      {FD_DOLIST(elt,base_path)
         if (STRINGP(elt))
           u8_printf(&out,"%s/",CSTRING(elt));}
      u8_puts(&out,CSTRING(pathstring));
      path_end = breakup_path(uri,out.u8_outbuf);
      handle_path_end(uri,out.u8_outbuf,path_end);
      u8_free(out.u8_outbuf);
      return 1;}
    else return 0;}
  else return 0;
}

/* Parsing URIs */

static int guess_portno(u8_string string,int n)
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
lispval fd_parse_uri(u8_string uri,lispval base)
{
  lispval f = fd_empty_slotmap();
  u8_string start = uri, colon, slash, path_end;
  int default_portno = 80;
  colon = strchr(start,':'); slash = strchr(uri,'/');
  if ((colon) && ((slash == NULL) || (colon<slash)))  {
    default_portno = guess_portno(start,colon-start);
    assign_substring(f,scheme_symbol,start,colon);
    start = colon+1;}
  if (default_portno<0) {
    fd_add(f,pathstring_symbol,lispval_string(start));
    return f;}
  if ((start[0]=='/') && (start[1]=='/')) {
    u8_string atsign = strchr(start,'@');
    u8_string colon = strchr(start,':');
    u8_string slash = strchr(start+2,'/');
    start = start+2;
    if (atsign>slash) atsign = NULL;
    if ((atsign) && (colon) && (colon<atsign))
      colon = strchr(atsign,':');
    if (colon>slash) colon = NULL;
    if (atsign) {
      assign_substring(f,userinfo_symbol,start,atsign);
      start = atsign+2;}
    if (colon)
      assign_substring(f,hostname_symbol,start,colon);
    else assign_substring(f,hostname_symbol,start,slash);
    if ((colon) && (slash) && (slash>colon) && (slash-colon<64))  {
      char buf[64]; long int portno;
      strncpy(buf,colon+1,slash-colon); buf[slash-colon]='\0';
      portno = strtol(buf,NULL,10);
      fd_add(f,portno_symbol,FD_INT(portno));}
    else fd_add(f,portno_symbol,FD_INT(default_portno));
    if (slash) start = slash+1; else start = NULL;}
  if (start == NULL)
    return f;
  else if ((start == uri) && (TABLEP(base))) {
    assign_substring(f,pathstring_symbol,start,NULL);
    uri_merge(f,base);}
  else {
    path_end = breakup_path(f,start);
    if (*path_end) handle_path_end(f,start,path_end);}
  return f;
}

static lispval parseuri(lispval uri,lispval base)
{
  return fd_parse_uri(CSTRING(uri),base);
}

static lispval mergeuris(lispval uri,lispval base)
{
  if (!(TABLEP(uri)))
    return fd_type_error(_("table"),"mergeuris",uri);
  else if (!(TABLEP(uri)))
    return fd_type_error(_("table"),"mergeuris",base);
  else {
    lispval copy = fd_deep_copy(uri);
    if (uri_merge(copy,base)<0) {
      fd_decref(copy); return FD_ERROR;}
    else return copy;}
}

static void uri_output(u8_output out,u8_string s,int len,int upper,
                       const char *escape)
{
  u8_string lim = ((len<0)?(NULL):(s+len));
  while ((lim)?(s<lim):(*s))
    if (((*s)>=0x80)||(isspace(*s))||
        (*s=='+')||(*s=='%')||(*s=='=')||
        (*s=='&')||(*s=='#')||(*s==';')||
        ((escape == NULL)?
         (!((isalnum(*s))||(strchr("-_.~",*s)!=NULL))):
         ((strchr(escape,*s))!=NULL))) {
      char buf[8];
      if (upper) sprintf(buf,"%%%02X",*s);
      else sprintf(buf,"%%%02x",*s);
      u8_puts(out,buf); s++;}
    else {u8_putc(out,*s); s++;}
}

#define URI_ESCAPES "?#/'=<>:;&"

static lispval unparseuri(lispval uri,lispval noencode)
{
  u8_string escapes = URI_ESCAPES; int upper = 0;
  if (VOIDP(noencode)) {}
  else if (FD_TRUEP(noencode)) escapes = NULL;
  else if (STRINGP(noencode)) {
    u8_string data = CSTRING(noencode);
    int len = STRLEN(noencode);
    int c = data[0];
    if ((c<0x80)&&(isalpha(c))) {
      upper = isupper(c);
      if (len>1) escapes = data+1;}
    else escapes = data;}
  if (TABLEP(uri)) {
    struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,128);
    lispval scheme = fd_get(uri,scheme_symbol,EMPTY);
    lispval userinfo = fd_get(uri,userinfo_symbol,EMPTY);
    lispval hostname = fd_get(uri,hostname_symbol,EMPTY);
    lispval port = fd_get(uri,portno_symbol,EMPTY);
    lispval pathstring = fd_get(uri,pathstring_symbol,EMPTY);
    lispval path = fd_get(uri,path_symbol,EMPTY);
    lispval name = fd_get(uri,name_symbol,EMPTY);
    lispval query = fd_get(uri,query_symbol,EMPTY);
    lispval fragment = fd_get(uri,query_symbol,EMPTY);
    if (STRINGP(scheme))
      u8_printf(&out,"%s:",CSTRING(scheme));
    else u8_puts(&out,"http:");
    if (STRINGP(hostname)) {
      if (STRINGP(userinfo))
        u8_printf(&out,"//%s@%s",CSTRING(userinfo),CSTRING(hostname));
      else u8_printf(&out,"//%s",CSTRING(hostname));
      if (FD_UINTP(port))
        if (((FIX2INT(port))==80) &&
            ((!(STRINGP(scheme))) ||
             (strcmp(FD_STRING_DATA(scheme),"http")==0)))
          u8_printf(&out,"/");
        else u8_printf(&out,":%d/",FIX2INT(port));
      else u8_printf(&out,"/");}
    if (PAIRP(path)) {
      FD_DOLIST(elt,path) {
        if (STRINGP(elt)) {
          if (escapes)
            uri_output(&out,CSTRING(elt),STRLEN(elt),upper,escapes);
          else u8_puts(&out,CSTRING(elt));
          u8_putc(&out,'/');}}}
    else if (STRINGP(pathstring))
      u8_puts(&out,CSTRING(pathstring));
    else {}
    if (STRINGP(name))
      if (escapes)
        uri_output(&out,CSTRING(name),STRLEN(name),upper,escapes);
      else u8_puts(&out,CSTRING(name));
    else {}
    if (STRINGP(query)) {
      u8_putc(&out,'?');
      if (escapes)
        uri_output(&out,CSTRING(query),STRLEN(query),upper,escapes);
      else u8_puts(&out,CSTRING(query));}
    if (STRINGP(fragment)) {
      u8_putc(&out,'#');
      if (escapes)
        uri_output(&out,CSTRING(fragment),STRLEN(fragment),upper,escapes);
      else u8_puts(&out,CSTRING(fragment));}
    fd_decref(scheme);
    fd_decref(userinfo); fd_decref(hostname); fd_decref(port);
    fd_decref(pathstring);  fd_decref(path); fd_decref(name);
    fd_decref(fragment); fd_decref(query);
    return fd_stream2string(&out);}
  else return fd_type_error(_("table"),"unparseuri",uri);
}

static lispval urischeme_prim(lispval uri_arg)
{
  u8_string uri = CSTRING(uri_arg);
  u8_byte *scheme_end = strstr(uri,":");
  if (!(scheme_end)) return EMPTY;
  else return fd_substring(uri,scheme_end);
}

static lispval urihost_prim(lispval uri_arg)
{
  u8_string uri = CSTRING(uri_arg);
  u8_byte *host_start = strstr(uri,"//");
  if (!(host_start)) return EMPTY;
  else {
    u8_byte *host_end = strstr(host_start+2,"/");
    u8_byte *port_end = strstr(host_start+2,":");
    u8_byte *end = (((port_end) && (host_end) && (port_end<host_end)) ?
                  (port_end):(host_end));
    if (end) return fd_substring(host_start+2,host_end);
    else return EMPTY;}
}

static lispval urifrag_prim(lispval uri_arg)
{
  u8_string uri = CSTRING(uri_arg);
  u8_byte *qmark = strchr(uri,'?');
  u8_byte *hash = strchr(uri,'#');
  if ((hash)&&(qmark)&&(hash<qmark))
    return fd_substring(hash+1,qmark);
  else if (hash) return fd_substring(hash+1,NULL);
  else return EMPTY;
}

static lispval uriquery_prim(lispval uri_arg)
{
  u8_string uri = CSTRING(uri_arg);
  u8_byte *qmark = strchr(uri,'?');
  u8_byte *hash = strchr(uri,'#');
  if ((hash)&&(qmark)&&(qmark<hash))
    return fd_substring(qmark+1,hash);
  else if (qmark) return fd_substring(qmark+1,NULL);
  else return EMPTY;
}

static lispval uribase_prim(lispval uri_arg)
{
  u8_string uri = CSTRING(uri_arg);
  u8_byte *hash = strchr(uri,'#');
  u8_byte *qmark = strchr(uri,'?');
  if ((hash) && (qmark))
    if (hash<qmark)
      return fd_substring(uri,hash);
    else return fd_substring(uri,qmark);
  else if (hash)
    return fd_substring(uri,hash);
  else if (qmark)
    return fd_substring(uri,qmark);
  else return fd_incref(uri_arg);
}

static lispval uripath_prim(lispval uri_arg)
{
  u8_string uri = CSTRING(uri_arg);
  u8_string hash = strchr(uri,'#');
  u8_string qmark = strchr(uri,'?');
  u8_string pathstart = strchr(uri,'/');
  while ((pathstart)&&(pathstart[1]=='/'))
    pathstart = strchr(pathstart+2,'/');
  if (!(pathstart)) pathstart = uri;
  else pathstart++;
  if ((hash) && (qmark))
    if (hash<qmark)
      return fd_substring(pathstart,hash);
    else return fd_substring(pathstart,qmark);
  else if (hash)
    return fd_substring(pathstart,hash);
  else if (qmark)
    return fd_substring(pathstart,qmark);
  else if (pathstart == uri) return fd_incref(uri_arg);
  else return fd_substring(pathstart,uri+STRLEN(uri_arg));
}

FD_EXPORT void fd_uri_output(u8_output out,u8_string uri,int len,int upper,
                             const char *escape)
{
  uri_output(out,uri,len,upper,escape);
}

/* Making URI paths */

static lispval mkuripath_prim(lispval dirname,lispval name)
{
  lispval config_val = VOID; u8_string dir = NULL, namestring = NULL;
  char buf[128];
  if (!(STRINGP(name)))
    return fd_type_error(_("string"),"mkuripath_prim",name);
  else namestring = CSTRING(name);
  if (STRINGP(dirname)) dir = CSTRING(dirname);
  else if (SYMBOLP(dirname)) {
    config_val = fd_config_get(SYM_NAME(dirname));
    if (STRINGP(config_val)) dir = CSTRING(config_val);
    else {
      fd_decref(config_val);
      return fd_type_error(_("string CONFIG var"),"mkuripath_prim",dirname);}}
  else return fd_type_error
         (_("string or string CONFIG var"),"mkuripath_prim",dirname);
  if (*namestring=='/') {
    int host_len;
    u8_byte *host_start = strchr(dir,'/');
    u8_byte *host_end = ((host_start == NULL)?(host_start):
                       ((u8_byte*)strchr(host_start+2,'/')));
    /* Check that the URL host isn't insanely long. */
    if ((host_end-dir)>127) host_end = NULL;
    else host_len = host_end-dir;
    if (!(host_end)) {
      fd_decref(config_val);
      return fd_type_error(_("URI or URI CONFIG var"),"mkuripath_prim",dirname);}
    else {
      memcpy(buf,dir,host_len); buf[host_len]='\0'; dir = buf;}}
  if (VOIDP(config_val))
    return fd_lispstring(u8_mkpath(dir,namestring));
  else {
    lispval result = fd_lispstring(u8_mkpath(dir,namestring));
    fd_decref(config_val);
    return result;}
}

/* Making data URIs */

static lispval datauri_prim(lispval data,lispval ctype_arg)
{
  u8_string ctype = ((STRINGP(ctype_arg))?(CSTRING(ctype_arg)):((u8_string)NULL));
  u8_string base64; int data_len, uri_len;
  lispval result; struct FD_STRING *string; u8_byte *write;
  if (STRINGP(data))
    base64 = u8_write_base64(CSTRING(data),STRLEN(data),&data_len);
  else if (PACKETP(data))
    base64 = u8_write_base64(FD_PACKET_DATA(data),FD_PACKET_LENGTH(data),&data_len);
  else return fd_type_error("String or packet","datauri_prim",data);
  uri_len = 5+((ctype)?(STRLEN(ctype_arg)):(10))+
    ((STRINGP(data))?(13):(0))+8+data_len+1;
  result = fd_make_string(NULL,uri_len,NULL);
  string = fd_consptr(struct FD_STRING *,result,fd_string_type);
  write = (u8_byte *)CSTRING(result);
  if ((ctype)&&(STRINGP(data)))
    sprintf(write,"data:%s;charset = UTF-8;base64,",ctype);
  else if (ctype)
    sprintf(write,"data:%s;base64,",ctype);
  else if (STRINGP(data))
    sprintf(write,"data:text/plain;charset = UTF-8;base64,");
  else sprintf(write,"data:;base64,");
  string->str_bytelen = strlen(write)+data_len;
  strcat(write,base64); u8_free(base64);
  return result;
}

/* URI encoding, etc */

static lispval oid2id(lispval oid,lispval prefix)
{
  U8_OUTPUT tmp; U8_INIT_OUTPUT(&tmp,32);
  if (VOIDP(prefix))
    u8_printf(&tmp,":@%x/%x",
              FD_OID_HI(FD_OID_ADDR(oid)),
              FD_OID_LO(FD_OID_ADDR(oid)));
  else if (SYMBOLP(prefix))
    u8_printf(&tmp,"%s_%x_%x",
              SYM_NAME(prefix),
              FD_OID_HI(FD_OID_ADDR(oid)),
              FD_OID_LO(FD_OID_ADDR(oid)));
  else if (STRINGP(prefix))
    u8_printf(&tmp,"%s_%x_%x",
              CSTRING(prefix),
              FD_OID_HI(FD_OID_ADDR(oid)),
              FD_OID_LO(FD_OID_ADDR(oid)));
  else {
    u8_free(tmp.u8_outbuf);
    return fd_type_error("string","oid2id",prefix);}
  return fd_init_string(NULL,tmp.u8_write-tmp.u8_outbuf,tmp.u8_outbuf);
}

static int add_query_param(u8_output out,lispval name,lispval value,int nocolon)
{
  int lastc = -1, free_varname = 0, do_encode = 1, keep_secret = 0;
  u8_string varname; u8_byte namebuf[256];
  if ( out->u8_write > out->u8_outbuf )
    lastc = out->u8_write[-1];
  if (STRINGP(name)) varname = CSTRING(name);
  else if (SYMBOLP(name)) varname = SYM_NAME(name);
  else if (OIDP(name)) {
    FD_OID addr = FD_OID_ADDR(name);
    sprintf(namebuf,":@%x/%x",FD_OID_HI(addr),FD_OID_LO(addr));
    varname = namebuf;}
  else if (TYPEP(name,fd_secret_type)) {
    varname = FD_PACKET_DATA(name);
    keep_secret = 1;}
  else {
    varname = fd_lisp2string(name);
    free_varname = 1;}
  {DO_CHOICES(val,value) {
      if (lastc<0) {}
      else if (lastc=='?') {}
      else if (lastc=='&') {}
      else u8_putc(out,'&');
      fd_uri_output(out,varname,-1,0,NULL);
      u8_putc(out,'=');
      if (STRINGP(val))
        if (do_encode)
          fd_uri_output(out,CSTRING(val),STRLEN(val),0,NULL);
        else u8_puts(out,CSTRING(val));
      else if (TYPEP(val,fd_secret_type)) {
        if (do_encode)
          fd_uri_output(out,FD_PACKET_DATA(val),FD_PACKET_LENGTH(val),0,NULL);
        else u8_puts(out,FD_PACKET_DATA(val));
        keep_secret = 1;}
      else if (PACKETP(val))
        fd_uri_output(out,FD_PACKET_DATA(val),FD_PACKET_LENGTH(val),0,NULL);
      else if (OIDP(val)) {
        FD_OID addr = FD_OID_ADDR(val);
        u8_printf(out,":@%x/%x",FD_OID_HI(addr),FD_OID_LO(addr));}
      else {
        if (!(nocolon)) u8_putc(out,':');
        if (SYMBOLP(val))
          fd_uri_output(out,SYM_NAME(val),-1,0,NULL);
        else {
          u8_string as_string = fd_lisp2string(val);
          fd_uri_output(out,as_string,-1,0,NULL);
          u8_free(as_string);}}
      lastc = -1;}}
  if (free_varname) u8_free(varname);
  return keep_secret;
}

/* URI encoding */

static lispval uriencode_prim(lispval string,lispval escape,lispval uparg)
{
  u8_string input; int free_input = 0;
  int upper = (!(FALSEP(uparg)));
  struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
  if (STRINGP(string)) input = CSTRING(string);
  else if (SYMBOLP(string)) input = SYM_NAME(string);
  else if (PACKETP(string)) {
    int len = FD_PACKET_LENGTH(string);
    u8_byte *buf = u8_malloc(len+1);
    memcpy(buf,FD_PACKET_DATA(string),len);
    buf[len]='\0';
    free_input = 1;
    input = buf;}
  else {
    input = fd_lisp2string(string);
    free_input = 1;}
  if (VOIDP(escape))
    fd_uri_output(&out,input,-1,upper,NULL);
  else fd_uri_output(&out,input,-1,upper,CSTRING(escape));
  if (free_input) u8_free(input);
  if (STRINGP(string)) return fd_stream2string(&out);
  else if (TYPEP(string,fd_packet_type))
    return fd_init_packet(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);
  else return fd_stream2string(&out);
}

static lispval form_encode_prim(lispval table,lispval opts)
{
  int keep_secret=0;
  struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,2000);
  lispval colonize=fd_getopt(opts,colonize_symbol,FD_FALSE);
  int nocolon=(!(FALSEP(colonize)));
  lispval keys=fd_getkeys(table);
  DO_CHOICES(key,keys) {
    lispval value = fd_get(table,key,VOID);
    if (keep_secret)
      add_query_param(&out,key,value,nocolon);
    else keep_secret = add_query_param(&out,key,value,nocolon);
    fd_decref(value);}
  fd_decref(keys);
  fd_decref(colonize);
  if (keep_secret) {
    lispval result = fd_stream2string(&out);
    FD_SET_CONS_TYPE(result,fd_secret_type);
    return result;}
  else return fd_stream2string(&out);
}

/* URI decoding */

static int xdigit_weight(int c);

static lispval uridecode_prim(lispval string)
{
  int len = STRLEN(string), c;
  const u8_byte *scan = CSTRING(string), *limit = scan+len;
  u8_byte *result = result = u8_malloc(len+1), *write = result;
  while (scan<limit) {
    c = *(scan++);
    if (c=='%') {
      char digit1, digit2; unsigned char ec;
      c = *(scan++);
      if (!(isxdigit(c)))
        return fd_err("Invalid encoded URI","decodeuri_prim",NULL,string);
      digit1 = xdigit_weight(c);
      c = *(scan++);
      if (!(isxdigit(c)))
        return fd_err("Invalid encoded URI","decodeuri_prim",NULL,string);
      else digit2 = xdigit_weight(c);
      ec = (digit1)*16+digit2;
      *write++=ec;}
    /* else if (c=='+') *write++=' '; */
    else *write++=c;}
  *write='\0';
  return fd_init_string(NULL,write-result,result);
}

static int xdigit_weight(int c)
{
  if (isdigit(c)) return c-'0';
  else if (isupper(c)) return c-'A'+10;
  else return c-'a'+10;
}

/* Scripturl primitives */

static lispval scripturl_core(u8_string baseuri,lispval params,int n,
                             lispval *args,int nocolon,int keep_secret)
{
  struct U8_OUTPUT out;
  int i = 0, need_qmark = ((baseuri!=NULL)&&(strchr(baseuri,'?') == NULL));
  U8_INIT_OUTPUT(&out,64);
  if (baseuri) u8_puts(&out,baseuri);
  if (n == 1) {
    if (need_qmark) {u8_putc(&out,'?'); need_qmark = 0;}
    if (STRINGP(args[0]))
      fd_uri_output(&out,CSTRING(args[0]),STRLEN(args[0]),0,NULL);
    else if (TYPEP(args[0],fd_secret_type)) {
      fd_uri_output(&out,
                    FD_PACKET_DATA(args[0]),FD_PACKET_LENGTH(args[0]),
                    0,NULL);
      keep_secret = 1;}
    else if (OIDP(args[0])) {
      FD_OID addr = FD_OID_ADDR(args[0]);
      u8_printf(&out,":@%x/%x",FD_OID_HI(addr),FD_OID_LO(addr));}
    else u8_printf(&out,":%q",args[0]);
    return fd_stream2string(&out);}
  if (!((VOIDP(params))||(EMPTYP(params)))) {
    DO_CHOICES(table,params)
      if (TABLEP(table)) {
        lispval keys = fd_getkeys(table);
        DO_CHOICES(key,keys) {
          lispval value = fd_get(table,key,VOID);
          if (need_qmark) {u8_putc(&out,'?'); need_qmark = 0;}
          if (keep_secret)
            add_query_param(&out,key,value,nocolon);
          else keep_secret = add_query_param(&out,key,value,nocolon);
          fd_decref(value);}
        fd_decref(keys);}}
  while (i<n) {
    if (need_qmark) {u8_putc(&out,'?'); need_qmark = 0;}
    if (keep_secret)
      add_query_param(&out,args[i],args[i+1],nocolon);
    else keep_secret = add_query_param(&out,args[i],args[i+1],nocolon);
    i = i+2;}
  if (keep_secret) {
    lispval result = fd_stream2string(&out);
    FD_SET_CONS_TYPE(result,fd_secret_type);
    return result;}
  else return fd_stream2string(&out);
}

static lispval scripturl(int n,lispval *args)
{
  if (EMPTYP(args[0])) return EMPTY;
  else if (!((STRINGP(args[0]))||
             (FALSEP(args[0]))||
             (TYPEP(args[0],fd_secret_type))))
    return fd_err(fd_TypeError,"scripturl",
                  u8_strdup("script name or #f"),args[0]);
  else if ((n>2) && ((n%2)==0))
    return fd_err(fd_SyntaxError,"scripturl",
                  strdup("odd number of arguments"),VOID);
  else if (FALSEP(args[0]))
    return scripturl_core(NULL,VOID,n-1,args+1,1,0);
  else if (TYPEP(args[0],fd_secret_type))
    return scripturl_core(NULL,VOID,n-1,args+1,1,1);
  else return scripturl_core(CSTRING(args[0]),VOID,n-1,args+1,1,0);
}

static lispval fdscripturl(int n,lispval *args)
{
  if (EMPTYP(args[0])) return EMPTY;
  else if (!((STRINGP(args[0]))||
             (FALSEP(args[0]))||
             (TYPEP(args[0],fd_secret_type))))
    return fd_err(fd_TypeError,"fdscripturl",
                  u8_strdup("script name or #f"),args[0]);
  else if ((n>2) && ((n%2)==0))
    return fd_err(fd_SyntaxError,"fdscripturl",
                  u8dup("odd number of arguments"),VOID);
  else if (FALSEP(args[0]))
    return scripturl_core(NULL,VOID,n-1,args+1,0,0);
  else if (TYPEP(args[0],fd_secret_type))
    return scripturl_core(CSTRING(args[0]),VOID,n-1,args+1,0,1);
  else return scripturl_core(CSTRING(args[0]),VOID,n-1,args+1,0,0);
}

static lispval scripturlplus(int n,lispval *args)
{
  if (EMPTYP(args[0])) return EMPTY;
  else if (!((STRINGP(args[0]))||
             (FALSEP(args[0]))||
             (TYPEP(args[0],fd_secret_type))))
    return fd_err(fd_TypeError,"scripturlplus",
                  u8_strdup("script name or #f"),args[0]);
  else if ((n>2) && ((n%2)==1))
    return fd_err(fd_SyntaxError,"scripturlplus",
                  u8dup("odd number of arguments"),VOID);
  else if (FALSEP(args[0]))
    return scripturl_core(NULL,args[1],n-2,args+2,1,0);
  else if (TYPEP(args[0],fd_secret_type))
    return scripturl_core(CSTRING(args[0]),args[1],n-2,args+2,1,1);
  else return scripturl_core(CSTRING(args[0]),args[1],n-2,args+2,1,0);
}

static lispval fdscripturlplus(int n,lispval *args)
{
  if (EMPTYP(args[0])) return EMPTY;
  else if  (!((STRINGP(args[0]))||
             (FALSEP(args[0]))||
             (TYPEP(args[0],fd_secret_type))))
    return fd_err(fd_TypeError,"fdscripturlplus",
                  u8_strdup("script name"),args[0]);
  else if ((n>2) && ((n%2)==1))
    return fd_err(fd_SyntaxError,"fdscripturlplus",
                  u8dup("odd number of arguments"),VOID);
  else if (FALSEP(args[0]))
    return scripturl_core(NULL,args[1],n-2,args+2,0,0);
  else if (TYPEP(args[0],fd_secret_type))
    return scripturl_core(CSTRING(args[0]),args[1],n-2,args+2,0,1);
  else return scripturl_core(CSTRING(args[0]),args[1],n-2,args+2,0,0);
}


/* Module init */

FD_EXPORT void fd_init_urifns_c()
{
  lispval module = fd_new_module("FDWEB",(0));
  lispval safe_module = fd_new_module("FDWEB",(1));

  scheme_symbol = fd_intern("SCHEME");
  userinfo_symbol = fd_intern("USERINFO");
  hostname_symbol = fd_intern("HOSTNAME");
  portno_symbol = fd_intern("PORT");
  name_symbol = fd_intern("NAME");
  path_symbol = fd_intern("PATH");
  pathstring_symbol = fd_intern("PATHSTRING");
  query_symbol = fd_intern("QUERY");
  fragment_symbol = fd_intern("FRAGMENT");
  colonize_symbol = fd_intern("COLONIZE");

  fd_idefn(module,fd_make_cprim2x("PARSEURI",parseuri,1,
                                  fd_string_type,VOID,
                                  -1,VOID));
  fd_idefn(module,fd_make_cprim2("MERGEURIS",mergeuris,2));
  fd_idefn(module,fd_make_cprim2("UNPARSEURI",unparseuri,1));

  fd_idefn(module,fd_make_cprim1x("URISCHEME",urischeme_prim,1,
                                  fd_string_type,VOID));
  fd_idefn(module,fd_make_cprim1x("URIHOST",urihost_prim,1,
                                  fd_string_type,VOID));
  fd_idefn(module,fd_make_cprim1x("URIFRAG",urifrag_prim,1,
                                  fd_string_type,VOID));
  fd_idefn(module,fd_make_cprim1x("URIQUERY",uriquery_prim,1,
                                  fd_string_type,VOID));
  fd_idefn(module,fd_make_cprim1x("URIBASE",uribase_prim,1,
                                  fd_string_type,VOID));
  fd_idefn(module,fd_make_cprim1x("URIPATH",uripath_prim,1,
                                  fd_string_type,VOID));

  fd_idefn(module,fd_make_cprim2x("MKURIPATH",mkuripath_prim,2,
                                  -1,VOID,fd_string_type,VOID));

  fd_idefn(module,fd_make_cprim2x("DATAURI",datauri_prim,1,
                                  -1,VOID,fd_string_type,VOID));

  /* This is the non-deterministic version */
  lispval uriencode_proc=
    fd_new_cprim3("URIENCODE",_FILEINFO,
                  "(uriencode *val* [*chars*] [*upper*]) encodes a value "
                  "for use as a URI component (e.g. translating space "
                  "into '%20'.) "
                  "If *val* is a string, packet, or secret it is encoded "
                  "directly, otherwise it is unparsed into a string which "
                  "is then encoded. *chars*, if specified, indicates "
                  "additional characters to be escaped beyond (+%=&#;) "
                  "which are automatically escaped. If *upper* is not falsy, "
                  "generated hex escapes are generated with uppercase letters "
                  "(e.g. %A0 rather than %a0).",
                  uriencode_prim,1,1,0,
                  -1,VOID,
                  fd_string_type,VOID,
                  -1,VOID);
  lispval form_encode_proc=
    fd_new_cprim2("FORM->URISTRING",_FILEINFO,
                  "(FORM->URISTRING *form* [*opts*]) encodes a table as an "
                  "application/x-www-url-encoded string.",
                  form_encode_prim,1,0,0,
                  -1,VOID,-1,VOID);

  lispval uridecode_proc=
    fd_make_cprim1x("URIDECODE",uridecode_prim,1,
                    fd_string_type,VOID);

  lispval oid2id_proc=
    fd_make_cprim2x("OID2ID",oid2id,1,fd_oid_type,VOID,-1,VOID);
  lispval scripturl_proc=
    fd_make_ndprim(fd_make_cprimn("SCRIPTURL",scripturl,1));
  lispval fdscripturl_proc=
    fd_make_ndprim(fd_make_cprimn("FDSCRIPTURL",fdscripturl,2));
  lispval scripturlplus_proc=
    fd_make_ndprim(fd_make_cprimn("SCRIPTURL+",scripturlplus,1));
  lispval fdscripturlplus_proc=
    fd_make_ndprim(fd_make_cprimn("FDSCRIPTURL+",fdscripturlplus,2));

  fd_defn(module,uriencode_proc);
  fd_defn(module,uridecode_proc);
  fd_defn(module,form_encode_proc);

  fd_defn(module,oid2id_proc);
  fd_defn(module,scripturl_proc);
  fd_defn(module,scripturlplus_proc);
  fd_defn(module,fdscripturl_proc);
  fd_defn(module,fdscripturlplus_proc);

  fd_store(module,fd_intern("SCRIPTURL+"),scripturlplus_proc);

  fd_idefn(safe_module,form_encode_proc);
  fd_idefn(safe_module,uriencode_proc);
  fd_idefn(safe_module,uridecode_proc);

  fd_idefn(safe_module,oid2id_proc);
  fd_idefn(safe_module,scripturl_proc);
  fd_idefn(safe_module,scripturlplus_proc);
  fd_idefn(safe_module,fdscripturl_proc);
  fd_idefn(safe_module,fdscripturlplus_proc);

  fd_store(safe_module,fd_intern("SCRIPTURL+"),scripturlplus_proc);

  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
