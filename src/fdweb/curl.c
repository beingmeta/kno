/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "fdb/dtype.h"
#include "fdb/tables.h"
#include "fdb/eval.h"
#include "fdb/fdweb.h"

#include <libu8/libu8io.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8netfns.h>

#include <curl/curl.h>

static fdtype curl_defaults, url_symbol;
static fdtype content_type_symbol, charset_symbol, type_symbol;
static fdtype content_length_symbol, etag_symbol;
static fdtype text_symbol, content_symbol, header_symbol;
static fdtype referer_symbol, useragent_symbol, cookie_symbol;
static fdtype date_symbol, last_modified_symbol, name_symbol;
static fdtype cookiejar_symbol, authinfo_symbol, basicauth_symbol;

static fdtype text_types=FD_EMPTY_CHOICE;

static fd_exception NonTextualContent=
  _("can't parse non-textual content as XML");
static fd_exception CurlError=_("Internal libcurl error");

typedef struct FD_CURL_HANDLE {
  FD_CONS_HEADER;
  CURL *handle;
  fdtype strings;} FD_CURL_HANDLE;
typedef struct FD_CURL_HANDLE *fd_curl_handle;

FD_EXPORT struct FD_CURL_HANDLE *fd_open_curl_handle(void);

typedef struct DBUF {
  unsigned char *bytes;
  int size, limit;} DBUF;

static char *cookies_file=NULL;

#if ((FD_THREADS_ENABLED) && (FD_USE_TLS))
static u8_tld_key curl_threadkey;
static FD_CURL_HANDLE *get_curl_handle()
{
  fd_curl_handle result=u8_tld_get(curl_threadkey);
  if (result) return result;
  else {
    FD_CURL_HANDLE *curler=fd_open_curl_handle();
    if (curler) u8_tld_set(curl_threadkey,curler);
    return curler;}
}
static void free_threadlocal_curl(void *v)
{
  fdtype lv=(fdtype)v;
  fd_decref(lv);
}
#elif FD_USE__THREAD
static __thread struct FD_CURL_HANDLE *curl_handle;
#define get_curl_handle() \
  ((curl_handle==NULL) ? \
   (curl_handle=fd_open_curl_handle()) : \
   (curl_handle))
#else
static CURL *curl_handle;
#define get_curl_handle() \
  ((curl_handle==NULL) ? \
   (curl_handle=fd_open_curl_handle()) : \
   (curl_handle))
#endif

static size_t copy_content_data(char *data,size_t size,size_t n,void *vdbuf)
{
  DBUF *dbuf=(DBUF *)vdbuf;
  if (dbuf->size+size*n > dbuf->limit) {
    int new_limit; char *newptr;
    if (dbuf->limit >= 65536)
      new_limit=dbuf->limit+65536;
    else new_limit=dbuf->limit*2;
    newptr=u8_realloc(dbuf->bytes,new_limit);
    dbuf->bytes=newptr; dbuf->limit=new_limit;}
  memcpy(dbuf->bytes+dbuf->size,data,size*n);
  dbuf->size=dbuf->size+size*n;
  return size*n;
}

fd_ptr_type fd_curl_type;

void handle_content_type(char *value,fdtype table)
{
  char *chset, *chset_end; int endbyte;
  char *end=value, *slash=strchr(value,'/');
  fdtype major_type, full_type;
  while ((*end) && (!((*end==';') || (isspace(*end))))) end++;
  if (slash) *slash='\0';
  endbyte=*end; *end='\0';
  major_type=fd_parse(value);
  fd_add(table,type_symbol,major_type); *slash='/';
  full_type=fdtype_string(value);
  fd_add(table,type_symbol,full_type); *end=endbyte;
  fd_decref(major_type); fd_decref(full_type);
  if (chset=strstr(value,"charset=")) {
    fdtype chset_val;
    chset_end=chset=chset+8; if (*chset=='"') {
      chset++; chset_end=strchr(chset,'"');}
    else chset_end=strchr(chset,';');
    if (chset_end) *chset_end='\0';
    chset_val=fdtype_string(chset);
    fd_add(table,charset_symbol,chset_val);
    fd_decref(chset_val);}
}

static size_t handle_header(void *ptr,size_t size,size_t n,void *data)
{
  fdtype *valptr=(fdtype *)data, val=*valptr;
  fdtype slotid, hval; int byte_len=size*n;
  char *cdata=(char *)ptr, *copy=u8_malloc(byte_len+1), *valstart;
  strncpy(copy,cdata,byte_len); copy[byte_len]='\0';
  /* Strip off the CRLF */
  if ((copy[byte_len-1]='\n') && (copy[byte_len-2]='\r'))
    if (byte_len==2) {
      u8_free(copy); return byte_len;}
    else copy[byte_len-2]='\0';
  if (valstart=strchr(copy,':')) {
    *valstart++='\0'; while (isspace(*valstart)) valstart++;
    if (!(FD_TABLEP(val)))
      *valptr=val=fd_init_slotmap(NULL,0,NULL,NULL);
    slotid=fd_parse(copy);
    if (FD_EQ(slotid,content_type_symbol)) {
      handle_content_type(valstart,val);
      hval=fdtype_string(valstart);}
    else if ((FD_EQ(slotid,content_length_symbol)) ||
	     (FD_EQ(slotid,etag_symbol)))
      hval=fd_parse(valstart);
    else if ((FD_EQ(slotid,date_symbol)) ||
	     (FD_EQ(slotid,last_modified_symbol))) {
      time_t now, moment=curl_getdate(valstart,&now);
      struct U8_XTIME xt; u8_offtime(&xt,moment,0);
      hval=fd_make_timestamp(&xt,NULL);}
    else hval=fdtype_string(valstart);
    fd_add(val,slotid,hval);
    fd_decref(hval); u8_free(copy);}
  else {
    hval=fdtype_string(copy);
    fd_add(val,header_symbol,hval);
    fd_decref(hval);
    u8_free(copy);}
  return byte_len;
}

static fdtype addtexttype(fdtype type)
{
  FD_ADD_TO_CHOICE(text_types,fd_incref(type));
  return FD_VOID;
}

static struct FD_CURL_HANDLE *curl_err(u8_string cxt,int code)
{
  fd_seterr(CurlError,cxt,
	    u8_fromlibc((char *)curl_easy_strerror(code)),
	    FD_VOID);
  return NULL;
}

static int _curl_set(u8_string cxt,struct FD_CURL_HANDLE *h,
		     CURLoption option,void *v)
{
  CURLcode retval=curl_easy_setopt(h->handle,option,v);
  if (retval) {
    u8_free(h);
    fd_seterr(CurlError,cxt,
	      u8_fromlibc((char *)curl_easy_strerror(retval)),
	      FD_VOID);}
  return retval;
}

static int _curl_set_dtype(u8_string cxt,struct FD_CURL_HANDLE *h,
			   CURLoption option,
			   fdtype f,fdtype slotid)
{
  fdtype v=fd_get(f,slotid,FD_VOID);
  if (FD_ABORTP(v))
    return fd_interr(v);
  else if (FD_STRINGP(v)) {
    CURLcode retval=curl_easy_setopt(h->handle,option,FD_STRDATA(v));
    if (retval) {
      u8_free(h); fd_decref(v);
      fd_seterr(CurlError,cxt,
		u8_fromlibc((char *)curl_easy_strerror(retval)),
		FD_VOID);}
    return retval;}
  else {
    fd_seterr(fd_TypeError,cxt,u8_strdup("string"),v);
    return -1;}
}

FD_EXPORT
struct FD_CURL_HANDLE *fd_open_curl_handle()
{
  struct FD_CURL_HANDLE *h=u8_malloc(sizeof(struct FD_CURL_HANDLE));
  CURLcode cr; fdtype temp=FD_VOID;
#define curl_set(hl,o,v) \
   if (_curl_set("fd_open_curl_handle",hl,o,(void *)v)) return NULL;
#define curl_set_dtype(hl,o,f,s) \
   if (_curl_set_dtype("fd_open_curl_handle",hl,o,f,s)) return NULL;
  FD_INIT_CONS(h,fd_curl_type);
  h->handle=curl_easy_init(); h->strings=FD_EMPTY_CHOICE;
  if (h->handle==NULL) {
    u8_free(h);
    fd_seterr(CurlError,"fd_open_curl_handle",
	      strdup("curl_easy_init failed"),FD_VOID);
    return NULL;}
  curl_set(h,CURLOPT_NOPROGRESS,1);
  curl_set(h,CURLOPT_WRITEFUNCTION,copy_content_data);
  curl_set(h,CURLOPT_HEADERFUNCTION,handle_header);
  if (fd_test(curl_defaults,useragent_symbol,FD_VOID)) {
    curl_set_dtype(h,CURLOPT_USERAGENT,curl_defaults,useragent_symbol);}
  else curl_set(h,CURLOPT_USERAGENT,u8_sessionid());
  if (fd_test(curl_defaults,basicauth_symbol,FD_VOID)) {
    curl_set(h,CURLOPT_HTTPAUTH,CURLAUTH_BASIC);
    curl_set_dtype(h,CURLOPT_USERPWD,curl_defaults,basicauth_symbol);}
  if (fd_test(curl_defaults,authinfo_symbol,FD_VOID)) {
    curl_set(h,CURLOPT_HTTPAUTH,CURLAUTH_ANY);
    curl_set_dtype(h,CURLOPT_USERPWD,curl_defaults,authinfo_symbol);}
  if (fd_test(curl_defaults,referer_symbol,FD_VOID))
    curl_set_dtype(h,CURLOPT_REFERER,curl_defaults,referer_symbol);
  if (fd_test(curl_defaults,cookie_symbol,FD_VOID))
    curl_set_dtype(h,CURLOPT_REFERER,curl_defaults,cookie_symbol);
  if (fd_test(curl_defaults,cookiejar_symbol,FD_VOID))
    curl_set_dtype(h,CURLOPT_REFERER,curl_defaults,cookiejar_symbol);
  {
    fdtype http_headers=fd_get(curl_defaults,header_symbol,FD_EMPTY_CHOICE);
    FD_DO_CHOICES(header,http_headers) {
      if (FD_STRINGP(header)) {
	struct curl_slist *headers=
	  curl_slist_append(NULL,FD_STRDATA(header));
	if (_curl_set("fd_open_curl_handle",h,CURLOPT_HTTPHEADER,
		      (void *)headers)) {
	  curl_slist_free_all(headers);
	  return NULL;}
	curl_slist_free_all(headers);}}
    fd_decref(http_headers);}
  return h;
#undef curl_set
#undef curl_set_dtype
}

static void recycle_curl_handle(struct FD_CONS *c)
{
  struct FD_CURL_HANDLE *ch=(struct FD_CURL_HANDLE *)c;
  curl_easy_cleanup(ch->handle);
  fd_decref(ch->strings);
  if (FD_MALLOCD_CONSP(c))
    u8_free_x(c,sizeof(struct FD_CURL_HANDLE));
}

static int unparse_curl_handle(u8_output out,fdtype x)
{
  u8_printf(out,"#<CURL %lx>",x);
  return 1;
}

static fdtype set_curlopt
  (struct FD_CURL_HANDLE *ch,fdtype opt,fdtype val)
{
  if (FD_EQ(opt,referer_symbol))
    if (FD_STRINGP(val)) 
      curl_easy_setopt(ch->handle,CURLOPT_REFERER,FD_STRDATA(val));
    else return fd_type_error("string","set_curlopt",val);
  else if (FD_EQ(opt,useragent_symbol))
    if (FD_STRINGP(val)) 
      curl_easy_setopt(ch->handle,CURLOPT_USERAGENT,FD_STRDATA(val));
    else return fd_type_error("string","set_curlopt",val);
  else if (FD_EQ(opt,authinfo_symbol))
    if (FD_STRINGP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_HTTPAUTH,CURLAUTH_ANY);
      curl_easy_setopt(ch->handle,CURLOPT_USERPWD,FD_STRDATA(val));}
    else return fd_type_error("string","set_curlopt",val);
  else if (FD_EQ(opt,basicauth_symbol))
    if (FD_STRINGP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_HTTPAUTH,CURLAUTH_BASIC);
      curl_easy_setopt(ch->handle,CURLOPT_USERPWD,FD_STRDATA(val));}
    else return fd_type_error("string","set_curlopt",val);
  else if (FD_EQ(opt,cookie_symbol))
    if (FD_STRINGP(val)) 
      curl_easy_setopt(ch->handle,CURLOPT_COOKIE,FD_STRDATA(val));
    else return fd_type_error("string","set_curlopt",val);
  else if (FD_EQ(opt,cookiejar_symbol))
    if (FD_STRINGP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_COOKIEFILE,FD_STRDATA(val));
      curl_easy_setopt(ch->handle,CURLOPT_COOKIEJAR,FD_STRDATA(val));}
    else return fd_type_error("string","set_curlopt",val);
  else if (FD_EQ(opt,header_symbol))
    if (FD_STRINGP(val)) {
      struct curl_slist *headers=
	curl_slist_append(NULL,FD_STRDATA(val));
      curl_easy_setopt(ch->handle,CURLOPT_HTTPHEADER,headers);
      curl_slist_free_all(headers);}
    else return fd_type_error("string","set_curlopt",val);
  else return fd_err(_("Unknown CURL option"),"set_curl_handle",
		     NULL,opt);
  if (FD_STRINGP(val)) {FD_ADD_TO_CHOICE(ch->strings,fd_incref(val));}
  return FD_TRUE;
}

/* Primitives */

static fdtype urlget(fdtype url,fdtype handle)
{
  DBUF data; struct FD_CURL_HANDLE *h; CURLcode retval;
  fdtype result=fd_init_slotmap(NULL,0,NULL,NULL), cval;
  u8_string urltext=FD_STRDATA(url); char *urienc=NULL;
  fd_add(result,url_symbol,url);
  data.bytes=u8_malloc(8192); data.size=0; data.limit=8192;
  if (FD_PTR_TYPEP(handle,fd_curl_type))
    h=FD_GET_CONS(handle,fd_curl_type,fd_curl_handle);
  else h=get_curl_handle();
  if (h==NULL) {
    fd_decref(result);
    return fd_erreify();}
  if (urienc)
    curl_easy_setopt(h->handle,CURLOPT_URL,urienc);
  else curl_easy_setopt(h->handle,CURLOPT_URL,FD_STRDATA(url));  
  curl_easy_setopt(h->handle,CURLOPT_WRITEDATA,&data);
  curl_easy_setopt(h->handle,CURLOPT_WRITEHEADER,&result);
  retval=curl_easy_perform(h->handle);
  if (data.size<data.limit) data.bytes[data.size]='\0';
  else {
    data.bytes=u8_realloc(data.bytes,data.size+4);
    data.bytes[data.size]='\0';}
  if (data.size<=0) {
    if (data.size==0)
      cval=fd_init_packet(NULL,data.size,data.bytes);
    else cval=FD_EMPTY_CHOICE;}
  else if (fd_test(result,type_symbol,text_types)) {
    fdtype chset=fd_get(result,charset_symbol,FD_VOID);
    if (FD_STRINGP(chset)) {
      U8_OUTPUT out;
      u8_encoding enc=u8_get_encoding(FD_STRDATA(chset));
      if (enc) {
	unsigned char *scan=data.bytes;
	U8_INIT_OUTPUT(&out,data.size);
	u8_convert(enc,1,&out,&scan,data.bytes+data.size);
	cval=fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);}
      else cval=fd_init_string(NULL,-1,u8_valid_copy(data.bytes));}
    else cval=fd_init_string(NULL,-1,u8_valid_copy(data.bytes));
    u8_free(data.bytes);}
  else cval=fd_init_packet(NULL,data.size,data.bytes);
  if (urienc) u8_free(urienc);
  fd_add(result,content_symbol,cval);
  return result;
}

static fdtype urlcontent(fdtype url,fdtype handle)
{
  DBUF data; struct FD_CURL_HANDLE *h; CURLcode retval;
  fdtype result=fd_init_slotmap(NULL,0,NULL,NULL), cval;
  u8_string urltext=FD_STRDATA(url); char *urienc=NULL;
  fd_add(result,url_symbol,url);
  data.bytes=u8_malloc(8192); data.size=0; data.limit=8192;
  if (FD_PTR_TYPEP(handle,fd_curl_type))
    h=FD_GET_CONS(handle,fd_curl_type,fd_curl_handle);
  else h=get_curl_handle();
  if (urienc)
    curl_easy_setopt(h->handle,CURLOPT_URL,urienc);
  else curl_easy_setopt(h->handle,CURLOPT_URL,FD_STRDATA(url));  
  curl_easy_setopt(h->handle,CURLOPT_WRITEDATA,&data);
  curl_easy_setopt(h->handle,CURLOPT_WRITEHEADER,&result);
  retval=curl_easy_perform(h->handle);
  if (data.size<data.limit) data.bytes[data.size]='\0';
  else {
    data.bytes=u8_realloc(data.bytes,data.size+4);
    data.bytes[data.size]='\0';}
  if (data.size<=0) {
    if (data.size==0)
      cval=fd_init_packet(NULL,data.size,data.bytes);
    else cval=FD_EMPTY_CHOICE;}
  else if (fd_test(result,type_symbol,text_types)) {
    fdtype chset=fd_get(result,charset_symbol,FD_VOID);
    if (FD_STRINGP(chset)) {
      U8_OUTPUT out;
      u8_encoding enc=u8_get_encoding(FD_STRDATA(chset));
      if (enc) {
	unsigned char *scan=data.bytes;
	U8_INIT_OUTPUT(&out,data.size);
	u8_convert(enc,1,&out,&scan,data.bytes+data.size);
	cval=fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);}
      else cval=fd_init_string(NULL,-1,u8_valid_copy(data.bytes));}
    else cval=fd_init_string(NULL,-1,u8_valid_copy(data.bytes));
    u8_free(data.bytes);}
  else cval=fd_init_packet(NULL,data.size,data.bytes);
  if (urienc) u8_free(urienc);
  fd_decref(result);
  return cval;
}

static fdtype urlxml(fdtype url,fdtype xmloptions,fdtype handle)
{
  DBUF data; struct FD_CURL_HANDLE *h; CURLcode retval;
  fdtype result=fd_init_slotmap(NULL,0,NULL,NULL), cval;
  u8_string urltext=FD_STRDATA(url); char *urienc=NULL;
  int flags=fd_xmlparseoptions(xmloptions);
  /* Check that the XML options are okay */
  if (flags<0) {
    fd_decref(result); return fd_erreify();}
  fd_add(result,url_symbol,url);
  data.bytes=u8_malloc(8192); data.size=0; data.limit=8192;
  if (FD_PTR_TYPEP(handle,fd_curl_type))
    h=FD_GET_CONS(handle,fd_curl_type,fd_curl_handle);
  else h=get_curl_handle();
  if (urienc)
    curl_easy_setopt(h->handle,CURLOPT_URL,urienc);
  else curl_easy_setopt(h->handle,CURLOPT_URL,FD_STRDATA(url));  
  curl_easy_setopt(h->handle,CURLOPT_WRITEDATA,&data);
  curl_easy_setopt(h->handle,CURLOPT_WRITEHEADER,&result);
  retval=curl_easy_perform(h->handle);
  if (data.size<data.limit) data.bytes[data.size]='\0';
  else {
    data.bytes=u8_realloc(data.bytes,data.size+4);
    data.bytes[data.size]='\0';}
  if (data.size<=0) {
    if (data.size==0)
      cval=fd_init_packet(NULL,data.size,data.bytes);
    else cval=FD_EMPTY_CHOICE;}
  else if (fd_test(result,type_symbol,text_types)) {
    U8_INPUT in; u8_string buf;
    struct FD_XML xmlnode, *xmlret;
    fdtype chset=fd_get(result,charset_symbol,FD_VOID);
    if (FD_STRINGP(chset)) {
      U8_OUTPUT out;
      u8_encoding enc=u8_get_encoding(FD_STRDATA(chset));
      if (enc) {
	unsigned char *scan=data.bytes;
	U8_INIT_OUTPUT(&out,data.size);
	u8_convert(enc,1,&out,&scan,data.bytes+data.size);
	u8_free(data.bytes); buf=out.u8_outbuf;
	U8_INIT_STRING_INPUT(&in,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);}
      else {
	U8_INIT_STRING_INPUT(&in,data.size,data.bytes); buf=data.bytes;}}
    else {
      U8_INIT_STRING_INPUT(&in,data.size,data.bytes); buf=data.bytes;}
    fd_init_xml_node(&xmlnode,NULL,urltext); xmlnode.bits=flags;
    xmlret=fd_walk_xml(&in,fd_default_contentfn,NULL,NULL,NULL,
		       fd_default_popfn,
		       &xmlnode);
    if (xmlret) {
      {FD_DOLIST(elt,xmlret->head) {
	if (FD_SLOTMAPP(elt)) {
	  fdtype name=fd_get(elt,name_symbol,FD_EMPTY_CHOICE);
	  if (FD_SYMBOLP(name)) fd_add(result,name,elt);
	  else if ((FD_CHOICEP(name)) || (FD_ACHOICEP(name))) {
	    FD_DO_CHOICES(nm,name) {
	      if (FD_SYMBOLP(nm)) fd_add(result,nm,elt);}}}}}
      fd_add(result,content_symbol,xmlret->head);
      u8_free(buf); fd_decref(xmlret->head);
      return result;}
    else {
      fd_decref(result); u8_free(buf);
      return fd_erreify();}}
  else {
    fdtype err;
    cval=fd_init_packet(NULL,data.size,data.bytes);
    fd_add(result,content_symbol,cval); fd_decref(cval);
    err=fd_err(NonTextualContent,"urlxml",urltext,result);
    fd_decref(result);
    return err;}
  return cval;
}

static fdtype curlsetopt(fdtype handle,fdtype opt,fdtype value)
{
  if (FD_FALSEP(handle)) {
    if (FD_EQ(opt,header_symbol))
      fd_add(curl_defaults,opt,value);
    else fd_store(curl_defaults,opt,value);
    return FD_TRUE;}
  else if (FD_VOIDP(value)) {
    struct FD_CURL_HANDLE *h=fd_open_curl_handle();
    opt=handle; value=opt;
    return set_curlopt(h,opt,value);}
  else if (FD_PTR_TYPEP(handle,fd_curl_type)) {
    struct FD_CURL_HANDLE *h=FD_GET_CONS(handle,fd_curl_type,fd_curl_handle);
    return set_curlopt(h,opt,value);}
  else return fd_type_error("curl handle","curlsetopt",handle);
}

static fdtype curlopen(int n,fdtype *args)
{
  if (n==0)
    return (fdtype) fd_open_curl_handle();
  else if (n%2)
    return fd_err(fd_SyntaxError,"CURLOPEN",NULL,FD_VOID);
  else {
    int i=0;
    struct FD_CURL_HANDLE *ch=fd_open_curl_handle();
    while (i<n) {
      set_curlopt(ch,args[i],args[i+1]);
      i=i+2;}
    return (fdtype) ch;}
}

static fdtype urlpost(int n,fdtype *args)
{
  DBUF data; CURLcode retval; 
  int start, consed_handle=0;
  u8_string url; u8_byte *tmpbuf=NULL;
  struct FD_CURL_HANDLE *h=NULL;
  fdtype result=FD_VOID, cval;
  if (FD_PTR_TYPEP(args[0],fd_curl_type))
    if (FD_STRINGP(args[1])) {
      h=FD_GET_CONS(args[0],fd_curl_type,fd_curl_handle);
      start=2; url=FD_STRDATA(args[1]);}
    else return fd_err(fd_TypeError,"CURLPOST",NULL,args[1]);
  else if (FD_STRINGP(args[1])) {
    start=1; url=FD_STRDATA(args[1]);}
  else return fd_err(fd_TypeError,"CURLPOST",NULL,args[0]);
  if (((n-start)>1) && ((n-start)%2))
    fd_err(fd_SyntaxError,"CURLPOST",NULL,FD_VOID);
  else {consed_handle=1; h=fd_open_curl_handle();}
  data.bytes=u8_malloc(8192); data.size=0; data.limit=8192;
  result=fd_init_slotmap(NULL,0,NULL,NULL);
  curl_easy_setopt(h->handle,CURLOPT_URL,url);
  curl_easy_setopt(h->handle,CURLOPT_WRITEDATA,&data);
  curl_easy_setopt(h->handle,CURLOPT_WRITEHEADER,&result);
  if ((n-start)==1) {
    if (FD_STRINGP(args[start]))
      curl_easy_setopt(h->handle,CURLOPT_POSTFIELDS,
		       FD_STRDATA(args[start]));
    else if (FD_PACKETP(args[start])) {
      curl_easy_setopt(h->handle,CURLOPT_POSTFIELDSIZE,
		       FD_PACKET_LENGTH(args[start]));
      curl_easy_setopt(h->handle,CURLOPT_POSTFIELDS,
		       FD_PACKET_DATA(args[start]));}
    else {
      if (consed_handle) fd_decref((fdtype)h);
      return fd_err(fd_TypeError,"CURLPOST",u8_strdup("postdata"),
		    args[start]);}
    retval=curl_easy_perform(h->handle);}
  else {
    /* Construct form data */
    int i=start;
    struct curl_httppost *post = NULL, *last = NULL;
    while (i<n) {
      u8_string *keyname, *content_type=NULL;
      fdtype key=args[i], val=args[i+1]; i=i+2;
      if (!(FD_SYMBOLP(key)))
	return fd_err(fd_TypeError,"CURLPOST",u8_strdup("bad form var"),key);
      else if (FD_STRINGP(val))
	curl_formadd(&post,&last,
		     CURLFORM_COPYNAME,FD_SYMBOL_NAME(key),
		     CURLFORM_PTRCONTENTS,FD_STRDATA(val),
		     CURLFORM_CONTENTSLENGTH,FD_STRLEN(val),
		     CURLFORM_END);
      else if (FD_PACKETP(val))
	curl_formadd(&post,&last,
		     CURLFORM_COPYNAME,FD_SYMBOL_NAME(key),
		     CURLFORM_PTRCONTENTS,FD_PACKET_DATA(val),
		     CURLFORM_CONTENTSLENGTH,FD_PACKET_LENGTH(val),
		     CURLFORM_END);
      else {
	U8_OUTPUT out; U8_INIT_OUTPUT(&out,128);
	fd_unparse(&out,val);
	curl_formadd(&post,&last,
		     CURLFORM_COPYNAME,FD_SYMBOL_NAME(key),
		     CURLFORM_COPYCONTENTS,out.u8_outbuf,
		     CURLFORM_CONTENTSLENGTH,out.u8_outptr-out.u8_outbuf,
		     CURLFORM_END);
	u8_free(out.u8_outbuf);}}
    curl_easy_setopt(h->handle, CURLOPT_HTTPPOST, post);
    retval=curl_easy_perform(h->handle);
    curl_formfree(post);}
  if (fd_test(result,type_symbol,text_symbol)) {
    fdtype chset=fd_get(result,charset_symbol,FD_VOID);
    if (FD_STRINGP(chset)) {
      U8_OUTPUT out;
      u8_encoding enc=u8_get_encoding(FD_STRDATA(chset));
      if (enc) {
	unsigned char *scan=data.bytes;
	U8_INIT_OUTPUT(&out,data.size);
	u8_convert(enc,1,&out,&scan,data.bytes+data.size);
	cval=fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);}
      else cval=fd_init_string(NULL,-1,u8_valid_copy(data.bytes));}
    else cval=fd_init_string(NULL,-1,u8_valid_copy(data.bytes));
    u8_free(data.bytes);}
  else cval=fd_init_packet(NULL,data.size,data.bytes);
  fd_add(result,content_symbol,cval);
  fd_decref(cval);
  return result;
}

static int curl_initialized=0;

FD_EXPORT void fd_init_curl_c(void) FD_LIBINIT_FN;

FD_EXPORT void fd_init_curl_c()
{
  fdtype module;
  if (curl_initialized) return;
  curl_initialized=1;
  fd_init_fdscheme();

  module=fd_new_module("FDWEB",(FD_MODULE_DEFAULT));

  fd_curl_type=fd_register_cons_type("CURLHANDLE");
  fd_recyclers[fd_curl_type]=recycle_curl_handle;
  fd_unparsers[fd_curl_type]=unparse_curl_handle;
  
  curl_global_init(CURL_GLOBAL_ALL|CURL_GLOBAL_SSL);
  atexit(curl_global_cleanup);
  
#if ((FD_THREADS_ENABLED) && (FD_USE_TLS))
  u8_new_threadkey(&curl_threadkey,free_threadlocal_curl);
#endif
  
  url_symbol=fd_intern("URL");
  content_type_symbol=fd_intern("CONTENT-TYPE");
  content_length_symbol=fd_intern("CONTENT-LENGTH");
  etag_symbol=fd_intern("ETAG");
  charset_symbol=fd_intern("CHARSET");
  type_symbol=fd_intern("TYPE");
  text_symbol=fd_intern("TEXT");
  content_symbol=fd_intern("%CONTENT");
  header_symbol=fd_intern("HEADER");
  referer_symbol=fd_intern("REFERER");
  useragent_symbol=fd_intern("USERAGENT");
  cookie_symbol=fd_intern("COOKIE");
  cookiejar_symbol=fd_intern("COOKIEJAR");
  authinfo_symbol=fd_intern("AUTHINFO");
  basicauth_symbol=fd_intern("BASICAUTH");
  date_symbol=fd_intern("DATE");
  last_modified_symbol=fd_intern("LAST-MODIFIED");
  name_symbol=fd_intern("NAME");
  
  FD_ADD_TO_CHOICE(text_types,text_symbol);
  FD_ADD_TO_CHOICE(text_types,fd_init_string(NULL,-1,"application/xml"));
  

  curl_defaults=fd_init_slotmap(NULL,0,NULL,NULL);

  fd_idefn(module,fd_make_cprim2x("URLGET",urlget,1,
				  fd_string_type,FD_VOID,
				  fd_curl_type,FD_VOID));
  fd_idefn(module,fd_make_cprimn("URLPOST",urlpost,1));
  fd_idefn(module,fd_make_cprim2x("URLCONTENT",urlcontent,1,
				  fd_string_type,FD_VOID,
				  fd_curl_type,FD_VOID));
  fd_idefn(module,fd_make_cprim3x("URLXML",urlxml,1,
				  fd_string_type,FD_VOID,
				  -1,FD_VOID,
				  fd_curl_type,FD_VOID));
  fd_idefn(module,fd_make_cprimn("CURLOPEN",curlopen,0));
  fd_idefn(module,fd_make_cprim3("CURLSETOPT!",curlsetopt,2));
  fd_idefn(module,fd_make_cprim1("ADD-TEXT_TYPE!",addtexttype,1));
  fd_register_source_file(versionid);
}


/* The CVS log for this file
   $Log: curl.c,v $
   Revision 1.31  2006/01/31 13:47:23  haase
   Changed fd_str[n]dup into u8_str[n]dup

   Revision 1.30  2006/01/26 14:44:32  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.29  2006/01/24 15:53:39  haase
   Fixed some XML/fdweb slotids

   Revision 1.28  2006/01/07 23:46:32  haase
   Moved thread API into libu8

   Revision 1.27  2005/12/30 02:45:10  haase
   Made urlget functions store an URL slot on returned slotmaps

   Revision 1.26  2005/11/13 01:38:58  haase
   Added broader range of text types (including application/xml) to urlget etc.

   Revision 1.25  2005/11/11 05:40:38  haase
   Added unparser for CURL handles

   Revision 1.24  2005/11/03 14:28:59  haase
   Made URLXML handle the case where a top level node has multiple names (e.g. qualified, unqualified, etc)

   Revision 1.23  2005/10/25 21:04:19  haase
   Extended XML parsing to optionally include less information (useful for pure data XML such as REST replies) and added URLCONTENT and URLXML primitives for getting URL content and combining XML parsing and URL fetching

   Revision 1.22  2005/10/10 16:53:48  haase
   Fixes for new mktime/offtime functions

   Revision 1.21  2005/10/05 17:23:26  haase
   Fix missing NUL bug in urlget

   Revision 1.20  2005/09/20 17:10:44  haase
   Added a way to add authinfo to CURL requests and added field for keeping string SETOPTs

   Revision 1.19  2005/08/10 06:34:08  haase
   Changed module name to fdb, moving header file as well

   Revision 1.18  2005/08/04 23:22:44  haase
   Made URI encoding deal with control characters

   Revision 1.17  2005/07/20 02:31:58  haase
   Moved freeing of uriencoded string

   Revision 1.16  2005/07/17 19:38:10  haase
   Fixes to uri encoding and escaping

   Revision 1.15  2005/07/16 15:20:39  haase
   Add case for non-terminated charsets in content-types

   Revision 1.14  2005/07/08 20:19:59  haase
   Added type info to URLGET declaration

   Revision 1.13  2005/07/06 14:55:59  haase
   Added greedy matching to text matching, etc

   Revision 1.12  2005/06/19 19:40:04  haase
   Regularized use of FD_CONS_HEADER

   Revision 1.11  2005/05/19 16:44:21  haase
   Added source file registration for fdweb files and fixed header in exif.c

   Revision 1.10  2005/05/18 19:25:19  haase
   Fixes to header ordering to make off_t defaults be pervasive

   Revision 1.9  2005/05/10 18:43:35  haase
   Added context argument to fd_type_error

   Revision 1.8  2005/04/21 19:06:09  haase
   Cleaned up and unified initialization routines

   Revision 1.7  2005/04/17 17:03:59  haase
   Fix for tls implementations

   Revision 1.6  2005/04/17 15:36:56  haase
   Added explicit calls to fd_init_fdscheme to fdweb initializations

   Revision 1.5  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.4  2005/04/06 19:25:30  haase
   Made fdweb libraries initialize a single pair of safe/unsafe environments

   Revision 1.3  2005/04/02 20:57:15  haase
   Preprocess date fields from fetched documents

   Revision 1.2  2005/04/02 20:15:38  haase
   Added URLPOST and other extensions


*/
