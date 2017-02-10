/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
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

#include <curl/curl.h>

#include <ctype.h>
#include <math.h>

#if HAVE_CRYPTO_set_locking_callback
/* we have this global to let the callback get easy access to it */
static pthread_mutex_t *ssl_lockarray;
#include <openssl/crypto.h>
#define LOCK_OPENSSL 1
#else
#define LOCK_OPENSSL 0
#endif

static fdtype curl_defaults, url_symbol;
static fdtype content_type_symbol, charset_symbol, type_symbol;
static fdtype content_length_symbol, etag_symbol, content_encoding_symbol;
static fdtype verbose_symbol, text_symbol, content_symbol, header_symbol;
static fdtype referer_symbol, useragent_symbol, cookie_symbol;
static fdtype date_symbol, last_modified_symbol, name_symbol;
static fdtype cookiejar_symbol, authinfo_symbol, basicauth_symbol;
static fdtype maxtime_symbol, timeout_symbol, method_symbol;
static fdtype verifyhost_symbol, verifypeer_symbol, cainfo_symbol;
static fdtype eurl_slotid, filetime_slotid, response_code_slotid;
static fdtype timeout_symbol, connect_timeout_symbol, accept_timeout_symbol;
static fdtype dns_symbol, dnsip_symbol, dns_cachelife_symbol;
static fdtype fresh_connect_symbol, forbid_reuse_symbol, filetime_symbol;


static fdtype text_types=FD_EMPTY_CHOICE;

static int debugging_curl=0;

static fd_exception NonTextualContent=
  _("can't parse non-textual content as XML");
static fd_exception CurlError=_("Internal libcurl error");

typedef struct FD_CURL_HANDLE {
  FD_CONS_HEADER;
  CURL *handle;
  struct curl_slist *headers;
  /* char curl_errbuf[CURL_ERROR_SIZE]; */
  fdtype initdata;} FD_CURL_HANDLE;
typedef struct FD_CURL_HANDLE *fd_curl_handle;

FD_EXPORT struct FD_CURL_HANDLE *fd_open_curl_handle(void);

typedef struct INBUF {
  unsigned char *bytes;
  int size, limit;} INBUF;

typedef struct OUTBUF {
  unsigned char *scan, *end;} OUTBUF;

static size_t copy_upload_data(void *ptr,size_t size,size_t nmemb,void *stream)
{
  OUTBUF *rbuf=(OUTBUF *)stream;
  int bytes_to_read=size*nmemb;
  if (bytes_to_read<(rbuf->end-rbuf->scan)) {
    memcpy(ptr,rbuf->scan,bytes_to_read);
    rbuf->scan=rbuf->scan+bytes_to_read;
    return bytes_to_read;}
  else {
    int n_left=rbuf->end-rbuf->scan;
    memcpy(ptr,rbuf->scan,n_left);
    rbuf->scan=rbuf->end;
    return n_left;}
}

static size_t copy_content_data(char *data,size_t size,size_t n,void *vdbuf)
{
  INBUF *dbuf=(INBUF *)vdbuf; u8_byte *databuf;
  if (dbuf->size+size*n > dbuf->limit) {
    char *newptr;
    int need_space=dbuf->size+size*n, new_limit=dbuf->limit;
    while (new_limit<need_space) {
      if (new_limit >= 65536)
        new_limit=new_limit+65536;
      else new_limit=new_limit*2;}
    newptr=u8_realloc((char *)dbuf->bytes,new_limit);
    dbuf->bytes=newptr; dbuf->limit=new_limit;}
  databuf=(unsigned char *)dbuf->bytes;
  memcpy(databuf+dbuf->size,data,size*n);
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
  fd_add(table,type_symbol,major_type);
  if (slash) *slash='/';
  full_type=fdtype_string(value);
  fd_add(table,type_symbol,full_type); *end=endbyte;
  fd_decref(major_type); fd_decref(full_type);
  if ((chset=(strstr(value,"charset=")))) {
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
  if ((copy[byte_len-1]='\n') && (copy[byte_len-2]='\r')) {
    if (byte_len==2) {
      u8_free(copy); return byte_len;}
    else copy[byte_len-2]='\0';}
  else {}
  if ((valstart=(strchr(copy,':')))) {
    *valstart++='\0'; while (isspace(*valstart)) valstart++;
    if (!(FD_TABLEP(val)))
      *valptr=val=fd_empty_slotmap();
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
      struct U8_XTIME xt;
      u8_init_xtime(&xt,moment,u8_second,0,0,0);
      hval=fd_make_timestamp(&xt);}
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

FD_INLINE_FCN fdtype addtexttype(fdtype type)
{
  fd_incref(type);
  FD_ADD_TO_CHOICE(text_types,type);
  return FD_VOID;
}

static void decl_text_type(u8_string string)
{
  fdtype stringval=fdtype_string(string);
  FD_ADD_TO_CHOICE(text_types,stringval);
}

FD_INLINE_FCN struct FD_CURL_HANDLE *curl_err(u8_string cxt,int code)
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

static int _curl_set2dtype(u8_string cxt,struct FD_CURL_HANDLE *h,
                           CURLoption option,
                           fdtype f,fdtype slotid)
{
  fdtype v=fd_get(f,slotid,FD_VOID);
  if (FD_ABORTP(v))
    return fd_interr(v);
  else if ((FD_STRINGP(v))||
           (FD_PACKETP(v))||
           (FD_PRIM_TYPEP(v,fd_secret_type))) {
    CURLcode retval=curl_easy_setopt(h->handle,option,FD_STRDATA(v));
    if (retval) {
      u8_free(h); fd_decref(v);
      fd_seterr(CurlError,cxt,
                u8_fromlibc((char *)curl_easy_strerror(retval)),
                FD_VOID);}
    return retval;}
  else if (FD_FIXNUMP(v)) {
    CURLcode retval=curl_easy_setopt(h->handle,option,(long)(fd_getint(v)));
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

static int curl_add_header(fd_curl_handle ch,u8_string arg1,u8_string arg2)
{
  struct curl_slist *cur=ch->headers, *newh;
  if (arg2==NULL)
    newh=curl_slist_append(cur,arg1);
  else {
    u8_string hdr=u8_mkstring("%s: %s",arg1,arg2);
    newh=curl_slist_append(cur,hdr);
    u8_free(hdr);}
  ch->headers=newh;
  if (curl_easy_setopt(ch->handle,CURLOPT_HTTPHEADER,(void *)newh)!=CURLE_OK) {
    return -1;}
  else return 1;
}

static int curl_add_headers(fd_curl_handle ch,fdtype val)
{
  int retval=0;
  FD_DO_CHOICES(v,val)
    if (FD_STRINGP(v))
      retval=curl_add_header(ch,FD_STRDATA(v),NULL);
    else if (FD_PACKETP(v))
      retval=curl_add_header(ch,FD_PACKET_DATA(v),NULL);
    else if (FD_PAIRP(v)) {
      fdtype car=FD_CAR(v), cdr=FD_CDR(v); u8_string hdr=NULL;
      if ((FD_SYMBOLP(car)) && (FD_STRINGP(cdr)))
        hdr=u8_mkstring("%s: %s",FD_SYMBOL_NAME(car),FD_STRDATA(cdr));
      else if ((FD_STRINGP(car)) && (FD_STRINGP(cdr)))
        hdr=u8_mkstring("%s: %s",FD_STRDATA(car),FD_STRDATA(cdr));
      else if (FD_SYMBOLP(car))
        hdr=u8_mkstring("%s: %q",FD_SYMBOL_NAME(car),cdr);
      else if (FD_STRINGP(car))
        hdr=u8_mkstring("%s: %q",FD_STRDATA(car),cdr);
      else hdr=u8_mkstring("%q: %q",car,cdr);
      retval=curl_add_header(ch,hdr,NULL);
      u8_free(hdr);}
    else if (FD_SLOTMAPP(v)) {
      fdtype keys=fd_getkeys(v);
      FD_DO_CHOICES(key,keys) {
        if ((retval>=0)&&((FD_STRINGP(key))||(FD_SYMBOLP(key)))) {
          fdtype kval=fd_get(v,key,FD_EMPTY_CHOICE); u8_string hdr=NULL;
          if (FD_SYMBOLP(key)) {
            if (FD_STRINGP(kval))
              hdr=u8_mkstring("%s: %s",FD_SYMBOL_NAME(key),FD_STRDATA(kval));
            else hdr=u8_mkstring("%s: %q",FD_SYMBOL_NAME(key),kval);}
          else if (FD_STRINGP(kval))
            hdr=u8_mkstring("%s: %s",FD_STRDATA(key),FD_STRDATA(kval));
          else hdr=u8_mkstring("%s: %q",FD_STRDATA(key),kval);
          retval=curl_add_header(ch,hdr,NULL);
          u8_free(hdr);
          fd_decref(kval);}}
      fd_decref(keys);}
    else {}
  return retval;
}

FD_EXPORT
struct FD_CURL_HANDLE *fd_open_curl_handle()
{
  struct FD_CURL_HANDLE *h=u8_alloc(struct FD_CURL_HANDLE);
#define curl_set(hl,o,v) \
   if (_curl_set("fd_open_curl_handle",hl,o,(void *)v)) return NULL;
#define curl_set2dtype(hl,o,f,s) \
   if (_curl_set2dtype("fd_open_curl_handle",hl,o,f,s)) return NULL;
  FD_INIT_CONS(h,fd_curl_type);
  h->handle=curl_easy_init();
  h->headers=NULL;
  h->initdata=FD_EMPTY_CHOICE;
  if (h->handle==NULL) {
    u8_free(h);
    fd_seterr(CurlError,"fd_open_curl_handle",
              strdup("curl_easy_init failed"),FD_VOID);
    return NULL;}
  if (debugging_curl) {
    FD_INTPTR ptrval=(FD_INTPTR) h->handle;
    u8_log(LOG_DEBUG,"CURL","Creating CURL handle %llx",ptrval);
    curl_easy_setopt(h->handle,CURLOPT_VERBOSE,1);}
  /*
  memset(h->curl_errbuf,0,sizeof(h->curl_errbuf));
  curl_easy_setopt(h,CURLOPT_ERRORBUFFER,(h->curl_errbuf));
  */
  curl_set(h,CURLOPT_NOPROGRESS,1);
  curl_set(h,CURLOPT_FILETIME,(long)1);
  curl_set(h,CURLOPT_NOSIGNAL,1);
  curl_set(h,CURLOPT_WRITEFUNCTION,copy_content_data);
  curl_set(h,CURLOPT_HEADERFUNCTION,handle_header);
  if (fd_test(curl_defaults,useragent_symbol,FD_VOID)) {
    curl_set2dtype(h,CURLOPT_USERAGENT,curl_defaults,useragent_symbol);}
  else curl_set(h,CURLOPT_USERAGENT,u8_sessionid());
  if (fd_test(curl_defaults,basicauth_symbol,FD_VOID)) {
    curl_set(h,CURLOPT_HTTPAUTH,CURLAUTH_BASIC);
    curl_set2dtype(h,CURLOPT_USERPWD,curl_defaults,basicauth_symbol);}
  if (fd_test(curl_defaults,authinfo_symbol,FD_VOID)) {
    curl_set(h,CURLOPT_HTTPAUTH,CURLAUTH_ANY);
    curl_set2dtype(h,CURLOPT_USERPWD,curl_defaults,authinfo_symbol);}
  if (fd_test(curl_defaults,referer_symbol,FD_VOID))
    curl_set2dtype(h,CURLOPT_REFERER,curl_defaults,referer_symbol);
  if (fd_test(curl_defaults,cookie_symbol,FD_VOID))
    curl_set2dtype(h,CURLOPT_REFERER,curl_defaults,cookie_symbol);
  if (fd_test(curl_defaults,cookiejar_symbol,FD_VOID))
    curl_set2dtype(h,CURLOPT_REFERER,curl_defaults,cookiejar_symbol);
  if (fd_test(curl_defaults,maxtime_symbol,FD_VOID))
    curl_set2dtype(h,CURLOPT_TIMEOUT,curl_defaults,maxtime_symbol);
  if (fd_test(curl_defaults,timeout_symbol,FD_VOID))
    curl_set2dtype(h,CURLOPT_CONNECTTIMEOUT,curl_defaults,timeout_symbol);
  {
    fdtype http_headers=fd_get(curl_defaults,header_symbol,FD_EMPTY_CHOICE);
    curl_add_headers(h,http_headers);
    fd_decref(http_headers);}
  return h;
#undef curl_set
#undef curl_set2dtype
}

static char *getcurlerror(char *buf,int code)
{
  return (char *) curl_easy_strerror(code);
  /*
  if (*buf=='\0')
    return (char *) curl_easy_strerror(code);
  else return buf;
  */
}

static void recycle_curl_handle(struct FD_CONS *c)
{
  struct FD_CURL_HANDLE *ch=(struct FD_CURL_HANDLE *)c;
  if (debugging_curl) {
    FD_INTPTR ptrval=(FD_INTPTR) ch->handle;
    u8_log(LOG_DEBUG,"CURL","Freeing CURL handle %llx",ptrval);}
  /* curl_easy_setopt(ch->handle,CURLOPT_ERRORBUFFER,NULL); */
  curl_slist_free_all(ch->headers);
  curl_easy_cleanup(ch->handle);
  fd_decref(ch->initdata);
  if (FD_MALLOCD_CONSP(c)) u8_free(c);
}

static int unparse_curl_handle(u8_output out,fdtype x)
{
  u8_printf(out,"#<CURL %lx>",x);
  return 1;
}

static fdtype curlhandlep(fdtype arg)
{
  if (FD_PRIM_TYPEP(arg,fd_curl_type)) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype curlreset(fdtype arg)
{
  struct FD_CURL_HANDLE *ch=(struct FD_CURL_HANDLE *)arg;
  curl_easy_reset(ch->handle);
  return FD_VOID;
}

static fdtype set_curlopt
  (struct FD_CURL_HANDLE *ch,fdtype opt,fdtype val)
{
  if (FD_EQ(opt,referer_symbol))
    if (FD_STRINGP(val))
      curl_easy_setopt(ch->handle,CURLOPT_REFERER,FD_STRDATA(val));
    else return fd_type_error("string","set_curlopt",val);
  else if (FD_EQ(opt,method_symbol))
    if (FD_SYMBOLP(val))
      curl_easy_setopt(ch->handle,CURLOPT_CUSTOMREQUEST,FD_SYMBOL_NAME(val));
    else if (FD_STRINGP(val))
      curl_easy_setopt(ch->handle,CURLOPT_CUSTOMREQUEST,FD_STRDATA(val));
    else return fd_type_error("symbol/method","set_curlopt",val);
  else if (FD_EQ(opt,verbose_symbol)) {
    if ((FD_FALSEP(val))||(FD_EMPTY_CHOICEP(val)))
      curl_easy_setopt(ch->handle,CURLOPT_VERBOSE,0);
    else curl_easy_setopt(ch->handle,CURLOPT_VERBOSE,1);}
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
    if ((FD_STRINGP(val))||(FD_PRIM_TYPEP(val,fd_secret_type))) {
      curl_easy_setopt(ch->handle,CURLOPT_HTTPAUTH,CURLAUTH_BASIC);
      curl_easy_setopt(ch->handle,CURLOPT_USERPWD,FD_STRDATA(val));}
    else return fd_type_error("string","set_curlopt",val);
  else if (FD_EQ(opt,cookie_symbol))
    if ((FD_STRINGP(val))||(FD_PRIM_TYPEP(val,fd_secret_type)))
      curl_easy_setopt(ch->handle,CURLOPT_COOKIE,FD_STRDATA(val));
    else return fd_type_error("string","set_curlopt",val);
  else if (FD_EQ(opt,cookiejar_symbol))
    if (FD_STRINGP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_COOKIEFILE,FD_STRDATA(val));
      curl_easy_setopt(ch->handle,CURLOPT_COOKIEJAR,FD_STRDATA(val));}
    else return fd_type_error("string","set_curlopt",val);
  else if (FD_EQ(opt,header_symbol))
    curl_add_headers(ch,val);
  else if (FD_EQ(opt,cainfo_symbol))
    if (FD_STRINGP(val))
      curl_easy_setopt(ch->handle,CURLOPT_CAINFO,FD_STRDATA(val));
    else return fd_type_error("string","set_curlopt",val);
  else if (FD_EQ(opt,verifyhost_symbol))
    if (FD_FIXNUMP(val))
      curl_easy_setopt(ch->handle,CURLOPT_SSL_VERIFYHOST,FD_FIX2INT(val));
    else if (FD_FALSEP(val))
      curl_easy_setopt(ch->handle,CURLOPT_SSL_VERIFYHOST,0);
    else if (FD_TRUEP(val))
      curl_easy_setopt(ch->handle,CURLOPT_SSL_VERIFYHOST,2);
    else return fd_type_error("symbol/method","set_curlopt",val);
  else if ((FD_EQ(opt,timeout_symbol))||(FD_EQ(opt,maxtime_symbol))) {
    if (FD_FIXNUMP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_TIMEOUT,FD_FIX2INT(val));}
    else if (FD_FLONUMP(val)) {
      double secs=FD_FLONUM(val);
      long int msecs=(long int)floor(secs*1000);
      curl_easy_setopt(ch->handle,CURLOPT_TIMEOUT_MS,msecs);}
    else return fd_type_error("seconds","set_curlopt/timeout",val);}
  else if (FD_EQ(opt,connect_timeout_symbol)) {
    if (FD_FIXNUMP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_CONNECTTIMEOUT,FD_FIX2INT(val));}
    else if (FD_FLONUMP(val)) {
      double secs=FD_FLONUM(val);
      long int msecs=(long int)floor(secs*1000);
      curl_easy_setopt(ch->handle,CURLOPT_CONNECTTIMEOUT_MS,msecs);}
    else return fd_type_error("seconds","set_curlopt/connecttimeout",val);}
  else if (FD_EQ(opt,connect_timeout_symbol)) {
    if (FD_FIXNUMP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_CONNECTTIMEOUT,FD_FIX2INT(val));}
    else if (FD_FLONUMP(val)) {
      double secs=FD_FLONUM(val);
      long int msecs=(long int)floor(secs*1000);
      curl_easy_setopt(ch->handle,CURLOPT_CONNECTTIMEOUT_MS,msecs);}
    else return fd_type_error("seconds","set_curlopt/connecttimeout",val);}
  else if (FD_EQ(opt,accept_timeout_symbol)) {
    if (FD_FIXNUMP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_ACCEPTTIMEOUT_MS,
                       (1000*FD_FIX2INT(val)));}
    else if (FD_FLONUMP(val)) {
      double secs=FD_FLONUM(val);
      long int msecs=(long int)floor(secs*1000);
      curl_easy_setopt(ch->handle,CURLOPT_ACCEPTTIMEOUT_MS,msecs);}
    else return fd_type_error("seconds","set_curlopt/connecttimeout",val);}
  else if (FD_EQ(opt,dns_symbol)) {
    if ((FD_STRINGP(val))&&(FD_STRLEN(val)>0)) {
      fd_incref(val); FD_ADD_TO_CHOICE(ch->initdata,val);
      curl_easy_setopt(ch->handle,CURLOPT_DNS_SERVERS,
                       FD_STRDATA(val));}
    else if ((FD_FALSEP(val))||(FD_STRINGP(val))) {
      curl_easy_setopt(ch->handle,CURLOPT_DNS_SERVERS,NULL);}
    else if (FD_SYMBOLP(val)) {
      fdtype cval=fd_config_get(FD_SYMBOL_NAME(val));
      if (!(FD_STRINGP(cval)))
        return fd_type_error("string config","set_curlopt/dns",val);
      fd_incref(cval); FD_ADD_TO_CHOICE(ch->initdata,cval);
      curl_easy_setopt(ch->handle,CURLOPT_DNS_SERVERS,
                       FD_STRDATA(cval));}
    else return fd_type_error("string","set_curlopt/dns",val);}
  else if (FD_EQ(opt,dnsip_symbol)) {
    if ((FD_STRINGP(val))&&(FD_STRLEN(val)>0)) {
      fd_incref(val); FD_ADD_TO_CHOICE(ch->initdata,val);
      if (strchr(FD_STRDATA(val),':')) {
        curl_easy_setopt(ch->handle,CURLOPT_DNS_LOCAL_IP6,
                         FD_STRDATA(val));}
      else { 
        curl_easy_setopt(ch->handle,CURLOPT_DNS_LOCAL_IP4,
                         FD_STRDATA(val));}}
    else if ((FD_FALSEP(val))||(FD_STRINGP(val))) {
      curl_easy_setopt(ch->handle,CURLOPT_DNS_LOCAL_IP4,NULL);
      curl_easy_setopt(ch->handle,CURLOPT_DNS_LOCAL_IP6,NULL);}
    else if (FD_SYMBOLP(val)) {
      fdtype cval=fd_config_get(FD_SYMBOL_NAME(val));
      if (!(FD_STRINGP(cval)))
        return fd_type_error("string config","set_curlopt/dns",val);
      fd_incref(cval); FD_ADD_TO_CHOICE(ch->initdata,cval);
      if (FD_STRLEN(cval)==0) {
        curl_easy_setopt(ch->handle,CURLOPT_DNS_LOCAL_IP4,NULL);
        curl_easy_setopt(ch->handle,CURLOPT_DNS_LOCAL_IP6,NULL);}
      else if (strchr(FD_STRDATA(cval),':')) {
        curl_easy_setopt(ch->handle,CURLOPT_DNS_LOCAL_IP6,
                         FD_STRDATA(cval));}
      else { 
        curl_easy_setopt(ch->handle,CURLOPT_DNS_LOCAL_IP4,
                         FD_STRDATA(cval));}}
    else return fd_type_error("string","set_curlopt/dns",val);}
  else if (FD_EQ(opt,dns_cachelife_symbol)) {
    if (FD_TRUEP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_DNS_CACHE_TIMEOUT,-1);}
    else if (FD_FALSEP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_DNS_CACHE_TIMEOUT,0);}
    else if (FD_FIXNUMP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_DNS_CACHE_TIMEOUT,
                       FD_FIX2INT(val));}
    else if (FD_FLONUMP(val)) {
      double secs=FD_FLONUM(val);
      long int msecs=(long int)floor(secs);
      curl_easy_setopt(ch->handle,CURLOPT_DNS_CACHE_TIMEOUT,msecs);}
    else return fd_type_error("seconds","set_curlopt/connecttimeout",val);}
  else if (FD_EQ(opt,fresh_connect_symbol)) {
    if (FD_TRUEP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_FRESH_CONNECT,1);}
    else curl_easy_setopt(ch->handle,CURLOPT_FRESH_CONNECT,0);}
  else if (FD_EQ(opt,forbid_reuse_symbol)) {
    if (FD_TRUEP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_FORBID_REUSE,1);}
    else curl_easy_setopt(ch->handle,CURLOPT_FORBID_REUSE,0);}
  else if (FD_EQ(opt,filetime_symbol)) {
    if (FD_TRUEP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_FILETIME,1);}
    else curl_easy_setopt(ch->handle,CURLOPT_FILETIME,0);}
  else if (FD_EQ(opt,verifypeer_symbol))
    if (FD_FIXNUMP(val))
      curl_easy_setopt(ch->handle,CURLOPT_SSL_VERIFYPEER,FD_FIX2INT(val));
    else if (FD_FALSEP(val))
      curl_easy_setopt(ch->handle,CURLOPT_SSL_VERIFYPEER,0);
    else if (FD_TRUEP(val))
      curl_easy_setopt(ch->handle,CURLOPT_SSL_VERIFYPEER,1);
    else return fd_type_error("fixnum/boolean","set_curlopt",val);
  else if (FD_EQ(opt,content_type_symbol))
    if (FD_STRINGP(val)) {
      u8_string ctype_header=u8_mkstring("Content-type: %s",FD_STRDATA(val));
      fdtype hval=fd_lispstring(ctype_header);
      curl_add_headers(ch,hval);
      fd_decref(hval);}
    else return fd_type_error(_("string"),"set_curl_handle/content-type",val);
  else return fd_err(_("Unknown CURL option"),"set_curl_handle",
                     NULL,opt);
  if (FD_CONSP(val)) {
    fd_incref(val); FD_ADD_TO_CHOICE(ch->initdata,val);}
  return FD_TRUE;
}

/* Fix URL */

static const char *digits="0123456789ABCDEF";

static fdtype fixurl(u8_string url)
{
  const u8_byte *scan=url; int c;
  struct U8_OUTPUT out; char buf[8];
  U8_INIT_OUTPUT(&out,256); buf[0]='%'; buf[3]='\0';
  scan=url; while ((c=(*scan++))) {
    if ((c>=0x80)||(iscntrl(c))||(isspace(c))) {
      buf[1]=digits[c/16]; buf[2]=digits[c%16];
      u8_puts(&out,buf);}
    else u8_putc(&out,c);}
  return fd_stream2string(&out);
}

/* The core get function */

static fdtype handlefetchresult(struct FD_CURL_HANDLE *h,fdtype result,INBUF *data);

static fdtype fetchurl(struct FD_CURL_HANDLE *h,u8_string urltext)
{
  INBUF data; CURLcode retval;
  int consed_handle=0;
  fdtype result=fd_empty_slotmap();
  fdtype url=fixurl(urltext);
  fd_add(result,url_symbol,url); fd_decref(url);
  data.bytes=u8_malloc(8192); data.size=0; data.limit=8192;
  if (h==NULL) {h=fd_open_curl_handle(); consed_handle=1;}
  curl_easy_setopt(h->handle,CURLOPT_URL,FD_STRDATA(url));
  curl_easy_setopt(h->handle,CURLOPT_WRITEDATA,&data);
  curl_easy_setopt(h->handle,CURLOPT_WRITEHEADER,&result);
  curl_easy_setopt(h->handle,CURLOPT_NOBODY,0);
  retval=curl_easy_perform(h->handle);
  if (retval!=CURLE_OK) {
    char buf[CURL_ERROR_SIZE];
    fdtype errval=
      fd_err(CurlError,"fetchurl",getcurlerror(buf,retval),url);
    fd_decref(result); u8_free(data.bytes);
    if (consed_handle) {fd_decref((fdtype)h);}
    return errval;}
  handlefetchresult(h,result,&data);
  if (consed_handle) {fd_decref((fdtype)h);}
  return result;
}

static fdtype fetchurlhead(struct FD_CURL_HANDLE *h,u8_string urltext)
{
  INBUF data; CURLcode retval;
  int consed_handle=0;
  fdtype result=fd_empty_slotmap();
  fdtype url=fixurl(urltext);
  fd_add(result,url_symbol,url); fd_decref(url);
  data.bytes=u8_malloc(8192); data.size=0; data.limit=8192;
  if (h==NULL) {h=fd_open_curl_handle(); consed_handle=1;}
  curl_easy_setopt(h->handle,CURLOPT_URL,FD_STRDATA(url));
  curl_easy_setopt(h->handle,CURLOPT_WRITEDATA,&data);
  curl_easy_setopt(h->handle,CURLOPT_WRITEHEADER,&result);
  curl_easy_setopt(h->handle,CURLOPT_NOBODY,1);
  retval=curl_easy_perform(h->handle);
  if (retval!=CURLE_OK) {
    char buf[CURL_ERROR_SIZE];
    fdtype errval=fd_err(CurlError,"fetchurl",getcurlerror(buf,retval),url);
    fd_decref(result); u8_free(data.bytes);
    if (consed_handle) {fd_decref((fdtype)h);}
    return errval;}
  handlefetchresult(h,result,&data);
  if (consed_handle) {fd_decref((fdtype)h);}
  return result;
}

static fdtype handlefetchresult(struct FD_CURL_HANDLE *h,fdtype result,
                                INBUF *data)
{
  fdtype cval; long http_response=0;
  int retval=curl_easy_getinfo(h->handle,CURLINFO_RESPONSE_CODE,&http_response);
  if (retval==0)
    fd_add(result,response_code_slotid,FD_INT(http_response));
  if (data->size<data->limit) {
    unsigned char *buf=(unsigned char *)(data->bytes);
    buf[data->size]='\0';
    data->bytes=buf;}
  else {
    unsigned char *buf=
      u8_realloc((u8_byte *)data->bytes,data->size+4);
    data->bytes=buf;
    buf[data->size]='\0';}
  if (data->size<0) cval=FD_EMPTY_CHOICE;
  else if ((fd_test(result,type_symbol,text_types))&&
           (!(fd_test(result,content_encoding_symbol,FD_VOID))))
    if (data->size==0)
      cval=fd_block_string(data->size,data->bytes);
    else {
      fdtype chset=fd_get(result,charset_symbol,FD_VOID);
      if (FD_STRINGP(chset)) {
        U8_OUTPUT out;
        u8_encoding enc=u8_get_encoding(FD_STRDATA(chset));
        if (enc) {
          const unsigned char *scan=data->bytes;
          U8_INIT_OUTPUT(&out,data->size);
          u8_convert(enc,1,&out,&scan,data->bytes+data->size);
          cval=fd_block_string(out.u8_write-out.u8_outbuf,out.u8_outbuf);}
        else if (strstr(data->bytes,"\r\n"))
          cval=fd_lispstring(u8_convert_crlfs(data->bytes));
        else cval=fd_lispstring(u8_valid_copy(data->bytes));}
      else if (strstr(data->bytes,"\r\n"))
        cval=fd_lispstring(u8_convert_crlfs(data->bytes));
      else cval=fd_lispstring(u8_valid_copy(data->bytes));
      u8_free(data->bytes);
      fd_decref(chset);}
  else {
    cval=fd_make_packet(NULL,data->size,data->bytes);
    u8_free(data->bytes);}
  fd_add(result,content_symbol,cval);
  {
    char *urlbuf; long filetime;
    CURLcode rv=curl_easy_getinfo(h->handle,CURLINFO_EFFECTIVE_URL,&urlbuf);
    if (rv==CURLE_OK) {
      fdtype eurl=fdtype_string(urlbuf);
      fd_add(result,eurl_slotid,eurl);
      fd_decref(eurl);}
    rv=curl_easy_getinfo(h->handle,CURLINFO_FILETIME,&filetime);
    if ((rv==CURLE_OK) && (filetime>=0)) {
      fdtype ftime=fd_time2timestamp(filetime);
      fd_add(result,filetime_slotid,ftime);
      fd_decref(ftime);}}
  fd_decref(cval);
  return result;
}

/* Handling arguments to URL primitives */

static fdtype curl_arg(fdtype arg,u8_context cxt)
{
  if (FD_PTR_TYPEP(arg,fd_curl_type)) {
    fd_incref(arg);
    return arg;}
  else if (FD_TABLEP(arg)) {
    fdtype keys=fd_getkeys(arg);
    struct FD_CURL_HANDLE *h=fd_open_curl_handle();
    FD_DO_CHOICES(key,keys) {
      fdtype values=fd_get(arg,key,FD_VOID);
      fdtype setval=set_curlopt(h,key,values);
      if (FD_ABORTP(setval)) {
        fd_decref(keys);
        recycle_curl_handle((struct FD_CONS *)h);
        return setval;}
      fd_decref(values);}
    fd_decref(keys);
    return (fdtype)h;}
  else if ((FD_VOIDP(arg))||(FD_FALSEP(arg)))
    return (fdtype) fd_open_curl_handle();
  else return fd_type_error("curl handle",cxt,arg);
}

/* Primitives */

static fdtype urlget(fdtype url,fdtype curl)
{
  fdtype result, conn=curl_arg(curl,"urlget");
  if (FD_ABORTP(conn)) return conn;
  else if (!(FD_PRIM_TYPEP(conn,fd_curl_type)))
    return fd_type_error("CURLCONN","urlget",conn);
  else if (!((FD_STRINGP(url))||(FD_PRIM_TYPEP(url,fd_secret_type)))) {
    result=fd_type_error("string","urlget",url);}
  else result=fetchurl((fd_curl_handle)conn,FD_STRDATA(url));
  fd_decref(conn);
  return result;
}

static fdtype urlhead(fdtype url,fdtype curl)
{
  fdtype result, conn=curl_arg(curl,"urlhead");
  if (FD_ABORTP(conn)) return conn;
  else if (!(FD_PRIM_TYPEP(conn,fd_curl_type)))
    return fd_type_error("CURLCONN","urlhead",conn);
  else if (!((FD_STRINGP(url))||(FD_PRIM_TYPEP(url,fd_secret_type))))
    result=fd_type_error("string","urlhead",url);
  result=fetchurlhead((fd_curl_handle)conn,FD_STRDATA(url));
  fd_decref(conn);
  return result;
}

static fdtype urlput(fdtype url,fdtype content,fdtype ctype,fdtype curl)
{
  fdtype conn;
  INBUF data; OUTBUF rdbuf; CURLcode retval;
  fdtype result=FD_VOID;
  struct FD_CURL_HANDLE *h=NULL;
  if (!((FD_STRINGP(content))||(FD_PACKETP(content))||(FD_FALSEP(content))))
    return fd_type_error("string or packet","urlput",content);
  else conn=curl_arg(curl,"urlput");
  if (FD_ABORTP(conn)) return conn;
  else if (!(FD_PRIM_TYPEP(conn,fd_curl_type))) {
    return fd_type_error("CURLCONN","urlput",conn);}
  else h=(fd_curl_handle)conn;
  if (FD_STRINGP(ctype))
    curl_add_header(h,"Content-Type",FD_STRDATA(ctype));
  else if ((FD_TABLEP(curl)) && (fd_test(curl,content_type_symbol,FD_VOID))) {}
  else if (FD_STRINGP(content))
    curl_add_header(h,"Content-Type: text",NULL);
  else curl_add_header(h,"Content-Type: application",NULL);
  data.bytes=u8_malloc(8192); data.size=0; data.limit=8192;
  result=fd_empty_slotmap();
  curl_easy_setopt(h->handle,CURLOPT_URL,FD_STRDATA(url));
  curl_easy_setopt(h->handle,CURLOPT_WRITEDATA,&data);
  curl_easy_setopt(h->handle,CURLOPT_WRITEHEADER,&result);
  if ((FD_STRINGP(content))||(FD_PACKETP(content))) {
    curl_easy_setopt(h->handle,CURLOPT_UPLOAD,1);
    curl_easy_setopt(h->handle,CURLOPT_READFUNCTION,copy_upload_data);
    curl_easy_setopt(h->handle,CURLOPT_READDATA,&rdbuf);}
  if (FD_STRINGP(content)) {
    size_t length=FD_STRLEN(content);
    rdbuf.scan=(u8_byte *)FD_STRDATA(content);
    rdbuf.end=(u8_byte *)FD_STRDATA(content)+length;
    curl_easy_setopt(h->handle,CURLOPT_INFILESIZE,length);}
  else if (FD_PACKETP(content)) {
    size_t length=FD_PACKET_LENGTH(content);
    rdbuf.scan=(u8_byte *)FD_PACKET_DATA(content);
    rdbuf.end=(u8_byte *)FD_PACKET_DATA(content)+length;
    curl_easy_setopt(h->handle,CURLOPT_INFILESIZE,length);}
  else {}
  retval=curl_easy_perform(h->handle);
  if (retval!=CURLE_OK) {
    char buf[CURL_ERROR_SIZE];
    fdtype errval=fd_err(CurlError,"urlput",getcurlerror(buf,retval),url);
    fd_decref(result); u8_free(data.bytes);
    fd_decref(conn);
    return errval;}
  else handlefetchresult(h,result,&data);
  fd_decref(conn);
  return result;
}

/* Getting content */

static fdtype urlcontent(fdtype url,fdtype curl)
{
  fdtype result, conn=curl_arg(curl,"urlcontent"), content;
  if (FD_ABORTP(conn)) return conn;
  else if (!(FD_PRIM_TYPEP(conn,fd_curl_type)))
    result=fd_type_error("CURLCONN","urlcontent",conn);
  else result=fetchurl((fd_curl_handle)conn,FD_STRDATA(url));
  fd_decref(conn);
  if (FD_ABORTP(result)) {
    return result;}
  else {
    content=fd_get(result,content_symbol,FD_EMPTY_CHOICE);
    fd_decref(result);
    return content;}
}

static fdtype urlxml(fdtype url,fdtype xmlopt,fdtype curl)
{
  INBUF data;
  fdtype result, cval, conn=curl_arg(curl,"urlxml");
  int flags; long http_response=0;
  FD_CURL_HANDLE *h; CURLcode retval;
  if (FD_ABORTP(conn)) return conn;
  else if (!(FD_PRIM_TYPEP(conn,fd_curl_type)))
    return fd_type_error("CURLCONN","urlxml",conn);
  else {
    h=(fd_curl_handle)conn;
    result=fd_empty_slotmap();}
  flags=fd_xmlparseoptions(xmlopt);
  /* Check that the XML options are okay */
  if (flags<0) {
    fd_decref(result);
    fd_decref(conn);
    return FD_ERROR_VALUE;}
  fd_add(result,url_symbol,url);
  data.bytes=u8_malloc(8192); data.size=0; data.limit=8192;
  curl_easy_setopt(h->handle,CURLOPT_URL,FD_STRDATA(url));
  curl_easy_setopt(h->handle,CURLOPT_WRITEDATA,&data);
  curl_easy_setopt(h->handle,CURLOPT_WRITEHEADER,&result);
  retval=curl_easy_perform(h->handle);
  if (retval!=CURLE_OK) {
    char buf[CURL_ERROR_SIZE];
    fdtype errval=fd_err(CurlError,"urlxml",getcurlerror(buf,retval),url);
    fd_decref(result); u8_free(data.bytes);
    fd_decref(conn);
    return errval;}
  retval=curl_easy_getinfo(h->handle,CURLINFO_RESPONSE_CODE,&http_response);
  if (retval!=CURLE_OK) {
    char buf[CURL_ERROR_SIZE];
    fdtype errval=fd_err(CurlError,"urlxml",getcurlerror(buf,retval),url);
    fd_decref(result); u8_free(data.bytes);
    fd_decref(conn);
    return errval;}
  if (data.size<data.limit) data.bytes[data.size]='\0';
  else {
    data.bytes=u8_realloc(data.bytes,data.size+4);
    data.bytes[data.size]='\0';}
  if (data.size<=0) {
    if (data.size==0)
      cval=fd_init_packet(NULL,data.size,data.bytes);
    else cval=FD_EMPTY_CHOICE;}
  else if ((fd_test(result,type_symbol,text_types))&&
           (!(fd_test(result,content_encoding_symbol,FD_VOID)))) {
    U8_INPUT in; u8_string buf;
    struct FD_XML xmlnode, *xmlret;
    fdtype chset=fd_get(result,charset_symbol,FD_VOID);
    if (FD_STRINGP(chset)) {
      U8_OUTPUT out;
      u8_encoding enc=u8_get_encoding(FD_STRDATA(chset));
      if (enc) {
        const unsigned char *scan=data.bytes;
        U8_INIT_OUTPUT(&out,data.size);
        u8_convert(enc,1,&out,&scan,data.bytes+data.size);
        u8_free(data.bytes); buf=out.u8_outbuf;
        U8_INIT_STRING_INPUT(&in,out.u8_write-out.u8_outbuf,out.u8_outbuf);}
      else {
        U8_INIT_STRING_INPUT(&in,data.size,data.bytes); buf=data.bytes;}}
    else {
      U8_INIT_STRING_INPUT(&in,data.size,data.bytes); buf=data.bytes;}
    if (http_response>=300) {
      fd_seterr("HTTP error response","urlxml",u8_strdup(FD_STRDATA(url)),
                fd_init_string(NULL,in.u8_inlim-in.u8_inbuf,in.u8_inbuf));
      fd_decref(conn);
      return FD_ERROR_VALUE;}
    fd_init_xml_node(&xmlnode,NULL,FD_STRDATA(url));
    xmlnode.fdxml_bits=flags;
    xmlret=fd_walk_xml(&in,fd_default_contentfn,NULL,NULL,NULL,
                       fd_default_popfn,
                       &xmlnode);
    fd_decref(conn);
    if (xmlret) {
      {FD_DOLIST(elt,xmlret->fdxml_head) {
        if (FD_SLOTMAPP(elt)) {
          fdtype name=fd_get(elt,name_symbol,FD_EMPTY_CHOICE);
          if (FD_SYMBOLP(name)) fd_add(result,name,elt);
          else if ((FD_CHOICEP(name)) || (FD_ACHOICEP(name))) {
            FD_DO_CHOICES(nm,name) {
              if (FD_SYMBOLP(nm)) fd_add(result,nm,elt);}}}}}
      fd_add(result,content_symbol,xmlret->fdxml_head);
      u8_free(buf); fd_decref(xmlret->fdxml_head);
      return result;}
    else {
      fd_decref(result); u8_free(buf);
      return FD_ERROR_VALUE;}}
  else {
    fdtype err;
    cval=fd_make_packet(NULL,data.size,data.bytes);
    fd_add(result,content_symbol,cval); fd_decref(cval);
    err=fd_err(NonTextualContent,"urlxml",FD_STRDATA(url),result);
    fd_decref(result);
    u8_free(data.bytes);
    return err;}
  return cval;
}

/* Req checking */

static fdtype responsetest(fdtype response,int min,int max)
{
  fdtype status=((FD_TABLEP(response))?
                 (fd_get(response,response_code_slotid,FD_VOID)):
                 (FD_FIXNUMP(response))?(response):(FD_VOID));
  if ((FD_FIXNUMP(status))&&
      ((FD_FIX2INT(status))>=min)&&
      ((FD_FIX2INT(status))<max))
    return FD_TRUE;
  else {
    if (FD_TABLEP(response)) fd_decref(status);
    return FD_FALSE;}
}

static fdtype responseokp(fdtype response)
{
  return responsetest(response,200,300);
}

static fdtype responseredirectp(fdtype response)
{
  return responsetest(response,300,400);
}

static fdtype responseanyerrorp(fdtype response)
{
  return responsetest(response,400,600);
}

static fdtype responsemyerrorp(fdtype response)
{
  return responsetest(response,400,500);
}

static fdtype responseservererrorp(fdtype response)
{
  return responsetest(response,500,600);
}

static fdtype responseunauthorizedp(fdtype response)
{
  return responsetest(response,401,402);
}

static fdtype responseforbiddenp(fdtype response)
{
  return responsetest(response,401,405);
}

static fdtype responsetimeoutp(fdtype response)
{
  return responsetest(response,408,409);
}

static fdtype responsebadmethodp(fdtype response)
{
  return responsetest(response,405,406);
}

static fdtype responsenotfoundp(fdtype response)
{
  return responsetest(response,404,405);
}

static fdtype responsegonep(fdtype response)
{
  return responsetest(response,410,411);
}

static fdtype responsestatusprim(fdtype response)
{
  fdtype status=((FD_TABLEP(response))?
                 (fd_get(response,response_code_slotid,FD_VOID)):
                 (FD_FIXNUMP(response))?(response):(FD_VOID));
  if (!(FD_FIXNUMP(status))) {
    fd_decref(status);
    return fd_type_error("HTTP response","responsestatusprim",response);}
  else return status;
}
static fdtype testresponseprim(fdtype response,fdtype arg1,fdtype arg2)
{
  if (FD_AMBIGP(response)) {
    FD_DO_CHOICES(r,response) {
      fdtype result=testresponseprim(r,arg1,arg2);
      if (FD_TRUEP(result)) {
        FD_STOP_DO_CHOICES;
        return result;}
      fd_decref(result);}
    return FD_FALSE;}
  else {
    fdtype status=((FD_TABLEP(response))?
                   (fd_get(response,response_code_slotid,FD_VOID)):
                   (FD_FIXNUMP(response))?(response):(FD_VOID));
    if (!(FD_FIXNUMP(status))) {
      if (FD_TABLEP(response)) fd_decref(status);
      return FD_FALSE;}
    if (FD_VOIDP(arg2)) {
      if (fd_choice_containsp(status,arg1))
        return FD_TRUE;
      else return FD_FALSE;}
    else if ((FD_FIXNUMP(arg1))&&(FD_FIXNUMP(arg2))) {
      int min=FD_FIX2INT(arg1), max=FD_FIX2INT(arg2);
      int rval=FD_FIX2INT(status);
      if ((rval>=min)&&(rval<max)) return FD_TRUE;
      else return FD_FALSE;}
    else if (!(FD_FIXNUMP(arg1)))
      return fd_type_error("HTTP status","resptestprim",arg1);
    else return fd_type_error("HTTP status","resptestprim",arg2);}
}

/* Opening URLs with options */

static fdtype curlsetopt(fdtype handle,fdtype opt,fdtype value)
{
  if (FD_FALSEP(handle)) {
    if (FD_EQ(opt,header_symbol))
      fd_add(curl_defaults,opt,value);
    else fd_store(curl_defaults,opt,value);
    return FD_TRUE;}
  else if (FD_PTR_TYPEP(handle,fd_curl_type)) {
    struct FD_CURL_HANDLE *h=FD_GET_CONS(handle,fd_curl_type,fd_curl_handle);
    return set_curlopt(h,opt,value);}
  else return fd_type_error("curl handle","curlsetopt",handle);
}

static fdtype curlopen(int n,fdtype *args)
{
  if (n==0)
    return (fdtype) fd_open_curl_handle();
  else if (n==1) {
    struct FD_CURL_HANDLE *ch=fd_open_curl_handle();
    fdtype spec=args[0], keys=fd_getkeys(spec);
    FD_DO_CHOICES(key,keys) {
      fdtype v=fd_get(spec,key,FD_VOID);
      if (!(FD_VOIDP(v))) {
        fdtype setval=set_curlopt(ch,key,v);
        if (FD_ABORTP(setval)) {
          fdtype conn= (fdtype) ch;
          FD_STOP_DO_CHOICES;
          fd_decref(keys);
          fd_decref(conn);
          fd_decref(v);
          return setval;}
        else fd_decref(setval);}
      fd_decref(v);}
    fd_decref(keys);
    return (fdtype) ch;}
  else if (n%2)
    return fd_err(fd_SyntaxError,"CURLOPEN",NULL,FD_VOID);
  else {
    int i=0;
    struct FD_CURL_HANDLE *ch=fd_open_curl_handle();
    while (i<n) {
      fdtype setv=set_curlopt(ch,args[i],args[i+1]);
      if (FD_ABORTP(setv)) {
        fdtype conn = (fdtype) ch;
        fd_decref(conn);
        return setv;}
      i=i+2;}
    return (fdtype) ch;}
}

/* Posting */

static fdtype urlpost(int n,fdtype *args)
{
  INBUF data; CURLcode retval;
  fdtype result=FD_VOID, conn, urlarg=FD_VOID;
  u8_string url; int start;
  struct FD_CURL_HANDLE *h=NULL;
  struct curl_httppost *post = NULL;
  if (n<2) return fd_err(fd_TooFewArgs,"URLPOST",NULL,FD_VOID);
  else if (FD_STRINGP(args[0])) {
    url=FD_STRDATA(args[0]); urlarg=args[0];}
  else if (FD_PRIM_TYPEP(args[0],fd_secret_type)) {
    url=FD_STRDATA(args[0]); urlarg=args[0];}
  else return fd_type_error("url","urlpost",args[0]);
  if ((FD_PRIM_TYPEP(args[1],fd_curl_type))||(FD_TABLEP(args[1]))) {
    conn=curl_arg(args[1],"urlpost"); start=2;}
  else {
    conn=curl_arg(FD_VOID,"urlpost"); start=1;}
  if (!(FD_PRIM_TYPEP(conn,fd_curl_type))) {
    return fd_type_error("CURLCONN","urlpost",conn);}
  else h=(fd_curl_handle)conn;
  data.bytes=u8_malloc(8192); data.size=0; data.limit=8192;
  result=fd_empty_slotmap();
  curl_easy_setopt(h->handle,CURLOPT_URL,url);
  curl_easy_setopt(h->handle,CURLOPT_WRITEDATA,&data);
  curl_easy_setopt(h->handle,CURLOPT_WRITEHEADER,&result);
  curl_easy_setopt(h->handle,CURLOPT_POST,1);
  if ((n-start)==1) {
    if (FD_STRINGP(args[start])) {
      size_t length=FD_STRLEN(args[start]);
      curl_easy_setopt(h->handle,CURLOPT_POSTFIELDSIZE,length);
      curl_easy_setopt(h->handle,CURLOPT_POSTFIELDS,
                       (char *) (FD_STRDATA(args[start])));}
    else if (FD_PACKETP(args[start])) {
      size_t length=FD_PACKET_LENGTH(args[start]);
      curl_easy_setopt(h->handle,CURLOPT_POSTFIELDSIZE,length);
      curl_easy_setopt(h->handle,CURLOPT_POSTFIELDS,
                       ((char *)(FD_PACKET_DATA(args[start]))));}
    else if (FD_TABLEP(args[start])) {
      /* Construct form data */
      fdtype postdata=args[start];
      fdtype keys=fd_getkeys(postdata);
      struct U8_OUTPUT nameout; u8_byte _buf[128]; int initnameout=1;
      struct curl_httppost *last = NULL;
      FD_DO_CHOICES(key,keys) {
        fdtype val=fd_get(postdata,key,FD_VOID);
        u8_string keyname=NULL; size_t keylen; int nametype=CURLFORM_PTRNAME;
        if (FD_EMPTY_CHOICEP(val)) continue;
        else if (FD_SYMBOLP(key)) {
          keyname=FD_SYMBOL_NAME(key); keylen=strlen(keyname);}
        else if (FD_STRINGP(key)) {
          keyname=FD_STRDATA(key); keylen=FD_STRLEN(key);}
        else {
          if (initnameout) {
            U8_INIT_STATIC_OUTPUT_BUF(nameout,128,_buf); initnameout=0;}
          else nameout.u8_write=nameout.u8_outbuf;
          fd_unparse(&nameout,key);
          keyname=nameout.u8_outbuf;
          keylen=(nameout.u8_write-nameout.u8_outbuf)+1;
          nametype=CURLFORM_COPYNAME;}
        if (keyname==NULL) {
          curl_formfree(post); fd_decref(conn);
          if (!(initnameout)) u8_close_output(&nameout);
          return fd_err(fd_TypeError,"CURLPOST",u8_strdup("bad form var"),key);}
        else if (FD_STRINGP(val)) {
          curl_formadd(&post,&last,
                       nametype,keyname,CURLFORM_NAMELENGTH,keylen,
                       CURLFORM_PTRCONTENTS,FD_STRDATA(val),
                       CURLFORM_CONTENTSLENGTH,((size_t)FD_STRLEN(val)),
                       CURLFORM_END);}
        else if (FD_PACKETP(val))
          curl_formadd(&post,&last,
                       nametype,keyname,CURLFORM_NAMELENGTH,keylen,
                       CURLFORM_PTRCONTENTS,FD_PACKET_DATA(val),
                       CURLFORM_CONTENTSLENGTH,((size_t)FD_PACKET_LENGTH(val)),
                       CURLFORM_END);
        else {
          U8_OUTPUT out; U8_INIT_OUTPUT(&out,128);
          fd_unparse(&out,val);
          curl_formadd(&post,&last,
                       nametype,keyname,CURLFORM_NAMELENGTH,keylen,
                       CURLFORM_COPYCONTENTS,out.u8_outbuf,
                       CURLFORM_CONTENTSLENGTH,
                       ((size_t)(out.u8_write-out.u8_outbuf)),
                       CURLFORM_END);
          u8_free(out.u8_outbuf);}
        fd_decref(val);}
      if (!(initnameout)) u8_close_output(&nameout);
      curl_easy_setopt(h->handle, CURLOPT_HTTPPOST, post);
      fd_decref(keys);}
    else {
      fd_decref(conn);
      return fd_err(fd_TypeError,"CURLPOST",u8_strdup("postdata"),
                    args[start]);}
    retval=curl_easy_perform(h->handle);
    if (post) curl_formfree(post);}
  else {
    /* Construct form data from args */
    int i=start;
    struct curl_httppost *post = NULL, *last = NULL;
    struct U8_OUTPUT nameout; u8_byte _buf[128]; int initnameout=1;
    while (i<n) {
      fdtype key=args[i], val=args[i+1];
      u8_string keyname=NULL; size_t keylen; int nametype=CURLFORM_PTRNAME;
      if (FD_EMPTY_CHOICEP(val)) {i=i+2; continue;}
      else if (FD_SYMBOLP(key)) {
        keyname=FD_SYMBOL_NAME(key); keylen=strlen(keyname);}
      else if (FD_STRINGP(key)) {
        keyname=FD_STRDATA(key); keylen=FD_STRLEN(key);}
      else {
        if (initnameout) {
          U8_INIT_STATIC_OUTPUT_BUF(nameout,128,_buf);
          initnameout=0;}
        else nameout.u8_write=nameout.u8_outbuf;
        fd_unparse(&nameout,key);
        keyname=nameout.u8_outbuf;
        keylen=nameout.u8_write-nameout.u8_outbuf;
        nametype=CURLFORM_COPYNAME;}
      i=i+2;
      if (keyname==NULL) {
        if (!(initnameout)) u8_close_output(&nameout);
        curl_formfree(post); fd_decref(conn);
        return fd_err(fd_TypeError,"CURLPOST",u8_strdup("bad form var"),key);}
      else if (FD_STRINGP(val))
        curl_formadd(&post,&last,
                     nametype,keyname,CURLFORM_NAMELENGTH,keylen,
                     CURLFORM_PTRCONTENTS,FD_STRDATA(val),
                     CURLFORM_CONTENTSLENGTH,((size_t)FD_STRLEN(val)),
                     CURLFORM_END);
      else if (FD_PACKETP(val))
        curl_formadd(&post,&last,
                     nametype,keyname,CURLFORM_NAMELENGTH,keylen,
                     CURLFORM_PTRCONTENTS,FD_PACKET_DATA(val),
                     CURLFORM_CONTENTSLENGTH,((size_t)FD_PACKET_LENGTH(val)),
                     CURLFORM_END);
      else {
        U8_OUTPUT out; U8_INIT_OUTPUT(&out,128);
        fd_unparse(&out,val);
        curl_formadd(&post,&last,
                     nametype,keyname,CURLFORM_NAMELENGTH,keylen,
                     CURLFORM_COPYCONTENTS,out.u8_outbuf,
                     CURLFORM_CONTENTSLENGTH,
                     ((size_t)(out.u8_write-out.u8_outbuf)),
                     CURLFORM_END);
        u8_free(out.u8_outbuf);}}
    if (!(initnameout)) u8_close_output(&nameout);
    curl_easy_setopt(h->handle, CURLOPT_HTTPPOST, post);
    retval=curl_easy_perform(h->handle);
    curl_formfree(post);}
  handlefetchresult(h,result,&data);
  fd_decref(conn);
  if (retval!=CURLE_OK) {
    char buf[CURL_ERROR_SIZE];
    fdtype errval=fd_err(CurlError,"fetchurl",getcurlerror(buf,retval),urlarg);
    return errval;}
  else return result;
}

static fdtype urlpostdata_handler(fdtype expr,fd_lispenv env)
{
  fdtype urlarg=fd_get_arg(expr,1), url=fd_eval(urlarg,env);
  fdtype ctype, curl, conn, body=FD_VOID;
  struct U8_OUTPUT out;
  INBUF data; OUTBUF rdbuf; CURLcode retval;
  struct FD_CURL_HANDLE *h=NULL;
  fdtype result=FD_VOID;

  if (FD_ABORTP(url)) return url;
  else if (!((FD_STRINGP(url))||(FD_PRIM_TYPEP(url,fd_secret_type))))
    return fd_type_error("url","urlpostdata_handler",url);
  else {
    ctype=fd_eval(fd_get_arg(expr,2),env);
    body=fd_get_body(expr,3);}

  if (FD_ABORTP(ctype)) {
    fd_decref(url); return ctype;}
  else if (!(FD_STRINGP(ctype))) {
    fd_decref(url);
    return fd_type_error("mime type","urlpostdata_handler",ctype);}
  else {
    curl=fd_eval(fd_get_arg(expr,3),env);
    body=fd_get_body(expr,4);}

  conn=curl_arg(curl,"urlpostdata");

  if (FD_ABORTP(conn)) {
    fd_decref(url); fd_decref(ctype); fd_decref(curl);
    return conn;}
  else if (!(FD_PRIM_TYPEP(conn,fd_curl_type))) {
    fd_decref(url); fd_decref(ctype); fd_decref(curl);
    return fd_type_error("CURLCONN","urlpostdata_handler",conn);}
  else h=FD_GET_CONS(conn,fd_curl_type,fd_curl_handle);

  curl_add_header(h,"Content-Type",FD_STRDATA(ctype));

  U8_INIT_OUTPUT(&out,8192);

  fd_printout_to(&out,body,env);

  curl_easy_setopt(h->handle,CURLOPT_URL,FD_STRDATA(url));

  rdbuf.scan=out.u8_outbuf; rdbuf.end=out.u8_write;
  curl_easy_setopt(h->handle,CURLOPT_POSTFIELDS,NULL);
  curl_easy_setopt(h->handle,CURLOPT_POSTFIELDSIZE,rdbuf.end-rdbuf.scan);
  curl_easy_setopt(h->handle,CURLOPT_READFUNCTION,copy_upload_data);
  curl_easy_setopt(h->handle,CURLOPT_READDATA,&rdbuf);

  data.bytes=u8_malloc(8192); data.size=0; data.limit=8192;
  curl_easy_setopt(h->handle,CURLOPT_WRITEDATA,&data);
  curl_easy_setopt(h->handle,CURLOPT_WRITEHEADER,&result);
  curl_easy_setopt(h->handle,CURLOPT_POST,1);
  result=fd_empty_slotmap();

  retval=curl_easy_perform(h->handle);

  if (retval!=CURLE_OK) {
    char buf[CURL_ERROR_SIZE];
    fdtype errval=fd_err(CurlError,"fetchurl",getcurlerror(buf,retval),urlarg);
    fd_decref(url); fd_decref(ctype); fd_decref(curl);
    fd_decref(conn);
    return errval;}

  handlefetchresult(h,result,&data);
  fd_decref(url); fd_decref(ctype); fd_decref(curl);
  fd_decref(conn);
  return result;
}

/* Using URLs for code source */

static u8_string url_source_fn(int fetch,u8_string uri,u8_string enc_name,
                               u8_string *path,time_t *timep,
                               void *ignored)
{
  if (((strncmp(uri,"http:",5))==0) ||
      ((strncmp(uri,"https:",6))==0) ||
      ((strncmp(uri,"ftp:",4))==0)) {
    if (fetch)  {
      fdtype result=fetchurl(NULL,uri);
      fdtype content=fd_get(result,content_symbol,FD_EMPTY_CHOICE);
      if (FD_PACKETP(content)) {
        u8_encoding enc=
          ((enc_name==NULL)?(u8_get_default_encoding()):
           (strcasecmp(enc_name,"auto")==0)?
           (u8_guess_encoding(FD_PACKET_DATA(content))):
           (u8_get_encoding(enc_name)));
        if (!(enc)) enc=u8_get_default_encoding();
        u8_string string_form=u8_make_string
          (enc,FD_PACKET_DATA(content),
           FD_PACKET_DATA(content)+FD_PACKET_LENGTH(content));
        fd_decref(content);
        content=fdtype_string(string_form);}
      if (FD_STRINGP(content)) {
        fdtype eurl=fd_get(result,eurl_slotid,FD_VOID);
        fdtype filetime=fd_get(result,filetime_slotid,FD_VOID);
        u8_string sdata=u8_strdup(FD_STRDATA(content));
        if ((FD_STRINGP(eurl))&&(path)) *path=u8_strdup(FD_STRDATA(eurl));
        if ((FD_PRIM_TYPEP(filetime,fd_timestamp_type))&&(timep))
          *timep=u8_mktime(&(((fd_timestamp)filetime)->fd_u8xtime));
        fd_decref(filetime); fd_decref(eurl);
        fd_decref(content); fd_decref(result);
        return sdata;}
      else {
        fd_decref(content); fd_decref(result);
        return NULL;}}
    else {
      fdtype result=fetchurlhead(NULL,uri);
      fdtype status=fd_get(result,response_code_slotid,FD_VOID);
      if ((FD_VOIDP(status))||
          ((FD_FIXNUMP(status))&&
           (FD_FIX2INT(status)<200)&&
           (FD_FIX2INT(status)>=400)))
        return NULL;
      else {
        fdtype eurl=fd_get(result,eurl_slotid,FD_VOID);
        fdtype filetime=fd_get(result,filetime_slotid,FD_VOID);
        if ((FD_STRINGP(eurl))&&(path)) *path=u8_strdup(FD_STRDATA(eurl));
        if ((FD_PRIM_TYPEP(filetime,fd_timestamp_type))&&(timep))
          *timep=u8_mktime(&(((fd_timestamp)filetime)->fd_u8xtime));
        return "exists";}}}
  else return NULL;
}

/* This is largely based on code from
     http://curl.haxx.se/libcurl/c/threaded-ssl.html
   to handle problems with multi-threaded https: calls.
*/

#if LOCK_OPENSSL
/* we have this global to let the callback get easy access to it */
static pthread_mutex_t *ssl_lockarray;

#include <openssl/crypto.h>
static void lock_callback(int mode, int type, char U8_MAYBE_UNUSED *file, int U8_MAYBE_UNUSED line)
{
  if (mode & CRYPTO_LOCK) {
    pthread_mutex_lock(&(ssl_lockarray[type]));
  }
  else {
    pthread_mutex_unlock(&(ssl_lockarray[type]));
  }
}

static unsigned long thread_id(void)
{
  unsigned long ret;

  ret=(unsigned long)pthread_self();
  return ret;
}

static void init_ssl_locks(void)
{
  int i;

  ssl_lockarray=
    (pthread_mutex_t *)OPENSSL_malloc
    (CRYPTO_num_locks() * sizeof(pthread_mutex_t));
  for (i=0; i<CRYPTO_num_locks(); i++) {
    pthread_mutex_init(&(ssl_lockarray[i]),NULL);}

  CRYPTO_set_id_callback((unsigned long (*)())thread_id);
  CRYPTO_set_locking_callback((void (*)())lock_callback);
}

static void destroy_ssl_locks(void)
{
  int i;

  CRYPTO_set_locking_callback(NULL);
  for (i=0; i<CRYPTO_num_locks(); i++)
    pthread_mutex_destroy(&(ssl_lockarray[i]));

  OPENSSL_free(ssl_lockarray);
}
#else
#define init_ssl_locks()
#define destroy_ssl_locks()
#endif

/* Initialization stuff */

static void global_curl_cleanup()
{
  destroy_ssl_locks();
  curl_global_cleanup();
}

static int curl_initialized=0;

FD_EXPORT void fd_init_curl_c(void) FD_LIBINIT_FN;

FD_EXPORT void fd_init_curl_c()
{
  fdtype module;
  if (curl_initialized) return;
  curl_initialized=1;
  fd_init_fdscheme();

  module=fd_new_module("FDWEB",(0));

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
  eurl_slotid=fd_intern("EFFECTIVE-URL");
  filetime_slotid=fd_intern("FILETIME");
  response_code_slotid=fd_intern("RESPONSE");

  timeout_symbol=fd_intern("TIMEOUT");
  connect_timeout_symbol=fd_intern("CONNECT-TIMEOUT");
  accept_timeout_symbol=fd_intern("ACCEPT-TIMEOUT");
  dns_symbol=fd_intern("DNS");
  dnsip_symbol=fd_intern("DNSIP");
  dns_cachelife_symbol=fd_intern("DNSCACHELIFE");
  fresh_connect_symbol=fd_intern("FRESHCONNECT");
  forbid_reuse_symbol=fd_intern("NOREUSE");
  filetime_symbol=fd_intern("FILETIME");

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
    ("CURL:DEBUG",_("Whether to debug low level CURL interaction"),
     fd_boolconfig_get,fd_boolconfig_set,&debugging_curl);


  fd_register_sourcefn(url_source_fn,NULL);

  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then make -C ../.. debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
