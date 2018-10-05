/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_LOGLEVEL (u8_getloglevel(curl_loglevel))

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

#if HAVE_CRYPTO_SET_LOCKING_CALLBACK
/* we have this global to let the callback get easy access to it */
static pthread_mutex_t *ssl_lockarray;
#include <openssl/crypto.h>
#define LOCK_OPENSSL 1
#else
#define LOCK_OPENSSL 0
#endif

static int curl_loglevel = LOG_NOTIFY;

static u8_string default_user_agent="FramerD/CURL";

static lispval curl_defaults, url_symbol;
static lispval content_type_symbol, charset_symbol, pcontent_symbol;
static lispval content_length_symbol, etag_symbol, content_encoding_symbol;
static lispval verbose_symbol, header_symbol;
static lispval referer_symbol, useragent_symbol, cookie_symbol;
static lispval date_symbol, last_modified_symbol, name_symbol;
static lispval cookiejar_symbol, authinfo_symbol, basicauth_symbol;
static lispval maxtime_symbol, timeout_symbol, method_symbol;
static lispval verifyhost_symbol, verifypeer_symbol, cainfo_symbol;
static lispval eurl_slotid, filetime_slotid, response_code_slotid;
static lispval timeout_symbol, connect_timeout_symbol, accept_timeout_symbol;
static lispval dns_symbol, dnsip_symbol, dns_cachelife_symbol;
static lispval fresh_connect_symbol, forbid_reuse_symbol, filetime_symbol;

static lispval text_types = EMPTY;

static u8_condition NonTextualContent=
  _("can't parse non-textual content as XML");
static u8_condition CurlError=_("Internal libcurl error");

typedef struct FD_CURL_HANDLE {
  FD_CONS_HEADER;
  CURL *handle;
  struct curl_slist *headers;
  /* char curl_errbuf[CURL_ERROR_SIZE]; */
  lispval initdata;} FD_CURL_HANDLE;
typedef struct FD_CURL_HANDLE *fd_curl_handle;

FD_EXPORT struct FD_CURL_HANDLE *fd_open_curl_handle(void);

typedef struct INBUF {
  unsigned char *bytes;
  int size, limit;} INBUF;

typedef struct OUTBUF {
  unsigned char *scan, *end;} OUTBUF;

static size_t copy_upload_data(void *ptr,size_t size,size_t nmemb,void *stream)
{
  OUTBUF *rbuf = (OUTBUF *)stream;
  int bytes_to_read = size*nmemb;
  if (bytes_to_read<(rbuf->end-rbuf->scan)) {
    memcpy(ptr,rbuf->scan,bytes_to_read);
    rbuf->scan = rbuf->scan+bytes_to_read;
    return bytes_to_read;}
  else {
    int n_left = rbuf->end-rbuf->scan;
    memcpy(ptr,rbuf->scan,n_left);
    rbuf->scan = rbuf->end;
    return n_left;}
}

static size_t copy_content_data(char *data,size_t size,size_t n,void *vdbuf)
{
  INBUF *dbuf = (INBUF *)vdbuf; u8_byte *databuf;
  if (dbuf->size+size*n > dbuf->limit) {
    char *newptr;
    int need_space = dbuf->size+size*n, new_limit = dbuf->limit;
    while (new_limit<need_space) {
      if (new_limit >= 65536)
        new_limit = new_limit+65536;
      else new_limit = new_limit*2;}
    newptr = u8_realloc((char *)dbuf->bytes,new_limit);
    dbuf->bytes = newptr;
    dbuf->limit = new_limit;}
  databuf = (unsigned char *)dbuf->bytes;
  memcpy(databuf+dbuf->size,data,size*n);
  dbuf->size = dbuf->size+size*n;
  return size*n;
}

static size_t process_content_data(char *data,size_t elt_size,size_t n_elts,
                                   void *vdhandler)
{
  // TODO: Possibly return CURL_READFUNC_PAUSE and CURL_WRITEFUNC_PAUSE
  lispval *state = (lispval *) vdhandler;
  lispval handler=state[0], result=state[1];
  u8_logf(LOG_INFO,"ProcessContentData",
          "Using %q to process %lld bytes into %q",
          handler,n_elts*elt_size,result);
  int data_len=n_elts*elt_size;
  lispval packet=fd_make_packet(NULL,data_len,data);
  lispval argvec[2]={packet,result};
  lispval apply_result=fd_apply(handler,2,argvec);
  fd_decref(packet);
  if (FD_ABORTP(apply_result)) {
    u8_exception ex=u8_erreify();
    state[2]=fd_wrap_exception(ex);
    return 0;}
  else if (FALSEP(apply_result))
    return 0;
  else if (FIXNUMP(result))
    return FIX2INT(result);
  else return n_elts*elt_size;
}

fd_ptr_type fd_curl_type;

void handle_content_type(char *value,lispval table)
{
  char *chset, *chset_end; int endbyte;
  char *end = value, *slash = strchr(value,'/');
  lispval major_type, full_type;
  while ((*end) && (!((*end==';') || (isspace(*end))))) end++;
  if (slash) *slash='\0';
  endbyte = *end; *end='\0';
  major_type = fd_parse(value);
  fd_store(table,FDSYM_TYPE,major_type);
  if (slash) *slash='/';
  full_type = lispval_string(value);
  fd_add(table,FDSYM_TYPE,full_type); *end = endbyte;
  fd_decref(major_type); fd_decref(full_type);
  if ((chset = (strstr(value,"charset=")))) {
    lispval chset_val;
    chset_end = chset = chset+8; if (*chset=='"') {
      chset++; chset_end = strchr(chset,'"');}
    else chset_end = strchr(chset,';');
    if (chset_end) *chset_end='\0';
    chset_val = lispval_string(chset);
    fd_store(table,charset_symbol,chset_val);
    fd_decref(chset_val);}
}

static size_t handle_header(void *ptr,size_t size,size_t n,void *data)
{
  lispval *valptr = (lispval *)data, val = *valptr;
  lispval slotid, hval; int byte_len = size*n;
  char *cdata = (char *)ptr, *copy = u8_malloc(byte_len+1), *valstart;
  strncpy(copy,cdata,byte_len); copy[byte_len]='\0';
  /* Strip off the CRLF */
  if ((copy[byte_len-1]='\n') && (copy[byte_len-2]='\r')) {
    if (byte_len==2) {
      u8_free(copy); return byte_len;}
    else copy[byte_len-2]='\0';}
  else {}
  if ((valstart = (strchr(copy,':')))) {
    int add = 1;
    *valstart++='\0'; while (isspace(*valstart)) valstart++;
    if (!(TABLEP(val)))
      *valptr = val = fd_empty_slotmap();
    slotid = fd_parse(copy);
    if (FD_EQ(slotid,content_type_symbol)) {
      handle_content_type(valstart,val);
      hval = lispval_string(valstart);
      add = 0;}
    else if ((FD_EQ(slotid,content_length_symbol)) ||
             (FD_EQ(slotid,etag_symbol)) ||
             (FD_EQ(slotid,response_code_slotid))) {
      hval = fd_parse(valstart);
      add = 0;}
    else if ((FD_EQ(slotid,date_symbol)) ||
             (FD_EQ(slotid,last_modified_symbol))) {
      time_t now, moment = curl_getdate(valstart,&now);
      struct U8_XTIME xt;
      u8_init_xtime(&xt,moment,u8_second,0,0,0);
      hval = fd_make_timestamp(&xt);
      add = 0;}
    else hval = lispval_string(valstart);
    if (add) fd_add(val,slotid,hval);
    else fd_store(val,slotid,hval);
    fd_decref(hval);
    u8_free(copy);}
  else {
    hval = lispval_string(copy);
    fd_add(val,header_symbol,hval);
    fd_decref(hval);
    u8_free(copy);}
  return byte_len;
}

FD_INLINE_FCN lispval addtexttype(lispval type)
{
  fd_incref(type);
  CHOICE_ADD(text_types,type);
  return VOID;
}

static void decl_text_type(u8_string string)
{
  lispval stringval = lispval_string(string);
  CHOICE_ADD(text_types,stringval);
}

FD_INLINE_FCN struct FD_CURL_HANDLE *curl_err(u8_string cxt,int code)
{
  u8_string details=u8_fromlibc((char *)curl_easy_strerror(code));
  fd_seterr(CurlError,cxt,details,VOID);
  u8_free(details);
  return NULL;
}

static int _curl_set(u8_string cxt,struct FD_CURL_HANDLE *h,
                     CURLoption option,void *v)
{
  CURLcode retval = curl_easy_setopt(h->handle,option,v);
  if (retval) {
    u8_string details=u8_fromlibc((char *)curl_easy_strerror(retval));
    u8_free(h);
    fd_seterr(CurlError,cxt,details,VOID);
    u8_free(details);}
  return retval;
}

static int _curl_set2dtype(u8_string cxt,struct FD_CURL_HANDLE *h,
                           CURLoption option,
                           lispval f,lispval slotid)
{
  lispval v = fd_get(f,slotid,VOID);
  if (FD_ABORTP(v))
    return fd_interr(v);
  else if ((STRINGP(v))||
           (PACKETP(v))||
           (TYPEP(v,fd_secret_type))) {
    CURLcode retval = curl_easy_setopt(h->handle,option,CSTRING(v));
    if (retval) {
      u8_string details=u8_fromlibc((char *)curl_easy_strerror(retval));
      fd_seterr(CurlError,cxt,details,VOID);
      u8_free(h); fd_decref(v);}
    return retval;}
  else if (FIXNUMP(v)) {
    CURLcode retval = curl_easy_setopt(h->handle,option,(long)(fd_getint(v)));
    if (retval) {
      u8_string details=u8_fromlibc((char *)curl_easy_strerror(retval));
      fd_seterr(CurlError,cxt,details,FD_VOID);
      u8_free(h); fd_decref(v);}
    return retval;}
  else {
    fd_seterr(fd_TypeError,cxt,"string",v);
    return -1;}
}

static int curl_add_header(fd_curl_handle ch,u8_string arg1,u8_string arg2)
{
  struct curl_slist *cur = ch->headers, *newh;
  if (arg2==NULL)
    newh = curl_slist_append(cur,arg1);
  else {
    u8_string hdr = u8_mkstring("%s: %s",arg1,arg2);
    newh = curl_slist_append(cur,hdr);
    u8_free(hdr);}
  ch->headers = newh;
  if (curl_easy_setopt(ch->handle,CURLOPT_HTTPHEADER,(void *)newh)!=CURLE_OK) {
    return -1;}
  else return 1;
}

static int curl_add_headers(fd_curl_handle ch,lispval val)
{
  int retval = 0;
  DO_CHOICES(v,val)
    if (STRINGP(v))
      retval = curl_add_header(ch,CSTRING(v),NULL);
    else if (PACKETP(v))
      retval = curl_add_header(ch,FD_PACKET_DATA(v),NULL);
    else if (PAIRP(v)) {
      lispval car = FD_CAR(v), cdr = FD_CDR(v); u8_string hdr = NULL;
      if ((SYMBOLP(car)) && (STRINGP(cdr)))
        hdr = u8_mkstring("%s: %s",SYM_NAME(car),CSTRING(cdr));
      else if ((STRINGP(car)) && (STRINGP(cdr)))
        hdr = u8_mkstring("%s: %s",CSTRING(car),CSTRING(cdr));
      else if (SYMBOLP(car))
        hdr = u8_mkstring("%s: %q",SYM_NAME(car),cdr);
      else if (STRINGP(car))
        hdr = u8_mkstring("%s: %q",CSTRING(car),cdr);
      else hdr = u8_mkstring("%q: %q",car,cdr);
      retval = curl_add_header(ch,hdr,NULL);
      u8_free(hdr);}
    else if (SLOTMAPP(v)) {
      lispval keys = fd_getkeys(v);
      DO_CHOICES(key,keys) {
        if ((retval>=0)&&((STRINGP(key))||(SYMBOLP(key)))) {
          lispval kval = fd_get(v,key,EMPTY); u8_string hdr = NULL;
          if (SYMBOLP(key)) {
            if (STRINGP(kval))
              hdr = u8_mkstring("%s: %s",SYM_NAME(key),CSTRING(kval));
            else hdr = u8_mkstring("%s: %q",SYM_NAME(key),kval);}
          else if (STRINGP(kval))
            hdr = u8_mkstring("%s: %s",CSTRING(key),CSTRING(kval));
          else hdr = u8_mkstring("%s: %q",CSTRING(key),kval);
          retval = curl_add_header(ch,hdr,NULL);
          u8_free(hdr);
          fd_decref(kval);}}
      fd_decref(keys);}
    else {}
  return retval;
}

FD_EXPORT
struct FD_CURL_HANDLE *fd_open_curl_handle()
{
  struct FD_CURL_HANDLE *h = u8_alloc(struct FD_CURL_HANDLE);
#define curl_set(hl,o,v) \
   if (_curl_set("fd_open_curl_handle",hl,o,(void *)v)) return NULL;
#define curl_set2dtype(hl,o,f,s) \
   if (_curl_set2dtype("fd_open_curl_handle",hl,o,f,s)) return NULL;
  FD_INIT_CONS(h,fd_curl_type);
  h->handle = curl_easy_init();
  h->headers = NULL;
  h->initdata = EMPTY;
  if (h->handle == NULL) {
    u8_free(h);
    fd_seterr(CurlError,"fd_open_curl_handle",
              "curl_easy_init failed",VOID);
    return NULL;}
  u8_logf(LOG_INFO,"CURL","Creating CURL handle %llx",(FD_INTPTR) h->handle);
  if (curl_loglevel > LOG_NOTIFY) {
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

  if (fd_test(curl_defaults,verifypeer_symbol,VOID))
    curl_easy_setopt(h->handle,CURLOPT_SSL_VERIFYPEER,0);
  if (fd_test(curl_defaults,verifyhost_symbol,VOID))
    curl_easy_setopt(h->handle,CURLOPT_SSL_VERIFYHOST,0);
  curl_easy_setopt(h->handle,CURLOPT_SSLVERSION,CURL_SSLVERSION_DEFAULT);

  if (fd_test(curl_defaults,useragent_symbol,VOID)) {
    curl_set2dtype(h,CURLOPT_USERAGENT,curl_defaults,useragent_symbol);}
  else curl_set(h,CURLOPT_USERAGENT,default_user_agent);
  if (fd_test(curl_defaults,basicauth_symbol,VOID)) {
    curl_set(h,CURLOPT_HTTPAUTH,CURLAUTH_BASIC);
    curl_set2dtype(h,CURLOPT_USERPWD,curl_defaults,basicauth_symbol);}
  if (fd_test(curl_defaults,authinfo_symbol,VOID)) {
    curl_set(h,CURLOPT_HTTPAUTH,CURLAUTH_ANY);
    curl_set2dtype(h,CURLOPT_USERPWD,curl_defaults,authinfo_symbol);}
  if (fd_test(curl_defaults,referer_symbol,VOID))
    curl_set2dtype(h,CURLOPT_REFERER,curl_defaults,referer_symbol);
  if (fd_test(curl_defaults,cookie_symbol,VOID))
    curl_set2dtype(h,CURLOPT_REFERER,curl_defaults,cookie_symbol);
  if (fd_test(curl_defaults,cookiejar_symbol,VOID))
    curl_set2dtype(h,CURLOPT_REFERER,curl_defaults,cookiejar_symbol);
  if (fd_test(curl_defaults,maxtime_symbol,VOID))
    curl_set2dtype(h,CURLOPT_TIMEOUT,curl_defaults,maxtime_symbol);
  if (fd_test(curl_defaults,timeout_symbol,VOID))
    curl_set2dtype(h,CURLOPT_CONNECTTIMEOUT,curl_defaults,timeout_symbol);

  {
    lispval http_headers = fd_get(curl_defaults,header_symbol,EMPTY);
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

static void reset_curl_handle(struct FD_CURL_HANDLE *ch)
{
  if (ch->headers) {
    curl_slist_free_all(ch->headers);
    ch->headers=NULL;
    curl_easy_setopt(ch->handle,CURLOPT_HTTPHEADER,(void *)NULL);}
  if (FD_CONSP(ch->initdata)) {
    fd_decref(ch->initdata);
    ch->initdata=EMPTY;}
}

static void recycle_curl_handle(struct FD_RAW_CONS *c)
{
  struct FD_CURL_HANDLE *ch = (struct FD_CURL_HANDLE *)c;
  u8_logf(LOG_INFO,"CURL","Freeing CURL handle %llx",(FD_INTPTR) ch->handle);
  /* curl_easy_setopt(ch->handle,CURLOPT_ERRORBUFFER,NULL); */
  curl_slist_free_all(ch->headers);
  curl_easy_cleanup(ch->handle);
  fd_decref(ch->initdata);
  if (FD_MALLOCD_CONSP(c)) u8_free(c);
}

static int unparse_curl_handle(u8_output out,lispval x)
{
  u8_printf(out,"#<CURL %lx>",x);
  return 1;
}

static lispval curlhandlep(lispval arg)
{
  if (TYPEP(arg,fd_curl_type)) return FD_TRUE;
  else return FD_FALSE;
}

static lispval curlreset(lispval arg)
{
  struct FD_CURL_HANDLE *ch = (struct FD_CURL_HANDLE *)arg;

  curl_easy_reset(ch->handle);

  curl_easy_setopt(ch->handle,CURLOPT_NOPROGRESS,1);
  curl_easy_setopt(ch->handle,CURLOPT_FILETIME,(long)1);
  curl_easy_setopt(ch->handle,CURLOPT_NOSIGNAL,1);
  curl_easy_setopt(ch->handle,CURLOPT_WRITEFUNCTION,copy_content_data);
  curl_easy_setopt(ch->handle,CURLOPT_HEADERFUNCTION,handle_header);

  return VOID;
}

static lispval set_curlopt
  (struct FD_CURL_HANDLE *ch,lispval opt,lispval val)
{
  if (FD_EQ(opt,referer_symbol))
    if (STRINGP(val))
      curl_easy_setopt(ch->handle,CURLOPT_REFERER,CSTRING(val));
    else return fd_type_error("string","set_curlopt",val);
  else if (FD_EQ(opt,method_symbol))
    if (SYMBOLP(val))
      curl_easy_setopt(ch->handle,CURLOPT_CUSTOMREQUEST,SYM_NAME(val));
    else if (STRINGP(val))
      curl_easy_setopt(ch->handle,CURLOPT_CUSTOMREQUEST,CSTRING(val));
    else return fd_type_error("symbol/method","set_curlopt",val);
  else if (FD_EQ(opt,verbose_symbol)) {
    if ((FALSEP(val))||(EMPTYP(val)))
      curl_easy_setopt(ch->handle,CURLOPT_VERBOSE,0);
    else curl_easy_setopt(ch->handle,CURLOPT_VERBOSE,1);}
  else if (FD_EQ(opt,useragent_symbol))
    if (STRINGP(val))
      curl_easy_setopt(ch->handle,CURLOPT_USERAGENT,CSTRING(val));
    else if (FALSEP(val)) {}
    else return fd_type_error("string","set_curlopt",val);
  else if (FD_EQ(opt,authinfo_symbol))
    if (STRINGP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_HTTPAUTH,CURLAUTH_ANY);
      curl_easy_setopt(ch->handle,CURLOPT_USERPWD,CSTRING(val));}
    else return fd_type_error("string","set_curlopt",val);
  else if (FD_EQ(opt,basicauth_symbol))
    if ((STRINGP(val))||(TYPEP(val,fd_secret_type))) {
      curl_easy_setopt(ch->handle,CURLOPT_HTTPAUTH,CURLAUTH_BASIC);
      curl_easy_setopt(ch->handle,CURLOPT_USERPWD,CSTRING(val));}
    else return fd_type_error("string","set_curlopt",val);
  else if (FD_EQ(opt,cookie_symbol))
    if ((STRINGP(val))||(TYPEP(val,fd_secret_type)))
      curl_easy_setopt(ch->handle,CURLOPT_COOKIE,CSTRING(val));
    else return fd_type_error("string","set_curlopt",val);
  else if (FD_EQ(opt,cookiejar_symbol))
    if (STRINGP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_COOKIEFILE,CSTRING(val));
      curl_easy_setopt(ch->handle,CURLOPT_COOKIEJAR,CSTRING(val));}
    else return fd_type_error("string","set_curlopt",val);
  else if (FD_EQ(opt,header_symbol))
    curl_add_headers(ch,val);
  else if (FD_EQ(opt,cainfo_symbol))
    if (STRINGP(val))
      curl_easy_setopt(ch->handle,CURLOPT_CAINFO,CSTRING(val));
    else return fd_type_error("string","set_curlopt",val);
  else if (FD_EQ(opt,verifyhost_symbol))
    if (FD_UINTP(val))
      curl_easy_setopt(ch->handle,CURLOPT_SSL_VERIFYHOST,FIX2INT(val));
    else if (FALSEP(val))
      curl_easy_setopt(ch->handle,CURLOPT_SSL_VERIFYHOST,0);
    else if (FD_TRUEP(val))
      curl_easy_setopt(ch->handle,CURLOPT_SSL_VERIFYHOST,2);
    else return fd_type_error("symbol/method","set_curlopt",val);
  else if ((FD_EQ(opt,timeout_symbol))||(FD_EQ(opt,maxtime_symbol))) {
    if (FIXNUMP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_TIMEOUT,FIX2INT(val));}
    else if (FD_FLONUMP(val)) {
      double secs = FD_FLONUM(val);
      long int msecs = (long int)floor(secs*1000);
      curl_easy_setopt(ch->handle,CURLOPT_TIMEOUT_MS,msecs);}
    else return fd_type_error("seconds","set_curlopt/timeout",val);}
  else if (FD_EQ(opt,connect_timeout_symbol)) {
    if (FIXNUMP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_CONNECTTIMEOUT,FIX2INT(val));}
    else if (FD_FLONUMP(val)) {
      double secs = FD_FLONUM(val);
      long int msecs = (long int)floor(secs*1000);
      curl_easy_setopt(ch->handle,CURLOPT_CONNECTTIMEOUT_MS,msecs);}
    else return fd_type_error("seconds","set_curlopt/connecttimeout",val);}
  else if (FD_EQ(opt,connect_timeout_symbol)) {
    if (FIXNUMP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_CONNECTTIMEOUT,FIX2INT(val));}
    else if (FD_FLONUMP(val)) {
      double secs = FD_FLONUM(val);
      long int msecs = (long int)floor(secs*1000);
      curl_easy_setopt(ch->handle,CURLOPT_CONNECTTIMEOUT_MS,msecs);}
    else return fd_type_error("seconds","set_curlopt/connecttimeout",val);}
  else if (FD_EQ(opt,accept_timeout_symbol)) {
    if (FIXNUMP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_ACCEPTTIMEOUT_MS,
                       (1000*FIX2INT(val)));}
    else if (FD_FLONUMP(val)) {
      double secs = FD_FLONUM(val);
      long int msecs = (long int)floor(secs*1000);
      curl_easy_setopt(ch->handle,CURLOPT_ACCEPTTIMEOUT_MS,msecs);}
    else return fd_type_error("seconds","set_curlopt/connecttimeout",val);}
  else if (FD_EQ(opt,dns_symbol)) {
    if ((STRINGP(val))&&(STRLEN(val)>0)) {
      fd_incref(val); CHOICE_ADD(ch->initdata,val);
      curl_easy_setopt(ch->handle,CURLOPT_DNS_SERVERS,
                       CSTRING(val));}
    else if ((FALSEP(val))||(STRINGP(val))) {
      curl_easy_setopt(ch->handle,CURLOPT_DNS_SERVERS,NULL);}
    else if (SYMBOLP(val)) {
      lispval cval = fd_config_get(SYM_NAME(val));
      if (!(STRINGP(cval)))
        return fd_type_error("string config","set_curlopt/dns",val);
      fd_incref(cval); CHOICE_ADD(ch->initdata,cval);
      curl_easy_setopt(ch->handle,CURLOPT_DNS_SERVERS,
                       CSTRING(cval));}
    else return fd_type_error("string","set_curlopt/dns",val);}
  else if (FD_EQ(opt,dnsip_symbol)) {
    if ((STRINGP(val))&&(STRLEN(val)>0)) {
      fd_incref(val); CHOICE_ADD(ch->initdata,val);
      if (strchr(CSTRING(val),':')) {
        curl_easy_setopt(ch->handle,CURLOPT_DNS_LOCAL_IP6,
                         CSTRING(val));}
      else {
        curl_easy_setopt(ch->handle,CURLOPT_DNS_LOCAL_IP4,
                         CSTRING(val));}}
    else if ((FALSEP(val))||(STRINGP(val))) {
      curl_easy_setopt(ch->handle,CURLOPT_DNS_LOCAL_IP4,NULL);
      curl_easy_setopt(ch->handle,CURLOPT_DNS_LOCAL_IP6,NULL);}
    else if (SYMBOLP(val)) {
      lispval cval = fd_config_get(SYM_NAME(val));
      if (!(STRINGP(cval)))
        return fd_type_error("string config","set_curlopt/dns",val);
      fd_incref(cval); CHOICE_ADD(ch->initdata,cval);
      if (STRLEN(cval)==0) {
        curl_easy_setopt(ch->handle,CURLOPT_DNS_LOCAL_IP4,NULL);
        curl_easy_setopt(ch->handle,CURLOPT_DNS_LOCAL_IP6,NULL);}
      else if (strchr(CSTRING(cval),':')) {
        curl_easy_setopt(ch->handle,CURLOPT_DNS_LOCAL_IP6,
                         CSTRING(cval));}
      else {
        curl_easy_setopt(ch->handle,CURLOPT_DNS_LOCAL_IP4,
                         CSTRING(cval));}}
    else return fd_type_error("string","set_curlopt/dns",val);}
  else if (FD_EQ(opt,dns_cachelife_symbol)) {
    if (FD_TRUEP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_DNS_CACHE_TIMEOUT,-1);}
    else if (FALSEP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_DNS_CACHE_TIMEOUT,0);}
    else if (FIXNUMP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_DNS_CACHE_TIMEOUT,
                       FIX2INT(val));}
    else if (FD_FLONUMP(val)) {
      double secs = FD_FLONUM(val);
      long int msecs = (long int)floor(secs);
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
    if (FIXNUMP(val))
      curl_easy_setopt(ch->handle,CURLOPT_SSL_VERIFYPEER,FIX2INT(val));
    else if (FALSEP(val))
      curl_easy_setopt(ch->handle,CURLOPT_SSL_VERIFYPEER,0);
    else if (FD_TRUEP(val))
      curl_easy_setopt(ch->handle,CURLOPT_SSL_VERIFYPEER,1);
    else return fd_type_error("fixnum/boolean","set_curlopt",val);
  else if (FD_EQ(opt,content_type_symbol))
    if (STRINGP(val)) {
      u8_string ctype_header = u8_mkstring("Content-type: %s",CSTRING(val));
      lispval hval = fd_lispstring(ctype_header);
      curl_add_headers(ch,hval);
      fd_decref(hval);}
    else return fd_type_error(_("string"),"set_curl_handle/content-type",val);
  else return fd_err(_("Unknown CURL option"),"set_curl_handle",
                     NULL,opt);
  if (CONSP(val)) {
    fd_incref(val); CHOICE_ADD(ch->initdata,val);}
  return FD_TRUE;
}

/* Fix URL */

static const char *digits="0123456789ABCDEF";

static lispval fixurl(u8_string url)
{
  const u8_byte *scan = url; int c;
  struct U8_OUTPUT out; char buf[8];
  U8_INIT_OUTPUT(&out,256); buf[0]='%'; buf[3]='\0';
  scan = url; while ((c = (*scan++))) {
    if ((c>=0x80)||(iscntrl(c))||(isspace(c))) {
      buf[1]=digits[c/16]; buf[2]=digits[c%16];
      u8_puts(&out,buf);}
    else u8_putc(&out,c);}
  return fd_stream2string(&out);
}

/* The core get function */

static lispval handlefetchresult(struct FD_CURL_HANDLE *h,lispval result,INBUF *data);

static lispval fetchurl(struct FD_CURL_HANDLE *h,u8_string urltext)
{
  INBUF data; CURLcode retval;
  int consed_handle = 0;
  lispval result = fd_empty_slotmap();
  lispval url = fixurl(urltext);
  fd_add(result,url_symbol,url); fd_decref(url);
  data.bytes = u8_malloc(8192); data.size = 0; data.limit = 8192;
  if (h == NULL) {h = fd_open_curl_handle(); consed_handle = 1;}
  curl_easy_setopt(h->handle,CURLOPT_URL,CSTRING(url));
  curl_easy_setopt(h->handle,CURLOPT_WRITEDATA,&data);
  curl_easy_setopt(h->handle,CURLOPT_WRITEHEADER,&result);
  curl_easy_setopt(h->handle,CURLOPT_NOBODY,0);
  retval = curl_easy_perform(h->handle);
  if (retval!=CURLE_OK) {
    char buf[CURL_ERROR_SIZE];
    lispval errval=
      fd_err(CurlError,"fetchurl",getcurlerror(buf,retval),url);
    fd_decref(result);
    u8_free(data.bytes);
    if (consed_handle) {fd_decref((lispval)h);}
    return errval;}
  handlefetchresult(h,result,&data);
  if (consed_handle) {fd_decref((lispval)h);}
  return result;
}

static lispval streamurl(struct FD_CURL_HANDLE *h,
                        u8_string urltext,
                        lispval handler,
                        const unsigned char *payload_type,
                        const unsigned char *payload,
                        size_t payload_size)
{
  OUTBUF rdbuf;
  CURLcode retval;
  int consed_handle = 0;
  lispval result = fd_empty_slotmap();
  lispval url = fixurl(urltext);
  lispval stream_data[3]={handler,result,VOID};
  fd_add(result,url_symbol,url); fd_decref(url);
  if (h == NULL) {h = fd_open_curl_handle(); consed_handle = 1;}
  // Possibly add option for setting CURLOPT_PROGRESSFUNCTION with handler
  //  for curl handle and result.
  curl_easy_setopt(h->handle,CURLOPT_URL,CSTRING(url));
  curl_easy_setopt(h->handle,CURLOPT_WRITEFUNCTION,process_content_data);
  curl_easy_setopt(h->handle,CURLOPT_WRITEDATA,(void *)stream_data);
  curl_easy_setopt(h->handle,CURLOPT_WRITEHEADER,&result);
  curl_easy_setopt(h->handle,CURLOPT_NOBODY,0);
  if ((payload_type) && (payload) && (payload_size)) {
    curl_easy_setopt(h->handle,CURLOPT_POST,1);
    rdbuf.scan=(unsigned char *) payload;
    rdbuf.end= (unsigned char *) (payload+payload_size);
    curl_easy_setopt(h->handle,CURLOPT_UPLOAD,1);
    curl_easy_setopt(h->handle,CURLOPT_READFUNCTION,copy_upload_data);
    curl_easy_setopt(h->handle,CURLOPT_READDATA,&rdbuf);}
  retval = curl_easy_perform(h->handle);
  if (retval==CURLE_WRITE_ERROR) {
    if (TYPEP(stream_data[2],fd_exception_type)) {
      fd_exception exo=(fd_exception)stream_data[2];
      fd_restore_exception(exo);
      if (consed_handle) {fd_decref((lispval)h);}
      return FD_ERROR;}
    else {
      if (consed_handle) {fd_decref((lispval)h);}
      return result;}}
  else if (retval!=CURLE_OK) {
    char buf[CURL_ERROR_SIZE];
    lispval errval=
      fd_err(CurlError,"fetchurl",getcurlerror(buf,retval),result);
    fd_decref(result);
    if (consed_handle) {fd_decref((lispval)h);}
    return errval;}
  if (consed_handle) {fd_decref((lispval)h);}
  return result;
}

static lispval fetchurlhead(struct FD_CURL_HANDLE *h,u8_string urltext)
{
  INBUF data; CURLcode retval;
  int consed_handle = 0;
  lispval result = fd_empty_slotmap();
  lispval url = fixurl(urltext);
  fd_add(result,url_symbol,url); fd_decref(url);
  data.bytes = u8_malloc(8192); data.size = 0; data.limit = 8192;
  if (h == NULL) {h = fd_open_curl_handle(); consed_handle = 1;}
  curl_easy_setopt(h->handle,CURLOPT_URL,CSTRING(url));
  curl_easy_setopt(h->handle,CURLOPT_WRITEDATA,&data);
  curl_easy_setopt(h->handle,CURLOPT_WRITEHEADER,&result);
  curl_easy_setopt(h->handle,CURLOPT_NOBODY,1);
  retval = curl_easy_perform(h->handle);
  if (retval!=CURLE_OK) {
    char buf[CURL_ERROR_SIZE];
    lispval errval = fd_err(CurlError,"fetchurl",getcurlerror(buf,retval),url);
    fd_decref(result);
    u8_free(data.bytes);
    if (consed_handle) {fd_decref((lispval)h);}
    return errval;}
  handlefetchresult(h,result,&data);
  if (consed_handle) {fd_decref((lispval)h);}
  return result;
}

static lispval handlefetchresult(struct FD_CURL_HANDLE *h,lispval result,
                                INBUF *data)
{
  lispval cval; long http_response = 0;
  int retval = curl_easy_getinfo(h->handle,CURLINFO_RESPONSE_CODE,&http_response);
  if (retval==0)
    fd_add(result,response_code_slotid,FD_INT(http_response));
  /* Add a trailing NUL, just in case we need it */
  if (data->size<data->limit) {
    unsigned char *buf = (unsigned char *)(data->bytes);
    buf[data->size]='\0';
    data->bytes = buf;}
  else {
    unsigned char *buf=
      u8_realloc((u8_byte *)data->bytes,data->size+4);
    data->bytes = buf;
    buf[data->size]='\0';}
  if (data->size<0) cval = EMPTY;
  else if ((fd_test(result,FDSYM_TYPE,text_types))&&
           (!(fd_test(result,content_encoding_symbol,VOID))))
    if (data->size==0)
      cval = fd_block_string(data->size,data->bytes);
    else {
      lispval chset = fd_get(result,charset_symbol,VOID);
      if (STRINGP(chset)) {
        U8_OUTPUT out;
        u8_encoding enc = u8_get_encoding(CSTRING(chset));
        if (enc) {
          const unsigned char *scan = data->bytes;
          U8_INIT_OUTPUT(&out,data->size);
          u8_convert(enc,1,&out,&scan,data->bytes+data->size);
          cval = fd_block_string(out.u8_write-out.u8_outbuf,out.u8_outbuf);}
        else if (strstr(data->bytes,"\r\n"))
          cval = fd_lispstring(u8_convert_crlfs(data->bytes));
        else cval = fd_lispstring(u8_valid_copy(data->bytes));}
      else if (strstr(data->bytes,"\r\n"))
        cval = fd_lispstring(u8_convert_crlfs(data->bytes));
      else cval = fd_lispstring(u8_valid_copy(data->bytes));
      u8_free(data->bytes);
      fd_decref(chset);}
  else {
    cval = fd_make_packet(NULL,data->size,data->bytes);
    u8_free(data->bytes);}
  fd_store(result,pcontent_symbol,cval);
  {
    char *urlbuf; long filetime;
    CURLcode rv = curl_easy_getinfo(h->handle,CURLINFO_EFFECTIVE_URL,&urlbuf);
    if (rv == CURLE_OK) {
      lispval eurl = lispval_string(urlbuf);
      fd_add(result,eurl_slotid,eurl);
      fd_decref(eurl);}
    rv = curl_easy_getinfo(h->handle,CURLINFO_FILETIME,&filetime);
    if ((rv == CURLE_OK) && (filetime>=0)) {
      lispval ftime = fd_time2timestamp(filetime);
      fd_add(result,filetime_slotid,ftime);
      fd_decref(ftime);}}
  fd_decref(cval);
  return result;
}

/* Handling arguments to URL primitives */

static lispval curl_arg(lispval arg,u8_context cxt)
{
  if (TYPEP(arg,fd_curl_type)) {
    fd_incref(arg);
    return arg;}
  else if (TABLEP(arg)) {
    lispval keys = fd_getkeys(arg);
    struct FD_CURL_HANDLE *h = fd_open_curl_handle();
    DO_CHOICES(key,keys) {
      lispval values = fd_get(arg,key,VOID);
      lispval setval = set_curlopt(h,key,values);
      if (FD_ABORTP(setval)) {
        fd_decref(keys);
        recycle_curl_handle((fd_raw_cons)h);
        return setval;}
      fd_decref(values);}
    fd_decref(keys);
    return (lispval)h;}
  else if ((VOIDP(arg))||(FALSEP(arg)))
    return (lispval) fd_open_curl_handle();
  else return fd_type_error("curl handle",cxt,arg);
}

/* Primitives */

static lispval urlget(lispval url,lispval curl)
{
  lispval result, conn = curl_arg(curl,"urlget");
  if (FD_ABORTP(conn)) return conn;
  else if (!(TYPEP(conn,fd_curl_type)))
    return fd_type_error("CURLCONN","urlget",conn);
  else if (!((STRINGP(url))||(TYPEP(url,fd_secret_type)))) {
    result = fd_type_error("string","urlget",url);}
  else {
    result = fetchurl((fd_curl_handle)conn,CSTRING(url));}
  if (conn == curl) reset_curl_handle((fd_curl_handle)conn);
  fd_decref(conn);
  return result;
}

static lispval urlstream(lispval url,lispval handler,
                         lispval payload,
                         lispval curl)
{
  if (!(FD_APPLICABLEP(handler)))
    return fd_type_error("applicable","urlstream",handler);
  lispval result, conn = curl_arg(curl,"urlstream");
  if (FD_ABORTP(conn)) return conn;
  else if (!(TYPEP(conn,fd_curl_type)))
    return fd_type_error("CURLCONN","urlget",conn);
  else if (!((STRINGP(url))||(TYPEP(url,fd_secret_type)))) {
    result = fd_type_error("string","urlget",url);}
  else if (VOIDP(payload))
    result = streamurl((fd_curl_handle)conn,CSTRING(url),
                       handler,NULL,NULL,0);
  else {
    result = streamurl((fd_curl_handle)conn,CSTRING(url),
                       handler,CSTRING(payload),
                       "application/x-www-urlform-encoded",
                       STRLEN(payload));}
  if (conn == curl) reset_curl_handle((fd_curl_handle)conn);
  fd_decref(conn);
  return result;
}

static lispval urlhead(lispval url,lispval curl)
{
  lispval result, conn = curl_arg(curl,"urlhead");
  if (FD_ABORTP(conn)) return conn;
  else if (!(TYPEP(conn,fd_curl_type)))
    return fd_type_error("CURLCONN","urlhead",conn);
  else if (!((STRINGP(url))||(TYPEP(url,fd_secret_type))))
    result = fd_type_error("string","urlhead",url);
  result = fetchurlhead((fd_curl_handle)conn,CSTRING(url));
  if (conn == curl) reset_curl_handle((fd_curl_handle)conn);
  fd_decref(conn);
  return result;
}

static lispval urlput(lispval url,lispval content,lispval ctype,lispval curl)
{
  lispval conn;
  INBUF data; OUTBUF rdbuf; CURLcode retval;
  lispval result = VOID;
  struct FD_CURL_HANDLE *h = NULL;
  if (!((STRINGP(content))||(PACKETP(content))||(FALSEP(content))))
    return fd_type_error("string or packet","urlput",content);
  else conn = curl_arg(curl,"urlput");
  if (FD_ABORTP(conn)) return conn;
  else if (!(TYPEP(conn,fd_curl_type))) {
    return fd_type_error("CURLCONN","urlput",conn);}
  else h = (fd_curl_handle)conn;
  if (STRINGP(ctype))
    curl_add_header(h,"Content-Type",CSTRING(ctype));
  else if ((TABLEP(curl)) && (fd_test(curl,content_type_symbol,VOID))) {}
  else if (STRINGP(content))
    curl_add_header(h,"Content-Type: text",NULL);
  else curl_add_header(h,"Content-Type: application",NULL);
  data.bytes = u8_malloc(8192); data.size = 0; data.limit = 8192;
  result = fd_empty_slotmap();
  curl_easy_setopt(h->handle,CURLOPT_URL,CSTRING(url));
  curl_easy_setopt(h->handle,CURLOPT_WRITEDATA,&data);
  curl_easy_setopt(h->handle,CURLOPT_WRITEHEADER,&result);
  if ((STRINGP(content))||(PACKETP(content))) {
    curl_easy_setopt(h->handle,CURLOPT_UPLOAD,1);
    curl_easy_setopt(h->handle,CURLOPT_READFUNCTION,copy_upload_data);
    curl_easy_setopt(h->handle,CURLOPT_READDATA,&rdbuf);}
  if (STRINGP(content)) {
    size_t length = STRLEN(content);
    rdbuf.scan = (u8_byte *)CSTRING(content);
    rdbuf.end = (u8_byte *)CSTRING(content)+length;
    curl_easy_setopt(h->handle,CURLOPT_INFILESIZE,length);}
  else if (PACKETP(content)) {
    size_t length = FD_PACKET_LENGTH(content);
    rdbuf.scan = (u8_byte *)FD_PACKET_DATA(content);
    rdbuf.end = (u8_byte *)FD_PACKET_DATA(content)+length;
    curl_easy_setopt(h->handle,CURLOPT_INFILESIZE,length);}
  else {}
  retval = curl_easy_perform(h->handle);
  if (retval!=CURLE_OK) {
    char buf[CURL_ERROR_SIZE];
    lispval errval = fd_err(CurlError,"urlput",getcurlerror(buf,retval),url);
    fd_decref(result);
    u8_free(data.bytes);
    fd_decref(conn);
    return errval;}
  else handlefetchresult(h,result,&data);
  if (conn == curl) reset_curl_handle((fd_curl_handle)conn);
  fd_decref(conn);
  return result;
}

/* Getting content */

static lispval urlcontent(lispval url,lispval curl)
{
  lispval result, conn = curl_arg(curl,"urlcontent"), content;
  if (FD_ABORTP(conn)) return conn;
  else if (!(TYPEP(conn,fd_curl_type)))
    result = fd_type_error("CURLCONN","urlcontent",conn);
  else result = fetchurl((fd_curl_handle)conn,CSTRING(url));
  if (conn == curl) reset_curl_handle((fd_curl_handle)conn);
  fd_decref(conn);
  if (FD_ABORTP(result)) {
    return result;}
  else {
    content = fd_get(result,pcontent_symbol,EMPTY);
    fd_decref(result);
    return content;}
}

static lispval urlxml(lispval url,lispval xmlopt,lispval curl)
{
  INBUF data;
  lispval result, cval, conn = curl_arg(curl,"urlxml");
  int flags; long http_response = 0;
  FD_CURL_HANDLE *h; CURLcode retval;
  if (FD_ABORTP(conn)) return conn;
  else if (!(TYPEP(conn,fd_curl_type)))
    return fd_type_error("CURLCONN","urlxml",conn);
  else {
    h = (fd_curl_handle)conn;
    result = fd_empty_slotmap();}
  flags = fd_xmlparseoptions(xmlopt);
  /* Check that the XML options are okay */
  if (flags<0) {
    fd_decref(result);
    fd_decref(conn);
    return FD_ERROR;}
  fd_add(result,url_symbol,url);
  data.bytes = u8_malloc(8192); data.size = 0; data.limit = 8192;
  curl_easy_setopt(h->handle,CURLOPT_URL,CSTRING(url));
  curl_easy_setopt(h->handle,CURLOPT_WRITEDATA,&data);
  curl_easy_setopt(h->handle,CURLOPT_WRITEHEADER,&result);
  retval = curl_easy_perform(h->handle);
  if (retval!=CURLE_OK) {
    char buf[CURL_ERROR_SIZE];
    lispval errval = fd_err(CurlError,"urlxml",getcurlerror(buf,retval),url);
    fd_decref(result);
    u8_free(data.bytes);
    if (conn == curl) reset_curl_handle((fd_curl_handle)conn);
    fd_decref(conn);
    return errval;}
  retval = curl_easy_getinfo(h->handle,CURLINFO_RESPONSE_CODE,&http_response);
  if (retval!=CURLE_OK) {
    char buf[CURL_ERROR_SIZE];
    lispval errval = fd_err(CurlError,"urlxml",getcurlerror(buf,retval),url);
    fd_decref(result);
    u8_free(data.bytes);
    if (conn == curl) reset_curl_handle((fd_curl_handle)conn);
    fd_decref(conn);
    return errval;}
  if (data.size<data.limit) data.bytes[data.size]='\0';
  else {
    data.bytes = u8_realloc(data.bytes,data.size+4);
    data.bytes[data.size]='\0';}
  if (data.size<=0) {
    if (data.size==0)
      cval = fd_init_packet(NULL,data.size,data.bytes);
    else cval = EMPTY;}
  else if ((fd_test(result,FDSYM_TYPE,text_types))&&
           (!(fd_test(result,content_encoding_symbol,VOID)))) {
    U8_INPUT in; u8_string buf;
    struct FD_XML xmlnode, *xmlret;
    lispval chset = fd_get(result,charset_symbol,VOID);
    if (STRINGP(chset)) {
      U8_OUTPUT out;
      u8_encoding enc = u8_get_encoding(CSTRING(chset));
      if (enc) {
        const unsigned char *scan = data.bytes;
        U8_INIT_OUTPUT(&out,data.size);
        u8_convert(enc,1,&out,&scan,data.bytes+data.size);
        u8_free(data.bytes);
        buf = out.u8_outbuf;
        U8_INIT_STRING_INPUT(&in,out.u8_write-out.u8_outbuf,out.u8_outbuf);}
      else {
        U8_INIT_STRING_INPUT(&in,data.size,data.bytes); buf = data.bytes;}}
    else {
      U8_INIT_STRING_INPUT(&in,data.size,data.bytes); buf = data.bytes;}
    if (http_response>=300) {
      fd_seterr("HTTP error response","urlxml",CSTRING(url),
                fd_init_string(NULL,in.u8_inlim-in.u8_inbuf,in.u8_inbuf));
      if (conn == curl) reset_curl_handle((fd_curl_handle)conn);
      fd_decref(conn);
      return FD_ERROR;}
    fd_init_xml_node(&xmlnode,NULL,CSTRING(url));
    xmlnode.xml_bits = flags;
    xmlret = fd_walk_xml(&in,fd_default_contentfn,NULL,NULL,NULL,
                       fd_default_popfn,
                       &xmlnode);
    if (conn == curl) reset_curl_handle((fd_curl_handle)conn);
    fd_decref(conn);
    if (xmlret) {
      {FD_DOLIST(elt,xmlret->xml_head) {
        if (SLOTMAPP(elt)) {
          lispval name = fd_get(elt,name_symbol,EMPTY);
          if (SYMBOLP(name)) fd_add(result,name,elt);
          else if ((CHOICEP(name)) || (PRECHOICEP(name))) {
            DO_CHOICES(nm,name) {
              if (SYMBOLP(nm)) fd_add(result,nm,elt);}}}}}
      fd_store(result,pcontent_symbol,xmlret->xml_head);
      u8_free(buf); fd_decref(xmlret->xml_head);
      return result;}
    else {
      fd_decref(result); u8_free(buf);
      return FD_ERROR;}}
  else {
    lispval err;
    cval = fd_make_packet(NULL,data.size,data.bytes);
    fd_store(result,pcontent_symbol,cval); fd_decref(cval);
    err = fd_err(NonTextualContent,"urlxml",CSTRING(url),result);
    fd_decref(result);
    u8_free(data.bytes);
    return err;}
  return cval;
}

/* Req checking */

static lispval responsetest(lispval response,int min,int max)
{
  lispval status = ((TABLEP(response))?
                    (fd_get(response,response_code_slotid,VOID)):
                    (FIXNUMP(response))?(response):(VOID));
  if ((FD_UINTP(status))&&
      ((FIX2INT(status))>=min)&&
      ((FIX2INT(status))<max))
    return FD_TRUE;
  else {
    if (TABLEP(response)) fd_decref(status);
    return FD_FALSE;}
}

static lispval responseokp(lispval response)
{
  return responsetest(response,200,300);
}

static lispval responseredirectp(lispval response)
{
  return responsetest(response,300,400);
}

static lispval responseanyerrorp(lispval response)
{
  return responsetest(response,400,600);
}

static lispval responsemyerrorp(lispval response)
{
  return responsetest(response,400,500);
}

static lispval responseservererrorp(lispval response)
{
  return responsetest(response,500,600);
}

static lispval responseunauthorizedp(lispval response)
{
  return responsetest(response,401,402);
}

static lispval responseforbiddenp(lispval response)
{
  return responsetest(response,401,405);
}

static lispval responsetimeoutp(lispval response)
{
  return responsetest(response,408,409);
}

static lispval responsebadmethodp(lispval response)
{
  return responsetest(response,405,406);
}

static lispval responsenotfoundp(lispval response)
{
  return responsetest(response,404,405);
}

static lispval responsegonep(lispval response)
{
  return responsetest(response,410,411);
}

static lispval responsestatusprim(lispval response)
{
  lispval status = ((TABLEP(response))?
                    (fd_get(response,response_code_slotid,VOID)):
                    (FIXNUMP(response))?(response):(VOID));
  if (!(FIXNUMP(status))) {
    fd_decref(status);
    return fd_type_error("HTTP response","responsestatusprim",response);}
  else return status;
}
static lispval testresponseprim(lispval response,lispval arg1,lispval arg2)
{
  if (FD_AMBIGP(response)) {
    DO_CHOICES(r,response) {
      lispval result = testresponseprim(r,arg1,arg2);
      if (FD_TRUEP(result)) {
        FD_STOP_DO_CHOICES;
        return result;}
      fd_decref(result);}
    return FD_FALSE;}
  else {
    lispval status = ((TABLEP(response))?
                      (fd_get(response,response_code_slotid,VOID)):
                      (FIXNUMP(response))?(response):(VOID));
    if (!(FIXNUMP(status))) {
      if (TABLEP(response)) fd_decref(status);
      return FD_FALSE;}
    if (VOIDP(arg2)) {
      if (fd_choice_containsp(status,arg1))
        return FD_TRUE;
      else return FD_FALSE;}
    else if ((FD_UINTP(arg1))&&(FD_UINTP(arg2))) {
      int min = FIX2INT(arg1), max = FIX2INT(arg2);
      int rval = FIX2INT(status);
      if ((rval>=min)&&(rval<max)) return FD_TRUE;
      else return FD_FALSE;}
    else if (!(FIXNUMP(arg1)))
      return fd_type_error("HTTP status","resptestprim",arg1);
    else return fd_type_error("HTTP status","resptestprim",arg2);}
}

/* Opening URLs with options */

static lispval curlsetopt(lispval handle,lispval opt,lispval value)
{
  if (FALSEP(handle)) {
    if (FD_EQ(opt,header_symbol))
      fd_add(curl_defaults,opt,value);
    else fd_store(curl_defaults,opt,value);
    return FD_TRUE;}
  else if (TYPEP(handle,fd_curl_type)) {
    struct FD_CURL_HANDLE *h = fd_consptr(fd_curl_handle,handle,fd_curl_type);
    return set_curlopt(h,opt,value);}
  else return fd_type_error("curl handle","curlsetopt",handle);
}

static lispval curlopen(int n,lispval *args)
{
  if (n==0)
    return (lispval) fd_open_curl_handle();
  else if (n==1) {
    struct FD_CURL_HANDLE *ch = fd_open_curl_handle();
    lispval spec = args[0], keys = fd_getkeys(spec);
    DO_CHOICES(key,keys) {
      lispval v = fd_get(spec,key,VOID);
      if (!(VOIDP(v))) {
        lispval setval = set_curlopt(ch,key,v);
        if (FD_ABORTP(setval)) {
          lispval conn = (lispval) ch;
          FD_STOP_DO_CHOICES;
          fd_decref(keys);
          fd_decref(conn);
          fd_decref(v);
          return setval;}
        else fd_decref(setval);}
      fd_decref(v);}
    fd_decref(keys);
    return (lispval) ch;}
  else if (n%2)
    return fd_err(fd_SyntaxError,"CURLOPEN",NULL,VOID);
  else {
    int i = 0;
    struct FD_CURL_HANDLE *ch = fd_open_curl_handle();
    while (i<n) {
      lispval setv = set_curlopt(ch,args[i],args[i+1]);
      if (FD_ABORTP(setv)) {
        lispval conn = (lispval) ch;
        fd_decref(conn);
        return setv;}
      i = i+2;}
    return (lispval) ch;}
}

/* Posting */

static lispval urlpost(int n,lispval *args)
{
  INBUF data; CURLcode retval;
  lispval result = VOID, conn, urlarg = VOID, curl=FD_VOID;
  u8_string url; int start;
  struct FD_CURL_HANDLE *h = NULL;
  struct curl_httppost *post = NULL;
  if (n<2) return fd_err(fd_TooFewArgs,"URLPOST",NULL,VOID);
  else if (STRINGP(args[0])) {
    url = CSTRING(args[0]); urlarg = args[0];}
  else if (TYPEP(args[0],fd_secret_type)) {
    url = CSTRING(args[0]); urlarg = args[0];}
  else return fd_type_error("url","urlpost",args[0]);
  if (TYPEP(args[1],fd_curl_type)) curl=args[1];
  if ((TYPEP(args[1],fd_curl_type))||(TABLEP(args[1]))) {
    conn = curl_arg(args[1],"urlpost"); start = 2;}
  else {
    conn = curl_arg(VOID,"urlpost"); start = 1;}
  if (!(TYPEP(conn,fd_curl_type))) {
    return fd_type_error("CURLCONN","urlpost",conn);}
  else h = (fd_curl_handle)conn;
  data.bytes = u8_malloc(8192); data.size = 0; data.limit = 8192;
  result = fd_empty_slotmap();
  curl_easy_setopt(h->handle,CURLOPT_URL,url);
  curl_easy_setopt(h->handle,CURLOPT_WRITEDATA,&data);
  curl_easy_setopt(h->handle,CURLOPT_WRITEHEADER,&result);
  curl_easy_setopt(h->handle,CURLOPT_POST,1);
  if ((n-start)==1) {
    if (STRINGP(args[start])) {
      size_t length = STRLEN(args[start]);
      curl_easy_setopt(h->handle,CURLOPT_POSTFIELDSIZE,length);
      curl_easy_setopt(h->handle,CURLOPT_POSTFIELDS,
                       (char *) (CSTRING(args[start])));}
    else if (PACKETP(args[start])) {
      size_t length = FD_PACKET_LENGTH(args[start]);
      curl_easy_setopt(h->handle,CURLOPT_POSTFIELDSIZE,length);
      curl_easy_setopt(h->handle,CURLOPT_POSTFIELDS,
                       ((char *)(FD_PACKET_DATA(args[start]))));}
    else if (TABLEP(args[start])) {
      /* Construct form data */
      lispval postdata = args[start];
      lispval keys = fd_getkeys(postdata);
      struct U8_OUTPUT nameout; u8_byte _buf[128]; int initnameout = 1;
      struct curl_httppost *last = NULL;
      DO_CHOICES(key,keys) {
        lispval val = fd_get(postdata,key,VOID);
        u8_string keyname = NULL; size_t keylen; int nametype = CURLFORM_PTRNAME;
        if (EMPTYP(val)) continue;
        else if (SYMBOLP(key)) {
          keyname = SYM_NAME(key); keylen = strlen(keyname);}
        else if (STRINGP(key)) {
          keyname = CSTRING(key); keylen = STRLEN(key);}
        else {
          if (initnameout) {
            U8_INIT_STATIC_OUTPUT_BUF(nameout,128,_buf); initnameout = 0;}
          else nameout.u8_write = nameout.u8_outbuf;
          fd_unparse(&nameout,key);
          keyname = nameout.u8_outbuf;
          keylen = (nameout.u8_write-nameout.u8_outbuf)+1;
          nametype = CURLFORM_COPYNAME;}
        if (keyname == NULL) {
          curl_formfree(post); fd_decref(conn);
          if (!(initnameout)) u8_close_output(&nameout);
          return fd_err(fd_TypeError,"CURLPOST",u8_strdup("bad form var"),key);}
        else if (STRINGP(val)) {
          curl_formadd(&post,&last,
                       nametype,keyname,CURLFORM_NAMELENGTH,keylen,
                       CURLFORM_PTRCONTENTS,CSTRING(val),
                       CURLFORM_CONTENTSLENGTH,((size_t)STRLEN(val)),
                       CURLFORM_END);}
        else if (PACKETP(val))
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
      if (conn == curl) reset_curl_handle((fd_curl_handle)conn);
      fd_decref(conn);
      return fd_err(fd_TypeError,"CURLPOST",u8_strdup("postdata"),
                    args[start]);}
    retval = curl_easy_perform(h->handle);
    if (post) curl_formfree(post);}
  else {
    /* Construct form data from args */
    int i = start;
    struct curl_httppost *post = NULL, *last = NULL;
    struct U8_OUTPUT nameout; u8_byte _buf[128]; int initnameout = 1;
    while (i<n) {
      lispval key = args[i], val = args[i+1];
      u8_string keyname = NULL; size_t keylen; int nametype = CURLFORM_PTRNAME;
      if (EMPTYP(val)) {i = i+2; continue;}
      else if (SYMBOLP(key)) {
        keyname = SYM_NAME(key); keylen = strlen(keyname);}
      else if (STRINGP(key)) {
        keyname = CSTRING(key); keylen = STRLEN(key);}
      else {
        if (initnameout) {
          U8_INIT_STATIC_OUTPUT_BUF(nameout,128,_buf);
          initnameout = 0;}
        else nameout.u8_write = nameout.u8_outbuf;
        fd_unparse(&nameout,key);
        keyname = nameout.u8_outbuf;
        keylen = nameout.u8_write-nameout.u8_outbuf;
        nametype = CURLFORM_COPYNAME;}
      i = i+2;
      if (keyname == NULL) {
        if (!(initnameout)) u8_close_output(&nameout);
        curl_formfree(post);
        if (conn == curl) reset_curl_handle((fd_curl_handle)conn);
        fd_decref(conn);
        return fd_err(fd_TypeError,"CURLPOST",u8_strdup("bad form var"),key);}
      else if (STRINGP(val))
        curl_formadd(&post,&last,
                     nametype,keyname,CURLFORM_NAMELENGTH,keylen,
                     CURLFORM_PTRCONTENTS,CSTRING(val),
                     CURLFORM_CONTENTSLENGTH,((size_t)STRLEN(val)),
                     CURLFORM_END);
      else if (PACKETP(val))
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
    retval = curl_easy_perform(h->handle);
    curl_formfree(post);}
  handlefetchresult(h,result,&data);
  if (conn == curl) reset_curl_handle((fd_curl_handle)conn);
  fd_decref(conn);
  if (retval!=CURLE_OK) {
    char buf[CURL_ERROR_SIZE];
    lispval errval = fd_err(CurlError,"fetchurl",getcurlerror(buf,retval),urlarg);
    return errval;}
  else return result;
}

static lispval urlpostdata_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval urlarg = fd_get_arg(expr,1), url = fd_eval(urlarg,env);
  lispval ctype, curl, conn, body = VOID;
  struct U8_OUTPUT out;
  INBUF data; OUTBUF rdbuf; CURLcode retval;
  struct FD_CURL_HANDLE *h = NULL;
  lispval result = VOID;

  if (FD_ABORTP(url)) return url;
  else if (!((STRINGP(url))||(TYPEP(url,fd_secret_type))))
    return fd_type_error("url","urlpostdata_evalfn",url);
  else {
    ctype = fd_eval(fd_get_arg(expr,2),env);
    body = fd_get_body(expr,3);}

  if (FD_ABORTP(ctype)) {
    fd_decref(url); return ctype;}
  else if (!(STRINGP(ctype))) {
    fd_decref(url);
    return fd_type_error("mime type","urlpostdata_evalfn",ctype);}
  else {
    curl = fd_eval(fd_get_arg(expr,3),env);
    body = fd_get_body(expr,4);}

  conn = curl_arg(curl,"urlpostdata");

  if (FD_ABORTP(conn)) {
    fd_decref(url); fd_decref(ctype); fd_decref(curl);
    return conn;}
  else if (!(TYPEP(conn,fd_curl_type))) {
    fd_decref(url); fd_decref(ctype); fd_decref(curl);
    return fd_type_error("CURLCONN","urlpostdata_evalfn",conn);}
  else h = fd_consptr(fd_curl_handle,conn,fd_curl_type);

  curl_add_header(h,"Content-Type",CSTRING(ctype));

  U8_INIT_OUTPUT(&out,8192);

  fd_printout_to(&out,body,env);

  curl_easy_setopt(h->handle,CURLOPT_URL,CSTRING(url));

  rdbuf.scan = out.u8_outbuf; rdbuf.end = out.u8_write;
  curl_easy_setopt(h->handle,CURLOPT_POSTFIELDS,NULL);
  curl_easy_setopt(h->handle,CURLOPT_POSTFIELDSIZE,rdbuf.end-rdbuf.scan);
  curl_easy_setopt(h->handle,CURLOPT_READFUNCTION,copy_upload_data);
  curl_easy_setopt(h->handle,CURLOPT_READDATA,&rdbuf);

  data.bytes = u8_malloc(8192); data.size = 0; data.limit = 8192;
  curl_easy_setopt(h->handle,CURLOPT_WRITEDATA,&data);
  curl_easy_setopt(h->handle,CURLOPT_WRITEHEADER,&result);
  curl_easy_setopt(h->handle,CURLOPT_POST,1);
  result = fd_empty_slotmap();

  retval = curl_easy_perform(h->handle);

  if (retval!=CURLE_OK) {
    char buf[CURL_ERROR_SIZE];
    lispval errval = fd_err(CurlError,"fetchurl",getcurlerror(buf,retval),urlarg);
    fd_decref(url); fd_decref(ctype); fd_decref(curl);
    if (conn == curl) reset_curl_handle((fd_curl_handle)conn);
    fd_decref(conn);
    return errval;}

  handlefetchresult(h,result,&data);
  fd_decref(url); fd_decref(ctype); fd_decref(curl);
  if (conn == curl) reset_curl_handle((fd_curl_handle)conn);
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
      lispval result = fetchurl(NULL,uri);
      lispval content = fd_get(result,pcontent_symbol,EMPTY);
      if (PACKETP(content)) {
        u8_encoding enc=
          ((enc_name == NULL)?(u8_get_default_encoding()):
           (strcasecmp(enc_name,"auto")==0)?
           (u8_guess_encoding(FD_PACKET_DATA(content))):
           (u8_get_encoding(enc_name)));
        if (!(enc)) enc = u8_get_default_encoding();
        u8_string string_form = u8_make_string
          (enc,FD_PACKET_DATA(content),
           FD_PACKET_DATA(content)+FD_PACKET_LENGTH(content));
        fd_decref(content);
        content = lispval_string(string_form);}
      if (STRINGP(content)) {
        lispval eurl = fd_get(result,eurl_slotid,VOID);
        lispval filetime = fd_get(result,filetime_slotid,VOID);
        u8_string sdata = u8_strdup(CSTRING(content));
        if ((STRINGP(eurl))&&(path)) *path = u8_strdup(CSTRING(eurl));
        if ((TYPEP(filetime,fd_timestamp_type))&&(timep))
          *timep = u8_mktime(&(((fd_timestamp)filetime)->u8xtimeval));
        fd_decref(filetime); fd_decref(eurl);
        fd_decref(content); fd_decref(result);
        return sdata;}
      else {
        fd_decref(content); fd_decref(result);
        return NULL;}}
    else {
      lispval result = fetchurlhead(NULL,uri);
      lispval status = fd_get(result,response_code_slotid,VOID);
      if ((VOIDP(status))||
          ((FD_UINTP(status))&&
           (FIX2INT(status)<200)&&
           (FIX2INT(status)>=400)))
        return NULL;
      else {
        lispval eurl = fd_get(result,eurl_slotid,VOID);
        lispval filetime = fd_get(result,filetime_slotid,VOID);
        if ((STRINGP(eurl))&&(path)) *path = u8_strdup(CSTRING(eurl));
        if ((TYPEP(filetime,fd_timestamp_type))&&(timep))
          *timep = u8_mktime(&(((fd_timestamp)filetime)->u8xtimeval));
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
static int n_crypto_locks=0;

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

  ret = (unsigned long)pthread_self();
  return ret;
}

static void init_ssl_locks(void)
{
  int i;
  int n = CRYPTO_num_locks();

  ssl_lockarray=
    (pthread_mutex_t *)OPENSSL_malloc(n * sizeof(pthread_mutex_t));
  for (i = 0; i<n; i++) {
    pthread_mutex_init(&(ssl_lockarray[i]),NULL);}

  CRYPTO_set_id_callback((unsigned long (*)())thread_id);
  CRYPTO_set_locking_callback((void (*)())lock_callback);

  n_crypto_locks=n;
}

static void destroy_ssl_locks(void)
{
  int i;

  CRYPTO_set_locking_callback(NULL);
  for (i = 0; i<n_crypto_locks; i++)
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

static int curl_initialized = 0;

FD_EXPORT void fd_init_curl_c(void) FD_LIBINIT_FN;

FD_EXPORT void fd_init_curl_c()
{
  lispval module;
  if (curl_initialized) return;
  curl_initialized = 1;
  fd_init_scheme();

  if (getenv("USERAGENT"))
    default_user_agent=u8_strdup(getenv("USERAGENT"));
  else default_user_agent=u8_strdup(default_user_agent);

  module = fd_new_module("FDWEB",(0));

  fd_curl_type = fd_register_cons_type("CURLHANDLE");
  fd_recyclers[fd_curl_type]=recycle_curl_handle;
  fd_unparsers[fd_curl_type]=unparse_curl_handle;

  curl_global_init(CURL_GLOBAL_ALL|CURL_GLOBAL_SSL);
  init_ssl_locks();
  atexit(global_curl_cleanup);

  url_symbol = fd_intern("URL");
  pcontent_symbol = fd_intern("%CONTENT");
  content_type_symbol = fd_intern("CONTENT-TYPE");
  content_length_symbol = fd_intern("CONTENT-LENGTH");
  content_encoding_symbol = fd_intern("CONTENT-ENCODING");
  etag_symbol = fd_intern("ETAG");
  charset_symbol = fd_intern("CHARSET");
  header_symbol = fd_intern("HEADER");
  referer_symbol = fd_intern("REFERER");
  method_symbol = fd_intern("METHOD");
  verbose_symbol = fd_intern("VERBOSE");
  useragent_symbol = fd_intern("USERAGENT");
  verifyhost_symbol = fd_intern("VERIFYHOST");
  verifypeer_symbol = fd_intern("VERIFYPEER");
  cainfo_symbol = fd_intern("CAINFO");
  cookie_symbol = fd_intern("COOKIE");
  cookiejar_symbol = fd_intern("COOKIEJAR");
  authinfo_symbol = fd_intern("AUTHINFO");
  basicauth_symbol = fd_intern("BASICAUTH");
  date_symbol = fd_intern("DATE");
  last_modified_symbol = fd_intern("LAST-MODIFIED");
  name_symbol = fd_intern("NAME");
  /* MAXTIME is the maximum time for a result, and TIMEOUT is the max time to
     establish a connection. */
  maxtime_symbol = fd_intern("MAXTIME");
  eurl_slotid = fd_intern("EFFECTIVE-URL");
  filetime_slotid = fd_intern("FILETIME");
  response_code_slotid = fd_intern("RESPONSE");

  timeout_symbol = fd_intern("TIMEOUT");
  connect_timeout_symbol = fd_intern("CONNECT-TIMEOUT");
  accept_timeout_symbol = fd_intern("ACCEPT-TIMEOUT");
  dns_symbol = fd_intern("DNS");
  dnsip_symbol = fd_intern("DNSIP");
  dns_cachelife_symbol = fd_intern("DNSCACHELIFE");
  fresh_connect_symbol = fd_intern("FRESHCONNECT");
  forbid_reuse_symbol = fd_intern("NOREUSE");
  filetime_symbol = fd_intern("FILETIME");

  CHOICE_ADD(text_types,FDSYM_TEXT);
  decl_text_type("application/xml");
  decl_text_type("application/rss+xml");
  decl_text_type("application/atom+xml");
  decl_text_type("application/json");

  curl_defaults = fd_empty_slotmap();

  fd_def_evalfn(module,"URLPOSTOUT","",urlpostdata_evalfn);

  fd_idefn(module,fd_make_cprim2("URLGET",urlget,1));
  fd_idefn4(module,"URLSTREAM",urlstream,1,
            "(URLSTREAM *url* *handler* [*curl*]) opens the remote URL *url* "
            "and calls *handler* on packets of data from the stream. A second "
            "argument to *handler* is a slotmap which will be returned when "
            "the *handler* either errs or returns #F",
            fd_string_type,VOID,-1,VOID,
            -1,VOID,-1,VOID);
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
           ("CURL/RESET!",curlreset,1,fd_curl_type,VOID));
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
    ("CURL:LOGLEVEL",_("Loglevel for debugging CURL calls"),
     fd_intconfig_get,fd_loglevelconfig_set,&curl_loglevel);

  fd_register_config
    ("CURL:USERAGENT",_("What CURL should use as the default user agent string"),
     fd_sconfig_get,fd_sconfig_set,&default_user_agent);


  fd_register_sourcefn(url_source_fn,NULL);

  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
