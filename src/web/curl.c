/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_LOGLEVEL (u8_getloglevel(curl_loglevel))

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/tables.h"
#include "kno/eval.h"
#include "kno/webtools.h"
#include "kno/ports.h"

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

static u8_string default_user_agent="Kno/CURL";

static lispval curl_defaults, url_symbol;
static lispval content_type_symbol, charset_symbol, pcontent_symbol;
static lispval content_length_symbol, etag_symbol, content_encoding_symbol;
static lispval verbose_symbol, header_symbol, bearer_symbol;
static lispval referer_symbol, useragent_symbol, cookie_symbol;
static lispval date_symbol, last_modified_symbol, name_symbol;
static lispval cookiejar_symbol, authinfo_symbol, basicauth_symbol;
static lispval maxtime_symbol, timeout_symbol, method_symbol;
static lispval verifyhost_symbol, verifypeer_symbol, cainfo_symbol;
static lispval eurl_slotid, filetime_slotid, response_code_slotid;
static lispval timeout_symbol, connect_timeout_symbol, accept_timeout_symbol;
static lispval dns_symbol, dnsip_symbol, dns_cachelife_symbol;
static lispval fresh_connect_symbol, forbid_reuse_symbol, filetime_symbol;
static lispval follow_symbol;

static lispval text_types = EMPTY;

static u8_condition NonTextualContent=
  _("can't parse non-textual content as XML");
static u8_condition CurlError=_("Internal libcurl error");

static int max_redirects = -1;

typedef struct KNO_CURL_HANDLE {
  KNO_CONS_HEADER;
  CURL *handle;
  struct curl_slist *headers;
  /* char curl_errbuf[CURL_ERROR_SIZE]; */
  lispval initdata;} KNO_CURL_HANDLE;
typedef struct KNO_CURL_HANDLE *kno_curl_handle;

KNO_EXPORT struct KNO_CURL_HANDLE *kno_open_curl_handle(void);

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
  lispval packet=kno_make_packet(NULL,data_len,data);
  lispval argvec[2]={packet,result};
  lispval apply_result=kno_apply(handler,2,argvec);
  kno_decref(packet);
  if (KNO_ABORTP(apply_result)) {
    u8_exception ex=u8_erreify();
    state[2]=kno_wrap_exception(ex);
    return 0;}
  else if (FALSEP(apply_result))
    return 0;
  else if (FIXNUMP(result))
    return FIX2INT(result);
  else return n_elts*elt_size;
}

kno_ptr_type kno_curl_type;

void handle_content_type(char *value,lispval table)
{
  char *chset, *chset_end; int endbyte;
  char *end = value, *slash = strchr(value,'/');
  lispval major_type, full_type;
  while ((*end) && (!((*end==';') || (isspace(*end))))) end++;
  if (slash) *slash='\0';
  endbyte = *end; *end='\0';
  major_type = kno_parse(value);
  kno_store(table,KNOSYM_TYPE,major_type);
  if (slash) *slash='/';
  full_type = lispval_string(value);
  kno_add(table,KNOSYM_TYPE,full_type); *end = endbyte;
  kno_decref(major_type); kno_decref(full_type);
  if ((chset = (strstr(value,"charset=")))) {
    lispval chset_val;
    chset_end = chset = chset+8; if (*chset=='"') {
      chset++; chset_end = strchr(chset,'"');}
    else chset_end = strchr(chset,';');
    if (chset_end) *chset_end='\0';
    chset_val = lispval_string(chset);
    kno_store(table,charset_symbol,chset_val);
    kno_decref(chset_val);}
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
      *valptr = val = kno_empty_slotmap();
    slotid = kno_parse(copy);
    if (KNO_EQ(slotid,content_type_symbol)) {
      handle_content_type(valstart,val);
      hval = lispval_string(valstart);
      add = 0;}
    else if ((KNO_EQ(slotid,content_length_symbol)) ||
             (KNO_EQ(slotid,response_code_slotid))) {
      hval = kno_parse(valstart);
      add = 0;}
    else if ((KNO_EQ(slotid,date_symbol)) ||
             (KNO_EQ(slotid,last_modified_symbol))) {
      time_t now, moment = curl_getdate(valstart,&now);
      struct U8_XTIME xt;
      u8_init_xtime(&xt,moment,u8_second,0,0,0);
      hval = kno_make_timestamp(&xt);
      add = 0;}
    else hval = lispval_string(valstart);
    if (add) kno_add(val,slotid,hval);
    else kno_store(val,slotid,hval);
    kno_decref(hval);
    u8_free(copy);}
  else {
    hval = lispval_string(copy);
    kno_add(val,header_symbol,hval);
    kno_decref(hval);
    u8_free(copy);}
  return byte_len;
}

KNO_INLINE_FCN lispval addtexttype(lispval type)
{
  kno_incref(type);
  CHOICE_ADD(text_types,type);
  return VOID;
}

static void decl_text_type(u8_string string)
{
  lispval stringval = lispval_string(string);
  CHOICE_ADD(text_types,stringval);
}

KNO_INLINE_FCN struct KNO_CURL_HANDLE *curl_err(u8_string cxt,int code)
{
  u8_string details=u8_fromlibc((char *)curl_easy_strerror(code));
  kno_seterr(CurlError,cxt,details,VOID);
  u8_free(details);
  return NULL;
}

static int _curl_set(u8_string cxt,struct KNO_CURL_HANDLE *h,
                     CURLoption option,void *v)
{
  CURLcode retval = curl_easy_setopt(h->handle,option,v);
  if (retval) {
    u8_string details=u8_fromlibc((char *)curl_easy_strerror(retval));
    u8_free(h);
    kno_seterr(CurlError,cxt,details,VOID);
    u8_free(details);}
  return retval;
}

static int _curl_set2dtype(u8_string cxt,struct KNO_CURL_HANDLE *h,
                           CURLoption option,
                           lispval f,lispval slotid)
{
  lispval v = kno_get(f,slotid,VOID);
  if (KNO_ABORTP(v))
    return kno_interr(v);
  else if ((STRINGP(v))||
           (PACKETP(v))||
           (TYPEP(v,kno_secret_type))) {
    CURLcode retval = curl_easy_setopt(h->handle,option,CSTRING(v));
    if (retval) {
      u8_string details=u8_fromlibc((char *)curl_easy_strerror(retval));
      kno_seterr(CurlError,cxt,details,VOID);
      u8_free(h); kno_decref(v);}
    return retval;}
  else if (FIXNUMP(v)) {
    CURLcode retval = curl_easy_setopt(h->handle,option,(long)(kno_getint(v)));
    if (retval) {
      u8_string details=u8_fromlibc((char *)curl_easy_strerror(retval));
      kno_seterr(CurlError,cxt,details,KNO_VOID);
      u8_free(h); kno_decref(v);}
    return retval;}
  else {
    kno_seterr(kno_TypeError,cxt,"string",v);
    return -1;}
}

static int curl_add_header(kno_curl_handle ch,u8_string arg1,u8_string arg2)
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

static int curl_add_headers(kno_curl_handle ch,lispval val)
{
  int retval = 0;
  DO_CHOICES(v,val)
    if (STRINGP(v))
      retval = curl_add_header(ch,CSTRING(v),NULL);
    else if (PACKETP(v))
      retval = curl_add_header(ch,KNO_PACKET_DATA(v),NULL);
    else if (PAIRP(v)) {
      lispval car = KNO_CAR(v), cdr = KNO_CDR(v); u8_string hdr = NULL;
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
      lispval keys = kno_getkeys(v);
      DO_CHOICES(key,keys) {
        if ((retval>=0)&&((STRINGP(key))||(SYMBOLP(key)))) {
          lispval kval = kno_get(v,key,EMPTY); u8_string hdr = NULL;
          if (SYMBOLP(key)) {
            if (STRINGP(kval))
              hdr = u8_mkstring("%s: %s",SYM_NAME(key),CSTRING(kval));
            else hdr = u8_mkstring("%s: %q",SYM_NAME(key),kval);}
          else if (STRINGP(kval))
            hdr = u8_mkstring("%s: %s",CSTRING(key),CSTRING(kval));
          else hdr = u8_mkstring("%s: %q",CSTRING(key),kval);
          retval = curl_add_header(ch,hdr,NULL);
          u8_free(hdr);
          kno_decref(kval);}}
      kno_decref(keys);}
    else {}
  return retval;
}

static int curl_set_bearer(kno_curl_handle ch,lispval v)
{
  if ( (KNO_STRINGP(v)) || (KNO_PACKETP(v)) || (KNO_SECRETP(v)) ) {
    struct curl_slist *cur = ch->headers, *newh;
    u8_string hdr = u8_mkstring("Authorization: Bearer %s",KNO_CSTRING(v));
    newh = curl_slist_append(cur,hdr);
    ch->headers = newh;
    /* Should check for errors */
    int rv = curl_easy_setopt(ch->handle,CURLOPT_HTTPHEADER,(void *)newh);
    if (rv != CURLE_OK) {
      kno_seterr("CurlBearerFailed","curl_set_bearer",NULL,v);}
    u8_free(hdr);
    if (rv != CURLE_OK)
      return -1;
    else return 1;}
  else if ( (KNO_VOIDP(v)) || (KNO_FALSEP(v)) || (KNO_EMPTYP(v)) )
    return 0;
  else {
    u8_log(LOGWARN,"CurlBearerFailed","Invalid bearer value of %q",v);
    return -1;}
}


KNO_EXPORT
struct KNO_CURL_HANDLE *kno_open_curl_handle()
{
  int rv = 0;
  struct KNO_CURL_HANDLE *h = u8_alloc(struct KNO_CURL_HANDLE);

#define curl_set(hl,o,v) \
  if ( (rv >= 0) && (_curl_set("kno_open_curl_handle",hl,o,(void *)v)) ) rv=-1;
#define curl_set2dtype(hl,o,f,s) \
  if ( (rv>=0) && (_curl_set2dtype("kno_open_curl_handle",hl,o,f,s)) ) rv = -1;

  KNO_INIT_CONS(h,kno_curl_type);
  h->handle = curl_easy_init();
  h->headers = NULL;
  h->initdata = EMPTY;
  if (h->handle == NULL) {
    u8_free(h);
    kno_seterr(CurlError,"kno_open_curl_handle","curl_easy_init failed",VOID);
    rv=-1;}
  if (rv >= 0)
    u8_logf(LOG_INFO,"CURL","Creating CURL handle %llx",(KNO_INTPTR) h->handle);

  if ( (rv >= 0) && (curl_loglevel > LOG_NOTIFY) ) {
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

  if (kno_test(curl_defaults,verifypeer_symbol,VOID))
    curl_set(h,CURLOPT_SSL_VERIFYPEER,0);
  if (kno_test(curl_defaults,verifyhost_symbol,VOID))
    curl_set(h,CURLOPT_SSL_VERIFYHOST,0);
  curl_set(h,CURLOPT_SSLVERSION,CURL_SSLVERSION_DEFAULT);

  if (kno_test(curl_defaults,useragent_symbol,VOID)) {
    curl_set2dtype(h,CURLOPT_USERAGENT,curl_defaults,useragent_symbol);}
  else curl_set(h,CURLOPT_USERAGENT,default_user_agent);
  if (kno_test(curl_defaults,basicauth_symbol,VOID)) {
    curl_set(h,CURLOPT_HTTPAUTH,CURLAUTH_BASIC);
    curl_set2dtype(h,CURLOPT_USERPWD,curl_defaults,basicauth_symbol);}
  if (kno_test(curl_defaults,bearer_symbol,VOID)) {
    lispval v = kno_get(curl_defaults,bearer_symbol,VOID);
    curl_set_bearer(h,v);
    kno_decref(v);}
  if (kno_test(curl_defaults,authinfo_symbol,VOID)) {
    curl_set(h,CURLOPT_HTTPAUTH,CURLAUTH_ANY);
    curl_set2dtype(h,CURLOPT_USERPWD,curl_defaults,authinfo_symbol);}
  if (kno_test(curl_defaults,referer_symbol,VOID))
    curl_set2dtype(h,CURLOPT_REFERER,curl_defaults,referer_symbol);
  if (kno_test(curl_defaults,cookie_symbol,VOID))
    curl_set2dtype(h,CURLOPT_REFERER,curl_defaults,cookie_symbol);
  if (kno_test(curl_defaults,cookiejar_symbol,VOID))
    curl_set2dtype(h,CURLOPT_REFERER,curl_defaults,cookiejar_symbol);
  if (kno_test(curl_defaults,maxtime_symbol,VOID))
    curl_set2dtype(h,CURLOPT_TIMEOUT,curl_defaults,maxtime_symbol);
  if (kno_test(curl_defaults,timeout_symbol,VOID))
    curl_set2dtype(h,CURLOPT_CONNECTTIMEOUT,curl_defaults,timeout_symbol);

  if (rv >= 0) {
   lispval http_headers = kno_get(curl_defaults,header_symbol,EMPTY);
    curl_add_headers(h,http_headers);
    kno_decref(http_headers);}

  if (rv < 0) {
    curl_slist_free_all(h->headers);
    curl_easy_cleanup(h->handle);
    kno_decref(h->initdata);
    u8_free(h);
    return NULL;}
  else return h;
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

static void reset_curl_handle(struct KNO_CURL_HANDLE *ch)
{
  if (ch->headers) {
    curl_slist_free_all(ch->headers);
    ch->headers=NULL;
    curl_easy_setopt(ch->handle,CURLOPT_HTTPHEADER,(void *)NULL);}
  if (KNO_CONSP(ch->initdata)) {
    kno_decref(ch->initdata);
    ch->initdata=EMPTY;}
}

static void recycle_curl_handle(struct KNO_RAW_CONS *c)
{
  struct KNO_CURL_HANDLE *ch = (struct KNO_CURL_HANDLE *)c;
  u8_logf(LOG_INFO,"CURL","Freeing CURL handle %llx",(KNO_INTPTR) ch->handle);
  /* curl_easy_setopt(ch->handle,CURLOPT_ERRORBUFFER,NULL); */
  curl_slist_free_all(ch->headers);
  curl_easy_cleanup(ch->handle);
  kno_decref(ch->initdata);
  if (KNO_MALLOCD_CONSP(c)) u8_free(c);
}

static int unparse_curl_handle(u8_output out,lispval x)
{
  u8_printf(out,"#<CURL %lx>",x);
  return 1;
}

static lispval curlhandlep(lispval arg)
{
  if (TYPEP(arg,kno_curl_type)) return KNO_TRUE;
  else return KNO_FALSE;
}

static lispval curlreset(lispval arg)
{
  struct KNO_CURL_HANDLE *ch = (struct KNO_CURL_HANDLE *)arg;

  curl_easy_reset(ch->handle);

  curl_easy_setopt(ch->handle,CURLOPT_NOPROGRESS,1);
  curl_easy_setopt(ch->handle,CURLOPT_FILETIME,(long)1);
  curl_easy_setopt(ch->handle,CURLOPT_NOSIGNAL,1);
  curl_easy_setopt(ch->handle,CURLOPT_WRITEFUNCTION,copy_content_data);
  curl_easy_setopt(ch->handle,CURLOPT_HEADERFUNCTION,handle_header);

  return VOID;
}

static lispval set_curlopt
  (struct KNO_CURL_HANDLE *ch,lispval opt,lispval val)
{
  if (KNO_EQ(opt,referer_symbol))
    if (STRINGP(val))
      curl_easy_setopt(ch->handle,CURLOPT_REFERER,CSTRING(val));
    else return kno_type_error("string","set_curlopt",val);
  else if (KNO_EQ(opt,method_symbol))
    if (SYMBOLP(val))
      curl_easy_setopt(ch->handle,CURLOPT_CUSTOMREQUEST,SYM_NAME(val));
    else if (STRINGP(val))
      curl_easy_setopt(ch->handle,CURLOPT_CUSTOMREQUEST,CSTRING(val));
    else return kno_type_error("symbol/method","set_curlopt",val);
  else if (KNO_EQ(opt,verbose_symbol)) {
    if ((FALSEP(val))||(EMPTYP(val)))
      curl_easy_setopt(ch->handle,CURLOPT_VERBOSE,0);
    else curl_easy_setopt(ch->handle,CURLOPT_VERBOSE,1);}
  else if (KNO_EQ(opt,useragent_symbol))
    if (STRINGP(val))
      curl_easy_setopt(ch->handle,CURLOPT_USERAGENT,CSTRING(val));
    else if (FALSEP(val)) {}
    else return kno_type_error("string","set_curlopt",val);
  else if (KNO_EQ(opt,authinfo_symbol))
    if (STRINGP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_HTTPAUTH,CURLAUTH_ANY);
      curl_easy_setopt(ch->handle,CURLOPT_USERPWD,CSTRING(val));}
    else return kno_type_error("string","set_curlopt",val);
  else if (KNO_EQ(opt,bearer_symbol)) {
    int rv = curl_set_bearer(ch,val);
    if (rv < 0) return kno_type_error("string","set_curlopt",val);}
  else if (KNO_EQ(opt,basicauth_symbol))
    if ((STRINGP(val))||(TYPEP(val,kno_secret_type))) {
      curl_easy_setopt(ch->handle,CURLOPT_HTTPAUTH,CURLAUTH_BASIC);
      curl_easy_setopt(ch->handle,CURLOPT_USERPWD,CSTRING(val));}
    else return kno_type_error("string","set_curlopt",val);
  else if (KNO_EQ(opt,cookie_symbol))
    if ((STRINGP(val))||(TYPEP(val,kno_secret_type)))
      curl_easy_setopt(ch->handle,CURLOPT_COOKIE,CSTRING(val));
    else return kno_type_error("string","set_curlopt",val);
  else if (KNO_EQ(opt,cookiejar_symbol))
    if (STRINGP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_COOKIEFILE,CSTRING(val));
      curl_easy_setopt(ch->handle,CURLOPT_COOKIEJAR,CSTRING(val));}
    else return kno_type_error("string","set_curlopt",val);
  else if (KNO_EQ(opt,header_symbol))
    curl_add_headers(ch,val);
  else if (KNO_EQ(opt,cainfo_symbol))
    if (STRINGP(val))
      curl_easy_setopt(ch->handle,CURLOPT_CAINFO,CSTRING(val));
    else return kno_type_error("string","set_curlopt",val);
  else if (KNO_EQ(opt,verifyhost_symbol))
    if (KNO_UINTP(val))
      curl_easy_setopt(ch->handle,CURLOPT_SSL_VERIFYHOST,FIX2INT(val));
    else if (FALSEP(val))
      curl_easy_setopt(ch->handle,CURLOPT_SSL_VERIFYHOST,0);
    else if (KNO_TRUEP(val))
      curl_easy_setopt(ch->handle,CURLOPT_SSL_VERIFYHOST,2);
    else return kno_type_error("symbol/method","set_curlopt",val);
  else if ((KNO_EQ(opt,timeout_symbol))||(KNO_EQ(opt,maxtime_symbol))) {
    if (FIXNUMP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_TIMEOUT,FIX2INT(val));}
    else if (KNO_FLONUMP(val)) {
      double secs = KNO_FLONUM(val);
      long int msecs = (long int)floor(secs*1000);
      curl_easy_setopt(ch->handle,CURLOPT_TIMEOUT_MS,msecs);}
    else return kno_type_error("seconds","set_curlopt/timeout",val);}
  else if (KNO_EQ(opt,connect_timeout_symbol)) {
    if (FIXNUMP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_CONNECTTIMEOUT,FIX2INT(val));}
    else if (KNO_FLONUMP(val)) {
      double secs = KNO_FLONUM(val);
      long int msecs = (long int)floor(secs*1000);
      curl_easy_setopt(ch->handle,CURLOPT_CONNECTTIMEOUT_MS,msecs);}
    else return kno_type_error("seconds","set_curlopt/connecttimeout",val);}
  else if (KNO_EQ(opt,connect_timeout_symbol)) {
    if (FIXNUMP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_CONNECTTIMEOUT,FIX2INT(val));}
    else if (KNO_FLONUMP(val)) {
      double secs = KNO_FLONUM(val);
      long int msecs = (long int)floor(secs*1000);
      curl_easy_setopt(ch->handle,CURLOPT_CONNECTTIMEOUT_MS,msecs);}
    else return kno_type_error("seconds","set_curlopt/connecttimeout",val);}
  else if (KNO_EQ(opt,accept_timeout_symbol)) {
    if (FIXNUMP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_ACCEPTTIMEOUT_MS,
                       (1000*FIX2INT(val)));}
    else if (KNO_FLONUMP(val)) {
      double secs = KNO_FLONUM(val);
      long int msecs = (long int)floor(secs*1000);
      curl_easy_setopt(ch->handle,CURLOPT_ACCEPTTIMEOUT_MS,msecs);}
    else return kno_type_error("seconds","set_curlopt/connecttimeout",val);}
  else if (KNO_EQ(opt,dns_symbol)) {
    if ((STRINGP(val))&&(STRLEN(val)>0)) {
      kno_incref(val); CHOICE_ADD(ch->initdata,val);
      curl_easy_setopt(ch->handle,CURLOPT_DNS_SERVERS,
                       CSTRING(val));}
    else if ((FALSEP(val))||(STRINGP(val))) {
      curl_easy_setopt(ch->handle,CURLOPT_DNS_SERVERS,NULL);}
    else if (SYMBOLP(val)) {
      lispval cval = kno_config_get(SYM_NAME(val));
      if (!(STRINGP(cval)))
        return kno_type_error("string config","set_curlopt/dns",val);
      kno_incref(cval); CHOICE_ADD(ch->initdata,cval);
      curl_easy_setopt(ch->handle,CURLOPT_DNS_SERVERS,
                       CSTRING(cval));}
    else return kno_type_error("string","set_curlopt/dns",val);}
  else if (KNO_EQ(opt,dnsip_symbol)) {
    if ((STRINGP(val))&&(STRLEN(val)>0)) {
      kno_incref(val); CHOICE_ADD(ch->initdata,val);
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
      lispval cval = kno_config_get(SYM_NAME(val));
      if (!(STRINGP(cval)))
        return kno_type_error("string config","set_curlopt/dns",val);
      kno_incref(cval); CHOICE_ADD(ch->initdata,cval);
      if (STRLEN(cval)==0) {
        curl_easy_setopt(ch->handle,CURLOPT_DNS_LOCAL_IP4,NULL);
        curl_easy_setopt(ch->handle,CURLOPT_DNS_LOCAL_IP6,NULL);}
      else if (strchr(CSTRING(cval),':')) {
        curl_easy_setopt(ch->handle,CURLOPT_DNS_LOCAL_IP6,
                         CSTRING(cval));}
      else {
        curl_easy_setopt(ch->handle,CURLOPT_DNS_LOCAL_IP4,
                         CSTRING(cval));}}
    else return kno_type_error("string","set_curlopt/dns",val);}
  else if (KNO_EQ(opt,dns_cachelife_symbol)) {
    if (KNO_TRUEP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_DNS_CACHE_TIMEOUT,-1);}
    else if (FALSEP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_DNS_CACHE_TIMEOUT,0);}
    else if (FIXNUMP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_DNS_CACHE_TIMEOUT,
                       FIX2INT(val));}
    else if (KNO_FLONUMP(val)) {
      double secs = KNO_FLONUM(val);
      long int msecs = (long int)floor(secs);
      curl_easy_setopt(ch->handle,CURLOPT_DNS_CACHE_TIMEOUT,msecs);}
    else return kno_type_error("seconds","set_curlopt/connecttimeout",val);}
  else if (KNO_EQ(opt,follow_symbol)) {
    if (KNO_TRUEP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_FOLLOWLOCATION,1);
      if (max_redirects > 0)
        curl_easy_setopt(ch->handle,CURLOPT_MAXREDIRS,max_redirects);}
    else if ( (KNO_FIXNUMP(val)) && ((KNO_FIX2INT(val))>0) ) {
      long long count = KNO_FIX2INT(val);
      curl_easy_setopt(ch->handle,CURLOPT_FOLLOWLOCATION,1);
      curl_easy_setopt(ch->handle,CURLOPT_MAXREDIRS,count);}
    else curl_easy_setopt(ch->handle,CURLOPT_FOLLOWLOCATION,0);}
  else if (KNO_EQ(opt,fresh_connect_symbol)) {
    if (KNO_TRUEP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_FRESH_CONNECT,1);}
    else curl_easy_setopt(ch->handle,CURLOPT_FRESH_CONNECT,0);}
  else if (KNO_EQ(opt,forbid_reuse_symbol)) {
    if (KNO_TRUEP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_FORBID_REUSE,1);}
    else curl_easy_setopt(ch->handle,CURLOPT_FORBID_REUSE,0);}
  else if (KNO_EQ(opt,filetime_symbol)) {
    if (KNO_TRUEP(val)) {
      curl_easy_setopt(ch->handle,CURLOPT_FILETIME,1);}
    else curl_easy_setopt(ch->handle,CURLOPT_FILETIME,0);}
  else if (KNO_EQ(opt,verifypeer_symbol))
    if (FIXNUMP(val))
      curl_easy_setopt(ch->handle,CURLOPT_SSL_VERIFYPEER,FIX2INT(val));
    else if (FALSEP(val))
      curl_easy_setopt(ch->handle,CURLOPT_SSL_VERIFYPEER,0);
    else if (KNO_TRUEP(val))
      curl_easy_setopt(ch->handle,CURLOPT_SSL_VERIFYPEER,1);
    else return kno_type_error("fixnum/boolean","set_curlopt",val);
  else if (KNO_EQ(opt,content_type_symbol))
    if (STRINGP(val)) {
      u8_string ctype_header = u8_mkstring("Content-type: %s",CSTRING(val));
      lispval hval = kno_lispstring(ctype_header);
      curl_add_headers(ch,hval);
      kno_decref(hval);}
    else return kno_type_error(_("string"),"set_curl_handle/content-type",val);
  else return kno_err(_("Unknown CURL option"),"set_curl_handle",
                     NULL,opt);
  if (CONSP(val)) {
    kno_incref(val); CHOICE_ADD(ch->initdata,val);}
  return KNO_TRUE;
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
  return kno_stream2string(&out);
}

/* The core get function */

static lispval handlefetchresult(struct KNO_CURL_HANDLE *h,lispval result,INBUF *data);

static lispval fetchurl(struct KNO_CURL_HANDLE *h,u8_string urltext)
{
  INBUF data; CURLcode retval;
  int consed_handle = 0;
  lispval result = kno_empty_slotmap();
  lispval url = fixurl(urltext);
  kno_add(result,url_symbol,url); kno_decref(url);
  data.bytes = u8_malloc(8192); data.size = 0; data.limit = 8192;
  if (h == NULL) {h = kno_open_curl_handle(); consed_handle = 1;}
  curl_easy_setopt(h->handle,CURLOPT_URL,CSTRING(url));
  curl_easy_setopt(h->handle,CURLOPT_WRITEDATA,&data);
  curl_easy_setopt(h->handle,CURLOPT_WRITEHEADER,&result);
  curl_easy_setopt(h->handle,CURLOPT_NOBODY,0);
  retval = curl_easy_perform(h->handle);
  if (retval!=CURLE_OK) {
    char buf[CURL_ERROR_SIZE];
    lispval errval=
      kno_err(CurlError,"fetchurl",getcurlerror(buf,retval),url);
    kno_decref(result);
    u8_free(data.bytes);
    if (consed_handle) {kno_decref((lispval)h);}
    return errval;}
  handlefetchresult(h,result,&data);
  if (consed_handle) {kno_decref((lispval)h);}
  return result;
}

static lispval streamurl(struct KNO_CURL_HANDLE *h,
                        u8_string urltext,
                        lispval handler,
                        const unsigned char *payload_type,
                        const unsigned char *payload,
                        size_t payload_size)
{
  OUTBUF rdbuf;
  CURLcode retval;
  int consed_handle = 0;
  lispval result = kno_empty_slotmap();
  lispval url = fixurl(urltext);
  lispval stream_data[3]={handler,result,VOID};
  kno_add(result,url_symbol,url); kno_decref(url);
  if (h == NULL) {h = kno_open_curl_handle(); consed_handle = 1;}
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
    if (TYPEP(stream_data[2],kno_exception_type)) {
      kno_exception exo=(kno_exception)stream_data[2];
      kno_restore_exception(exo);
      if (consed_handle) {kno_decref((lispval)h);}
      return KNO_ERROR;}
    else {
      if (consed_handle) {kno_decref((lispval)h);}
      return result;}}
  else if (retval!=CURLE_OK) {
    char buf[CURL_ERROR_SIZE];
    lispval errval=
      kno_err(CurlError,"fetchurl",getcurlerror(buf,retval),result);
    kno_decref(result);
    if (consed_handle) {kno_decref((lispval)h);}
    return errval;}
  if (consed_handle) {kno_decref((lispval)h);}
  return result;
}

static lispval fetchurlhead(struct KNO_CURL_HANDLE *h,u8_string urltext)
{
  INBUF data; CURLcode retval;
  int consed_handle = 0;
  lispval result = kno_empty_slotmap();
  lispval url = fixurl(urltext);
  kno_add(result,url_symbol,url); kno_decref(url);
  data.bytes = u8_malloc(8192); data.size = 0; data.limit = 8192;
  if (h == NULL) {h = kno_open_curl_handle(); consed_handle = 1;}
  curl_easy_setopt(h->handle,CURLOPT_URL,CSTRING(url));
  curl_easy_setopt(h->handle,CURLOPT_WRITEDATA,&data);
  curl_easy_setopt(h->handle,CURLOPT_WRITEHEADER,&result);
  curl_easy_setopt(h->handle,CURLOPT_NOBODY,1);
  retval = curl_easy_perform(h->handle);
  if (retval!=CURLE_OK) {
    char buf[CURL_ERROR_SIZE];
    lispval errval = kno_err(CurlError,"fetchurl",getcurlerror(buf,retval),url);
    kno_decref(result);
    u8_free(data.bytes);
    if (consed_handle) {kno_decref((lispval)h);}
    return errval;}
  handlefetchresult(h,result,&data);
  if (consed_handle) {kno_decref((lispval)h);}
  return result;
}

static lispval handlefetchresult(struct KNO_CURL_HANDLE *h,lispval result,
                                INBUF *data)
{
  lispval cval; long http_response = 0;
  int retval = curl_easy_getinfo(h->handle,CURLINFO_RESPONSE_CODE,&http_response);
  if (retval==0)
    kno_add(result,response_code_slotid,KNO_INT(http_response));
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
  else if ((kno_test(result,KNOSYM_TYPE,text_types))&&
           (!(kno_test(result,content_encoding_symbol,VOID))))
    if (data->size==0)
      cval = kno_block_string(data->size,data->bytes);
    else {
      lispval chset = kno_get(result,charset_symbol,VOID);
      if (STRINGP(chset)) {
        U8_OUTPUT out;
        u8_encoding enc = u8_get_encoding(CSTRING(chset));
        if (enc) {
          const unsigned char *scan = data->bytes;
          U8_INIT_OUTPUT(&out,data->size);
          u8_convert(enc,1,&out,&scan,data->bytes+data->size);
          cval = kno_block_string(out.u8_write-out.u8_outbuf,out.u8_outbuf);}
        else if (strstr(data->bytes,"\r\n"))
          cval = kno_lispstring(u8_convert_crlfs(data->bytes));
        else cval = kno_lispstring(u8_valid_copy(data->bytes));}
      else if (strstr(data->bytes,"\r\n"))
        cval = kno_lispstring(u8_convert_crlfs(data->bytes));
      else cval = kno_lispstring(u8_valid_copy(data->bytes));
      u8_free(data->bytes);
      kno_decref(chset);}
  else {
    cval = kno_make_packet(NULL,data->size,data->bytes);
    u8_free(data->bytes);}
  kno_store(result,pcontent_symbol,cval);
  {
    char *urlbuf; long filetime;
    CURLcode rv = curl_easy_getinfo(h->handle,CURLINFO_EFFECTIVE_URL,&urlbuf);
    if (rv == CURLE_OK) {
      lispval eurl = lispval_string(urlbuf);
      kno_add(result,eurl_slotid,eurl);
      kno_decref(eurl);}
    rv = curl_easy_getinfo(h->handle,CURLINFO_FILETIME,&filetime);
    if ((rv == CURLE_OK) && (filetime>=0)) {
      lispval ftime = kno_time2timestamp(filetime);
      kno_add(result,filetime_slotid,ftime);
      kno_decref(ftime);}}
  kno_decref(cval);
  return result;
}

/* Handling arguments to URL primitives */

static lispval curl_arg(lispval arg,u8_context cxt)
{
  if (TYPEP(arg,kno_curl_type)) {
    kno_incref(arg);
    return arg;}
  else if (TABLEP(arg)) {
    lispval keys = kno_getkeys(arg);
    struct KNO_CURL_HANDLE *h = kno_open_curl_handle();
    DO_CHOICES(key,keys) {
      lispval values = kno_get(arg,key,VOID);
      lispval setval = set_curlopt(h,key,values);
      if (KNO_ABORTP(setval)) {
        kno_decref(keys);
        recycle_curl_handle((kno_raw_cons)h);
        return setval;}
      kno_decref(values);}
    kno_decref(keys);
    return (lispval)h;}
  else if ((VOIDP(arg))||(FALSEP(arg)))
    return (lispval) kno_open_curl_handle();
  else return kno_type_error("curl handle",cxt,arg);
}

/* Primitives */

static lispval urlget(lispval url,lispval curl)
{
  lispval result, conn = curl_arg(curl,"urlget");
  if (KNO_ABORTP(conn)) return conn;
  else if (!(TYPEP(conn,kno_curl_type)))
    return kno_type_error("CURLCONN","urlget",conn);
  else if (!((STRINGP(url))||(TYPEP(url,kno_secret_type)))) {
    result = kno_type_error("string","urlget",url);}
  else {
    result = fetchurl((kno_curl_handle)conn,CSTRING(url));}
  if (conn == curl) reset_curl_handle((kno_curl_handle)conn);
  kno_decref(conn);
  return result;
}

static lispval urlstream(lispval url,lispval handler,
                         lispval payload,
                         lispval curl)
{
  if (!(KNO_APPLICABLEP(handler)))
    return kno_type_error("applicable","urlstream",handler);
  lispval result, conn = curl_arg(curl,"urlstream");
  if (KNO_ABORTP(conn)) return conn;
  else if (!(TYPEP(conn,kno_curl_type)))
    return kno_type_error("CURLCONN","urlget",conn);
  else if (!((STRINGP(url))||(TYPEP(url,kno_secret_type)))) {
    result = kno_type_error("string","urlget",url);}
  else if (VOIDP(payload))
    result = streamurl((kno_curl_handle)conn,CSTRING(url),
                       handler,NULL,NULL,0);
  else {
    result = streamurl((kno_curl_handle)conn,CSTRING(url),
                       handler,CSTRING(payload),
                       "application/x-www-urlform-encoded",
                       STRLEN(payload));}
  if (conn == curl) reset_curl_handle((kno_curl_handle)conn);
  kno_decref(conn);
  return result;
}

static lispval urlhead(lispval url,lispval curl)
{
  lispval result, conn = curl_arg(curl,"urlhead");
  if (KNO_ABORTP(conn)) return conn;
  else if (!(TYPEP(conn,kno_curl_type)))
    return kno_type_error("CURLCONN","urlhead",conn);
  else if (!((STRINGP(url))||(TYPEP(url,kno_secret_type))))
    result = kno_type_error("string","urlhead",url);
  result = fetchurlhead((kno_curl_handle)conn,CSTRING(url));
  if (conn == curl) reset_curl_handle((kno_curl_handle)conn);
  kno_decref(conn);
  return result;
}

static lispval urlput(lispval url,lispval content,lispval ctype,lispval curl)
{
  lispval conn;
  INBUF data; OUTBUF rdbuf; CURLcode retval;
  lispval result = VOID;
  struct KNO_CURL_HANDLE *h = NULL;
  if (!((STRINGP(content))||(PACKETP(content))||(FALSEP(content))))
    return kno_type_error("string or packet","urlput",content);
  else conn = curl_arg(curl,"urlput");
  if (KNO_ABORTP(conn)) return conn;
  else if (!(TYPEP(conn,kno_curl_type))) {
    return kno_type_error("CURLCONN","urlput",conn);}
  else h = (kno_curl_handle)conn;
  if (STRINGP(ctype))
    curl_add_header(h,"Content-Type",CSTRING(ctype));
  else if ((TABLEP(curl)) && (kno_test(curl,content_type_symbol,VOID))) {}
  else if (STRINGP(content))
    curl_add_header(h,"Content-Type: text",NULL);
  else curl_add_header(h,"Content-Type: application",NULL);
  data.bytes = u8_malloc(8192); data.size = 0; data.limit = 8192;
  result = kno_empty_slotmap();
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
    size_t length = KNO_PACKET_LENGTH(content);
    rdbuf.scan = (u8_byte *)KNO_PACKET_DATA(content);
    rdbuf.end = (u8_byte *)KNO_PACKET_DATA(content)+length;
    curl_easy_setopt(h->handle,CURLOPT_INFILESIZE,length);}
  else {}
  retval = curl_easy_perform(h->handle);
  if (retval!=CURLE_OK) {
    char buf[CURL_ERROR_SIZE];
    lispval errval = kno_err(CurlError,"urlput",getcurlerror(buf,retval),url);
    kno_decref(result);
    u8_free(data.bytes);
    kno_decref(conn);
    return errval;}
  else handlefetchresult(h,result,&data);
  if (conn == curl) reset_curl_handle((kno_curl_handle)conn);
  kno_decref(conn);
  return result;
}

/* Getting content */

static lispval urlcontent(lispval url,lispval curl)
{
  lispval result, conn = curl_arg(curl,"urlcontent"), content;
  if (KNO_ABORTP(conn)) return conn;
  else if (!(TYPEP(conn,kno_curl_type)))
    result = kno_type_error("CURLCONN","urlcontent",conn);
  else result = fetchurl((kno_curl_handle)conn,CSTRING(url));
  if (conn == curl) reset_curl_handle((kno_curl_handle)conn);
  kno_decref(conn);
  if (KNO_ABORTP(result)) {
    return result;}
  else {
    content = kno_get(result,pcontent_symbol,EMPTY);
    kno_decref(result);
    return content;}
}

static lispval urlxml(lispval url,lispval xmlopt,lispval curl)
{
  INBUF data;
  lispval result, cval, conn = curl_arg(curl,"urlxml");
  int flags; long http_response = 0;
  KNO_CURL_HANDLE *h; CURLcode retval;
  if (KNO_ABORTP(conn)) return conn;
  else if (!(TYPEP(conn,kno_curl_type)))
    return kno_type_error("CURLCONN","urlxml",conn);
  else {
    h = (kno_curl_handle)conn;
    result = kno_empty_slotmap();}
  flags = kno_xmlparseoptions(xmlopt);
  /* Check that the XML options are okay */
  if (flags<0) {
    kno_decref(result);
    kno_decref(conn);
    return KNO_ERROR;}
  kno_add(result,url_symbol,url);
  data.bytes = u8_malloc(8192); data.size = 0; data.limit = 8192;
  curl_easy_setopt(h->handle,CURLOPT_URL,CSTRING(url));
  curl_easy_setopt(h->handle,CURLOPT_WRITEDATA,&data);
  curl_easy_setopt(h->handle,CURLOPT_WRITEHEADER,&result);
  retval = curl_easy_perform(h->handle);
  if (retval!=CURLE_OK) {
    char buf[CURL_ERROR_SIZE];
    lispval errval = kno_err(CurlError,"urlxml",getcurlerror(buf,retval),url);
    kno_decref(result);
    u8_free(data.bytes);
    if (conn == curl) reset_curl_handle((kno_curl_handle)conn);
    kno_decref(conn);
    return errval;}
  retval = curl_easy_getinfo(h->handle,CURLINFO_RESPONSE_CODE,&http_response);
  if (retval!=CURLE_OK) {
    char buf[CURL_ERROR_SIZE];
    lispval errval = kno_err(CurlError,"urlxml",getcurlerror(buf,retval),url);
    kno_decref(result);
    u8_free(data.bytes);
    if (conn == curl) reset_curl_handle((kno_curl_handle)conn);
    kno_decref(conn);
    return errval;}
  if (data.size<data.limit) data.bytes[data.size]='\0';
  else {
    data.bytes = u8_realloc(data.bytes,data.size+4);
    data.bytes[data.size]='\0';}
  if (data.size<=0) {
    if (data.size==0)
      cval = kno_init_packet(NULL,data.size,data.bytes);
    else cval = EMPTY;}
  else if ((kno_test(result,KNOSYM_TYPE,text_types))&&
           (!(kno_test(result,content_encoding_symbol,VOID)))) {
    U8_INPUT in; u8_string buf;
    struct KNO_XML xmlnode, *xmlret;
    lispval chset = kno_get(result,charset_symbol,VOID);
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
      kno_seterr("HTTP error response","urlxml",CSTRING(url),
                kno_init_string(NULL,in.u8_inlim-in.u8_inbuf,in.u8_inbuf));
      if (conn == curl) reset_curl_handle((kno_curl_handle)conn);
      kno_decref(conn);
      return KNO_ERROR;}
    kno_init_xml_node(&xmlnode,NULL,CSTRING(url));
    xmlnode.xml_bits = flags;
    xmlret = kno_walk_xml(&in,kno_default_contentfn,NULL,NULL,NULL,
                       kno_default_popfn,
                       &xmlnode);
    if (conn == curl) reset_curl_handle((kno_curl_handle)conn);
    kno_decref(conn);
    if (xmlret) {
      {KNO_DOLIST(elt,xmlret->xml_head) {
        if (SLOTMAPP(elt)) {
          lispval name = kno_get(elt,name_symbol,EMPTY);
          if (SYMBOLP(name)) kno_add(result,name,elt);
          else if ((CHOICEP(name)) || (PRECHOICEP(name))) {
            DO_CHOICES(nm,name) {
              if (SYMBOLP(nm)) kno_add(result,nm,elt);}}}}}
      kno_store(result,pcontent_symbol,xmlret->xml_head);
      u8_free(buf); kno_decref(xmlret->xml_head);
      return result;}
    else {
      kno_decref(result); u8_free(buf);
      return KNO_ERROR;}}
  else {
    lispval err;
    cval = kno_make_packet(NULL,data.size,data.bytes);
    kno_store(result,pcontent_symbol,cval); kno_decref(cval);
    err = kno_err(NonTextualContent,"urlxml",CSTRING(url),result);
    kno_decref(result);
    u8_free(data.bytes);
    return err;}
  return cval;
}

/* Req checking */

static lispval responsetest(lispval response,int min,int max)
{
  lispval status = ((TABLEP(response))?
                    (kno_get(response,response_code_slotid,VOID)):
                    (FIXNUMP(response))?(response):(VOID));
  if ((KNO_UINTP(status))&&
      ((FIX2INT(status))>=min)&&
      ((FIX2INT(status))<max))
    return KNO_TRUE;
  else {
    if (TABLEP(response)) kno_decref(status);
    return KNO_FALSE;}
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
                    (kno_get(response,response_code_slotid,VOID)):
                    (FIXNUMP(response))?(response):(VOID));
  if (!(FIXNUMP(status))) {
    kno_decref(status);
    return kno_type_error("HTTP response","responsestatusprim",response);}
  else return status;
}
static lispval testresponseprim(lispval response,lispval arg1,lispval arg2)
{
  if (KNO_AMBIGP(response)) {
    DO_CHOICES(r,response) {
      lispval result = testresponseprim(r,arg1,arg2);
      if (KNO_TRUEP(result)) {
        KNO_STOP_DO_CHOICES;
        return result;}
      kno_decref(result);}
    return KNO_FALSE;}
  else {
    lispval status = ((TABLEP(response))?
                      (kno_get(response,response_code_slotid,VOID)):
                      (FIXNUMP(response))?(response):(VOID));
    if (!(FIXNUMP(status))) {
      if (TABLEP(response)) kno_decref(status);
      return KNO_FALSE;}
    if (VOIDP(arg2)) {
      if (kno_choice_containsp(status,arg1))
        return KNO_TRUE;
      else return KNO_FALSE;}
    else if ((KNO_UINTP(arg1))&&(KNO_UINTP(arg2))) {
      int min = FIX2INT(arg1), max = FIX2INT(arg2);
      int rval = FIX2INT(status);
      if ((rval>=min)&&(rval<max)) return KNO_TRUE;
      else return KNO_FALSE;}
    else if (!(FIXNUMP(arg1)))
      return kno_type_error("HTTP status","resptestprim",arg1);
    else return kno_type_error("HTTP status","resptestprim",arg2);}
}

/* Opening URLs with options */

static lispval curlsetopt(lispval handle,lispval opt,lispval value)
{
  if (FALSEP(handle)) {
    if (KNO_EQ(opt,header_symbol))
      kno_add(curl_defaults,opt,value);
    else kno_store(curl_defaults,opt,value);
    return KNO_TRUE;}
  else if (TYPEP(handle,kno_curl_type)) {
    struct KNO_CURL_HANDLE *h = kno_consptr(kno_curl_handle,handle,kno_curl_type);
    return set_curlopt(h,opt,value);}
  else return kno_type_error("curl handle","curlsetopt",handle);
}

static lispval curlopen(int n,lispval *args)
{
  if (n==0)
    return (lispval) kno_open_curl_handle();
  else if (n==1) {
    struct KNO_CURL_HANDLE *ch = kno_open_curl_handle();
    lispval spec = args[0], keys = kno_getkeys(spec);
    DO_CHOICES(key,keys) {
      lispval v = kno_get(spec,key,VOID);
      if (!(VOIDP(v))) {
        lispval setval = set_curlopt(ch,key,v);
        if (KNO_ABORTP(setval)) {
          lispval conn = (lispval) ch;
          KNO_STOP_DO_CHOICES;
          kno_decref(keys);
          kno_decref(conn);
          kno_decref(v);
          return setval;}
        else kno_decref(setval);}
      kno_decref(v);}
    kno_decref(keys);
    return (lispval) ch;}
  else if (n%2)
    return kno_err(kno_SyntaxError,"CURLOPEN",NULL,VOID);
  else {
    int i = 0;
    struct KNO_CURL_HANDLE *ch = kno_open_curl_handle();
    while (i<n) {
      lispval setv = set_curlopt(ch,args[i],args[i+1]);
      if (KNO_ABORTP(setv)) {
        lispval conn = (lispval) ch;
        kno_decref(conn);
        return setv;}
      i = i+2;}
    return (lispval) ch;}
}

/* Posting */

static lispval urlpost(int n,lispval *args)
{
  INBUF data; CURLcode retval;
  lispval result = VOID, conn, urlarg = VOID, curl=KNO_VOID;
  u8_string url; int start;
  struct KNO_CURL_HANDLE *h = NULL;
  struct curl_httppost *post = NULL;
  if (n<2) return kno_err(kno_TooFewArgs,"URLPOST",NULL,VOID);
  else if (STRINGP(args[0])) {
    url = CSTRING(args[0]); urlarg = args[0];}
  else if (TYPEP(args[0],kno_secret_type)) {
    url = CSTRING(args[0]); urlarg = args[0];}
  else return kno_type_error("url","urlpost",args[0]);
  if (TYPEP(args[1],kno_curl_type)) curl=args[1];
  if ((TYPEP(args[1],kno_curl_type))||(TABLEP(args[1]))) {
    conn = curl_arg(args[1],"urlpost"); start = 2;}
  else {
    conn = curl_arg(VOID,"urlpost"); start = 1;}
  if (!(TYPEP(conn,kno_curl_type))) {
    return kno_type_error("CURLCONN","urlpost",conn);}
  else h = (kno_curl_handle)conn;
  data.bytes = u8_malloc(8192); data.size = 0; data.limit = 8192;
  result = kno_empty_slotmap();
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
      size_t length = KNO_PACKET_LENGTH(args[start]);
      curl_easy_setopt(h->handle,CURLOPT_POSTFIELDSIZE,length);
      curl_easy_setopt(h->handle,CURLOPT_POSTFIELDS,
                       ((char *)(KNO_PACKET_DATA(args[start]))));}
    else if (TABLEP(args[start])) {
      /* Construct form data */
      lispval postdata = args[start];
      lispval keys = kno_getkeys(postdata);
      struct U8_OUTPUT nameout; u8_byte _buf[128]; int initnameout = 1;
      struct curl_httppost *last = NULL;
      DO_CHOICES(key,keys) {
        lispval val = kno_get(postdata,key,VOID);
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
          kno_unparse(&nameout,key);
          keyname = nameout.u8_outbuf;
          keylen = (nameout.u8_write-nameout.u8_outbuf)+1;
          nametype = CURLFORM_COPYNAME;}
        if (keyname == NULL) {
          curl_formfree(post); kno_decref(conn);
          if (!(initnameout)) u8_close_output(&nameout);
          return kno_err(kno_TypeError,"CURLPOST",u8_strdup("bad form var"),key);}
        else if (STRINGP(val)) {
          curl_formadd(&post,&last,
                       nametype,keyname,CURLFORM_NAMELENGTH,keylen,
                       CURLFORM_PTRCONTENTS,CSTRING(val),
                       CURLFORM_CONTENTSLENGTH,((size_t)STRLEN(val)),
                       CURLFORM_END);}
        else if (PACKETP(val))
          curl_formadd(&post,&last,
                       nametype,keyname,CURLFORM_NAMELENGTH,keylen,
                       CURLFORM_PTRCONTENTS,KNO_PACKET_DATA(val),
                       CURLFORM_CONTENTSLENGTH,((size_t)KNO_PACKET_LENGTH(val)),
                       CURLFORM_END);
        else {
          U8_OUTPUT out; U8_INIT_OUTPUT(&out,128);
          kno_unparse(&out,val);
          curl_formadd(&post,&last,
                       nametype,keyname,CURLFORM_NAMELENGTH,keylen,
                       CURLFORM_COPYCONTENTS,out.u8_outbuf,
                       CURLFORM_CONTENTSLENGTH,
                       ((size_t)(out.u8_write-out.u8_outbuf)),
                       CURLFORM_END);
          u8_free(out.u8_outbuf);}
        kno_decref(val);}
      if (!(initnameout)) u8_close_output(&nameout);
      curl_easy_setopt(h->handle, CURLOPT_HTTPPOST, post);
      kno_decref(keys);}
    else {
      if (conn == curl) reset_curl_handle((kno_curl_handle)conn);
      kno_decref(conn);
      return kno_err(kno_TypeError,"CURLPOST",u8_strdup("postdata"),
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
        kno_unparse(&nameout,key);
        keyname = nameout.u8_outbuf;
        keylen = nameout.u8_write-nameout.u8_outbuf;
        nametype = CURLFORM_COPYNAME;}
      i = i+2;
      if (keyname == NULL) {
        if (!(initnameout)) u8_close_output(&nameout);
        curl_formfree(post);
        if (conn == curl) reset_curl_handle((kno_curl_handle)conn);
        kno_decref(conn);
        return kno_err(kno_TypeError,"CURLPOST",u8_strdup("bad form var"),key);}
      else if (STRINGP(val))
        curl_formadd(&post,&last,
                     nametype,keyname,CURLFORM_NAMELENGTH,keylen,
                     CURLFORM_PTRCONTENTS,CSTRING(val),
                     CURLFORM_CONTENTSLENGTH,((size_t)STRLEN(val)),
                     CURLFORM_END);
      else if (PACKETP(val))
        curl_formadd(&post,&last,
                     nametype,keyname,CURLFORM_NAMELENGTH,keylen,
                     CURLFORM_PTRCONTENTS,KNO_PACKET_DATA(val),
                     CURLFORM_CONTENTSLENGTH,((size_t)KNO_PACKET_LENGTH(val)),
                     CURLFORM_END);
      else {
        U8_OUTPUT out; U8_INIT_OUTPUT(&out,128);
        kno_unparse(&out,val);
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
  if (conn == curl) reset_curl_handle((kno_curl_handle)conn);
  kno_decref(conn);
  if (retval!=CURLE_OK) {
    char buf[CURL_ERROR_SIZE];
    lispval errval = kno_err(CurlError,"fetchurl",getcurlerror(buf,retval),urlarg);
    return errval;}
  else return result;
}

static lispval urlpostdata_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval urlarg = kno_get_arg(expr,1), url = kno_eval(urlarg,env);
  lispval ctype, curl, conn, body = VOID;
  struct U8_OUTPUT out;
  INBUF data; OUTBUF rdbuf; CURLcode retval;
  struct KNO_CURL_HANDLE *h = NULL;
  lispval result = VOID;

  if (KNO_ABORTP(url)) return url;
  else if (!((STRINGP(url))||(TYPEP(url,kno_secret_type))))
    return kno_type_error("url","urlpostdata_evalfn",url);
  else {
    ctype = kno_eval(kno_get_arg(expr,2),env);
    body = kno_get_body(expr,3);}

  if (KNO_ABORTP(ctype)) {
    kno_decref(url); return ctype;}
  else if (!(STRINGP(ctype))) {
    kno_decref(url);
    return kno_type_error("mime type","urlpostdata_evalfn",ctype);}
  else {
    curl = kno_eval(kno_get_arg(expr,3),env);
    body = kno_get_body(expr,4);}

  conn = curl_arg(curl,"urlpostdata");

  if (KNO_ABORTP(conn)) {
    kno_decref(url); kno_decref(ctype); kno_decref(curl);
    return conn;}
  else if (!(TYPEP(conn,kno_curl_type))) {
    kno_decref(url); kno_decref(ctype); kno_decref(curl);
    return kno_type_error("CURLCONN","urlpostdata_evalfn",conn);}
  else h = kno_consptr(kno_curl_handle,conn,kno_curl_type);

  curl_add_header(h,"Content-Type",CSTRING(ctype));

  U8_INIT_OUTPUT(&out,8192);

  kno_printout_to(&out,body,env);

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
  result = kno_empty_slotmap();

  retval = curl_easy_perform(h->handle);

  if (retval!=CURLE_OK) {
    char buf[CURL_ERROR_SIZE];
    lispval errval = kno_err(CurlError,"fetchurl",getcurlerror(buf,retval),urlarg);
    kno_decref(url); kno_decref(ctype); kno_decref(curl);
    if (conn == curl) reset_curl_handle((kno_curl_handle)conn);
    kno_decref(conn);
    return errval;}

  handlefetchresult(h,result,&data);
  kno_decref(url); kno_decref(ctype); kno_decref(curl);
  if (conn == curl) reset_curl_handle((kno_curl_handle)conn);
  kno_decref(conn);
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
      lispval content = kno_get(result,pcontent_symbol,EMPTY);
      if (PACKETP(content)) {
        u8_encoding enc=
          ((enc_name == NULL)?(u8_get_default_encoding()):
           (strcasecmp(enc_name,"auto")==0)?
           (u8_guess_encoding(KNO_PACKET_DATA(content))):
           (u8_get_encoding(enc_name)));
        if (!(enc)) enc = u8_get_default_encoding();
        u8_string string_form = u8_make_string
          (enc,KNO_PACKET_DATA(content),
           KNO_PACKET_DATA(content)+KNO_PACKET_LENGTH(content));
        kno_decref(content);
        content = lispval_string(string_form);}
      if (STRINGP(content)) {
        lispval eurl = kno_get(result,eurl_slotid,VOID);
        lispval filetime = kno_get(result,filetime_slotid,VOID);
        u8_string sdata = u8_strdup(CSTRING(content));
        if ((STRINGP(eurl))&&(path)) *path = u8_strdup(CSTRING(eurl));
        if ((TYPEP(filetime,kno_timestamp_type))&&(timep))
          *timep = u8_mktime(&(((kno_timestamp)filetime)->u8xtimeval));
        kno_decref(filetime); kno_decref(eurl);
        kno_decref(content); kno_decref(result);
        return sdata;}
      else {
        kno_decref(content); kno_decref(result);
        return NULL;}}
    else {
      lispval result = fetchurlhead(NULL,uri);
      lispval status = kno_get(result,response_code_slotid,VOID);
      if ((VOIDP(status))||
          ((KNO_UINTP(status))&&
           (FIX2INT(status)<200)&&
           (FIX2INT(status)>=400)))
        return NULL;
      else {
        lispval eurl = kno_get(result,eurl_slotid,VOID);
        lispval filetime = kno_get(result,filetime_slotid,VOID);
        if ((STRINGP(eurl))&&(path)) *path = u8_strdup(CSTRING(eurl));
        if ((TYPEP(filetime,kno_timestamp_type))&&(timep))
          *timep = u8_mktime(&(((kno_timestamp)filetime)->u8xtimeval));
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

KNO_EXPORT void kno_init_curl_c(void) KNO_LIBINIT_FN;

KNO_EXPORT void kno_init_curl_c()
{
  if (curl_initialized) return;
  curl_initialized = 1;
  kno_init_scheme();

  if (getenv("USERAGENT"))
    default_user_agent=u8_strdup(getenv("USERAGENT"));
  else default_user_agent=u8_strdup(default_user_agent);

  lispval module = kno_new_module("WEBTOOLS",(0));

  kno_curl_type = kno_register_cons_type("CURLHANDLE");
  kno_recyclers[kno_curl_type]=recycle_curl_handle;
  kno_unparsers[kno_curl_type]=unparse_curl_handle;

  curl_global_init(CURL_GLOBAL_ALL|CURL_GLOBAL_SSL);
  init_ssl_locks();
  atexit(global_curl_cleanup);

  url_symbol = kno_intern("url");
  pcontent_symbol = kno_intern("%content");
  content_type_symbol = kno_intern("content-type");
  content_length_symbol = kno_intern("content-length");
  content_encoding_symbol = kno_intern("content-encoding");
  etag_symbol = kno_intern("etag");
  charset_symbol = kno_intern("charset");
  header_symbol = kno_intern("header");
  referer_symbol = kno_intern("referer");
  method_symbol = kno_intern("method");
  verbose_symbol = kno_intern("verbose");
  useragent_symbol = kno_intern("useragent");
  verifyhost_symbol = kno_intern("verifyhost");
  verifypeer_symbol = kno_intern("verifypeer");
  cainfo_symbol = kno_intern("cainfo");
  cookie_symbol = kno_intern("cookie");
  cookiejar_symbol = kno_intern("cookiejar");
  authinfo_symbol = kno_intern("authinfo");
  basicauth_symbol = kno_intern("basicauth");
  bearer_symbol = kno_intern("bearer");
  date_symbol = kno_intern("date");
  last_modified_symbol = kno_intern("last-modified");
  name_symbol = kno_intern("name");
  /* MAXTIME is the maximum time for a result, and TIMEOUT is the max time to
     establish a connection. */
  maxtime_symbol = kno_intern("maxtime");
  eurl_slotid = kno_intern("effective-url");
  filetime_slotid = kno_intern("filetime");
  response_code_slotid = kno_intern("response");

  timeout_symbol = kno_intern("timeout");
  connect_timeout_symbol = kno_intern("connect-timeout");
  accept_timeout_symbol = kno_intern("accept-timeout");
  dns_symbol = kno_intern("dns");
  dnsip_symbol = kno_intern("dnsip");
  dns_cachelife_symbol = kno_intern("dnscachelife");
  fresh_connect_symbol = kno_intern("freshconnect");
  forbid_reuse_symbol = kno_intern("noreuse");
  filetime_symbol = kno_intern("filetime");
  follow_symbol = kno_intern("follow");

  CHOICE_ADD(text_types,KNOSYM_TEXT);
  decl_text_type("application/xml");
  decl_text_type("application/rss+xml");
  decl_text_type("application/atom+xml");
  decl_text_type("application/json");

  curl_defaults = kno_empty_slotmap();

  kno_def_evalfn(module,"URLPOSTOUT","",urlpostdata_evalfn);

  kno_idefn(module,kno_make_cprim2("URLGET",urlget,1));
  kno_idefn4(module,"URLSTREAM",urlstream,1,
            "(URLSTREAM *url* *handler* [*curl*]) opens the remote URL *url* "
            "and calls *handler* on packets of data from the stream. A second "
            "argument to *handler* is a slotmap which will be returned when "
            "the *handler* either errs or returns #F",
            kno_string_type,VOID,-1,VOID,
            -1,VOID,-1,VOID);
  kno_idefn(module,kno_make_cprim2("URLHEAD",urlhead,1));
  kno_idefn(module,kno_make_cprimn("URLPOST",urlpost,1));
  kno_idefn(module,kno_make_cprim4("URLPUT",urlput,2));
  kno_idefn(module,kno_make_cprim2("URLCONTENT",urlcontent,1));
  kno_idefn(module,kno_make_cprim3("URLXML",urlxml,1));
  kno_idefn(module,kno_make_cprimn("CURL/OPEN",curlopen,0));
  kno_defalias(module,"CURLOPEN","CURL/OPEN");
  kno_idefn(module,kno_make_cprim3("CURL/SETOPT!",curlsetopt,2));
  kno_defalias(module,"CURLSETOPT!","CURL/SETOPT!");
  kno_idefn(module,kno_make_cprim1x
           ("CURL/RESET!",curlreset,1,kno_curl_type,VOID));
  kno_defalias(module,"CURLRESET!","CURL/RESET!");
  kno_idefn(module,kno_make_cprim1("ADD-TEXT_TYPE!",addtexttype,1));
  kno_idefn(module,kno_make_cprim1("CURL-HANDLE?",curlhandlep,1));

  kno_idefn(module,kno_make_cprim1("RESPONSE/OK?",responseokp,1));
  kno_idefn(module,kno_make_cprim1("RESPONSE/REDIRECT?",
                                 responseredirectp,1));
  kno_idefn(module,kno_make_cprim1("RESPONSE/ERROR?",responseanyerrorp,1));
  kno_idefn(module,kno_make_cprim1("RESPONSE/MYERROR?",responsemyerrorp,1));
  kno_idefn(module,kno_make_cprim1("RESPONSE/SERVERERROR?",
                                 responseservererrorp,1));
  kno_idefn(module,kno_make_cprim1("RESPONSE/UNAUTHORIZED?",
                                 responseunauthorizedp,1));
  kno_idefn(module,kno_make_cprim1("RESPONSE/FORBIDDEN?",
                                 responseforbiddenp,1));
  kno_idefn(module,kno_make_cprim1("RESPONSE/TIMEOUT?",
                                 responsetimeoutp,1));
  kno_idefn(module,kno_make_cprim1("RESPONSE/BADMETHOD?",
                                 responsebadmethodp,1));
  kno_idefn(module,kno_make_cprim1("RESPONSE/NOTFOUND?",
                                 responsenotfoundp,1));
  kno_idefn(module,kno_make_cprim1("RESPONSE/NOTFOUND?",
                                 responsenotfoundp,1));
  kno_idefn(module,kno_make_cprim1("RESPONSE/GONE?",
                                 responsegonep,1));
  kno_idefn(module,kno_make_cprim1("RESPONSE/STATUS",responsestatusprim,1));
  kno_idefn(module,kno_make_ndprim
           (kno_make_cprim3("RESPONSE/STATUS?",testresponseprim,2)));

  kno_register_config
    ("CURL:LOGLEVEL",_("Loglevel for debugging CURL calls"),
     kno_intconfig_get,kno_loglevelconfig_set,&curl_loglevel);

  kno_register_config
    ("CURL:USERAGENT",_("What CURL should use as the default user agent string"),
     kno_sconfig_get,kno_sconfig_set,&default_user_agent);

  kno_register_config
    ("CURL:REDIRECTS",_("Maximum number of redirects to allow"),
     kno_intconfig_get,kno_intconfig_set,&max_redirects);

  kno_register_sourcefn(url_source_fn,NULL);

  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
