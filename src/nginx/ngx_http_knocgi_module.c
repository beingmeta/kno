/* C Mode */

/* ngx_http_knocgi_module.c
   Copyright (C) 2002-2020 beingmeta, inc.  All Rights Reserved
   This is a nginx module supporting persistent FramerD servers

*/

#ifndef DEBUG_ALL
#define DEBUG_ALL 0
#endif

#ifndef DEBUG_KNOCGI
#define DEBUG_KNOCGI DEBUG_ALL
#endif

#ifndef DEBUG_CGIDATA
#define DEBUG_CGIDATA DEBUG_ALL
#endif

#ifndef DEBUG_CONNECT
#define DEBUG_CONNECT DEBUG_ALL
#endif

#ifndef DEBUG_TRANSPORT
#define DEBUG_TRANSPORT DEBUG_ALL
#endif

#ifndef DEBUG_SOCKETS
#define DEBUG_SOCKETS DEBUG_ALL
#endif

#define LOGNOTICE NGX_LOG_NOTICE
#define LOGINFO NGX_LOG_INFO

#if DEBUG_KNOCGI
#define LOGDEBUG NGX_LOG_INFO
#else
#define LOGDEBUG NGX_LOG_DEBUG
#endif

#if DEBUG_SOCKETS
#define LOGSOCKET NGX_LOG_WARN
#else
#define LOGSOCKET NGX_LOG_NOTICE
#endif

#if DEBUG_CONFIG
#define LOG_CONFIG(p,arg) log_config(p,arg)
#else
#define LOG_CONFIG(p,arg)
#endif

/* #include "ngx_http_knocgi_module_fileinfo.h" */

#if (SIZEOF_VOIDP==8)
typedef unsigned long long INTPOINTER;
#else
typedef unsigned int INTPOINTER;
#endif

#ifndef DEFAULT_SERVLET_WAIT
#define DEFAULT_SERVLET_WAIT 60
#endif

#ifndef MAX_CONFIGS
#define MAX_CONFIGS 128
#endif

#ifndef DEFAULT_LOG_SYNC
#define DEFAULT_LOG_SYNC 0
#endif

#include <stdio.h>
#include <sys/un.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <pthread.h>

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

#ifndef TRACK_EXECUTION_TIMES
#if HAVE_FTIME
#define TRACK_EXECUTION_TIMES 1
#else
#define TRACK_EXECUTION_TIMES 1
#endif
#endif

#define KNOCGI_INIT_SERVLETS 32

#if TRACK_EXECUTION_TIMES
#include "sys/timeb.h"
#endif

#define KNOCGI_MAGIC_TYPE "application/x-httpd-knocgi"

#ifndef DEFAULT_ISASYNC
#define DEFAULT_ISASYNC 1
#endif

#ifndef _FILEINFO
#define _FILEINFO __FILE
#endif

#ifndef USEDTBLOCK
#define USEDTBLOCK 0
#endif

typedef struct KNOCGI_CONFIG {
  unsigned char *spec;
  ngx_http_upstream_conf_t   upstream;
  ngx_msec_t servlet_wait;
  ngx_array_t params;} ngx_http_knocgi_config;
typedef struct KNOCGI_CONTEXT {
  ngx_http_request_t        *request;
  time_t started;} ngx_http_knocgi_ctx_t;


ngx_module_t ngx_http_knocgi_module;

/* Writing DTypes to BUFFs */

#define ARBITRARY_BUFLIM (8*65536)

/* In Apache 2.0, BUFFs seem to be replaced by buckets in brigades,
   but that seems a little overhead heavy for the output buffers used
   here, which are just used to write slotmaps to servlets.  So a
   simple reimplementation is done here.  */
typedef struct BUFF {
  ngx_pool_t *p; unsigned char *buf, *ptr, *lim;} BUFF;
static int buff_needs(BUFF *b,size_t n)
{
  if ((b->ptr+n)>=b->lim) {
    size_t old_size=b->lim-b->buf, off=b->ptr-b->buf;
    size_t need_off=off+n, new_size=old_size;
    unsigned char *nbuf;
    while (need_off>new_size) {
      if (new_size>=ARBITRARY_BUFLIM) {
	new_size=ARBITRARY_BUFLIM*(2+(need_off/ARBITRARY_BUFLIM));
	break;}
      else new_size=new_size*2;}
    nbuf=ngx_pcalloc(b->p,new_size);
    if (!(nbuf)) return -1;
    memcpy(nbuf,b->buf,off);
    b->buf=nbuf; b->ptr=nbuf+off; b->lim=nbuf+new_size;
    return new_size;}
  else return b->lim-b->buf;
}

static int buff_putc(BUFF *b,unsigned char c)
{
  if (buff_needs(b,1)<0) return -1;
  *(b->ptr++)=c;
  return 1;
}
static int buff_puts(BUFF *b,unsigned char *string)
{
  unsigned int len=strlen((char *)string);
  if (buff_needs(b,len+1)<0) return -1;
  memcpy((char *)b->ptr,string,len); b->ptr=b->ptr+len;
  return len;
}
static int buff_write(BUFF *b,unsigned char *string,int len)
{
  if (buff_needs(b,len+1)<0) return -1;
  memcpy((char *)b->ptr,string,len); b->ptr=b->ptr+len;
  return len;
}
static BUFF *buff_create(ngx_pool_t *p,int n)
{
  struct BUFF *b=ngx_pcalloc(p,sizeof(struct BUFF));
  if (!(b)) return b;
  b->p=p; b->ptr=b->buf=ngx_pcalloc(p,n); b->lim=b->buf+n;
  return b;
}

static int buff_write_4bytes(BUFF *b,unsigned int i)
{
  if (buff_needs(b,4)<0) return -1;
  buff_putc(b,(i>>24));	     buff_putc(b,((i>>16)&0xFF));
  buff_putc(b,((i>>8)&0xFF)); buff_putc(b,(i&0xFF));
  return 4;
}

static int buff_write_string(BUFF *b,unsigned char *string,unsigned int len)
{
  if (buff_needs(b,len+5)<0) return -1;
  buff_putc(b,0x06); buff_write_4bytes(b,len);
  buff_write(b,string,len);
  return len+5;
}
static int buff_write_symbol(BUFF *b,unsigned char *string,unsigned int len)
{
  if (buff_needs(b,len+5)<0) return -1;
  buff_putc(b,0x07); buff_write_4bytes(b,len);
  buff_write(b,string,len);
  return len+5;
}

static int buff_write_slotval(BUFF *b,
			     unsigned char *slotname,unsigned int name_len,
			     unsigned char *slotval,unsigned int value_len)
{
  int n_bytes=0, delta=0;
  if ((delta=buff_write_symbol(b,slotname,name_len))<0) return -1;
  n_bytes=n_bytes+delta;
  if ((delta=buff_write_string(b,slotval,value_len))<0) return -1;
  return n_bytes+delta;
}

static int ngx_list_length(ngx_list_t *l);

static int write_cgidata(ngx_http_request_t *r,BUFF *b,
			 ngx_array_t *params)
{
  ngx_list_t *headers=&(r->headers_in.headers);
  ngx_list_part_t *chunk=&(headers->part);
  ngx_table_elt_t *table=chunk->elts;
  ngx_keyval_t *keyvals=params->elts;
  ngx_http_request_body_t *body=r->request_body;
  ngx_buf_t *buf=body->buf;
  int post_size=(buf->last-buf->start);
  int n_slots=ngx_list_length(headers)+params->nelts+((post_size)?(1):(0));
  int n_bytes=0, delta=0, i=0, len=chunk->nelts;
  /* Write slotmap header */
  if (buff_needs(b,6)<0) return -1;
  buff_putc(b,0x42); buff_putc(b,0xC1);
  buff_write_4bytes(b,n_slots*2);
  n_bytes=6;
  for (i=0;;i++) {
    if (i>=len) {
      if (chunk->next==NULL) break;
      chunk=chunk->next;
      table=chunk->elts;
      len=chunk->nelts;
      i=0;}
    ngx_table_elt_t kv=table[i];
    delta=buff_write_slotval
      (b,kv.key.data,kv.key.len,kv.value.data,kv.value.len);
    if (delta<0) return delta;
    else n_bytes=n_bytes+delta;}
  i=0; while (i<len) {
    ngx_keyval_t kv=keyvals[i++];
    delta=buff_write_slotval
      (b,kv.key.data,kv.key.len,kv.value.data,kv.value.len);
    if (delta<0) return delta;
    else n_bytes=n_bytes+delta;}
  if (post_size) {
    if ((delta=buff_write_symbol(b,(unsigned char *)"POST_DATA",9))<0)
      return delta;
    n_bytes=n_bytes+delta;
    if (buff_needs(b,post_size+5)<0) return -1;
    buff_putc(b,0x05); buff_write_4bytes(b,post_size);
    buff_write(b,buf->start,post_size);
    n_bytes=n_bytes+post_size+5;}
  return n_bytes;
}

static int ngx_list_length(ngx_list_t *l)
{
  int n=0; ngx_list_part_t *scan=&(l->part);
  while (scan) {
    n=n+scan->nelts;
    scan=scan->next;}
  return n;
}

/* Compatibility and utilities */

#define NGX_LOG_HEAD LOGDEBUG,OK

static char *prealloc(ngx_pool_t *p,char *ptr,int new_size,int old_size)
{
  char *newptr=ngx_pcalloc(p,new_size);
  if (newptr != ptr) memmove(newptr,ptr,old_size);
  return newptr;
}

static void *create_knocgi_config(ngx_conf_t *cf)
{
  ngx_pool_t *p=cf->pool;
  struct KNOCGI_CONFIG *config=
    ngx_pcalloc(p,sizeof(struct KNOCGI_CONFIG));
  if (config == NULL) return NGX_CONF_ERROR;

  config->spec=NGX_CONF_UNSET_PTR;
  config->servlet_wait=NGX_CONF_UNSET_MSEC;
  ngx_array_init(&(config->params),cf->pool,1,sizeof(ngx_keyval_t));

  return (void *) config;
}

static int array_has_key(ngx_array_t *params,ngx_str_t key);

static char *merge_knocgi_config(ngx_conf_t *conf,void *vparent,void *vchild)
{
  ngx_pool_t *p=conf->pool;
  struct KNOCGI_CONFIG *parent=vparent;
  struct KNOCGI_CONFIG *child=vchild;
  ngx_array_t *pparams=&(parent->params), *cparams=&(child->params);
  ngx_keyval_t *scan=pparams->elts, *lim=scan+pparams->nelts;

  if (child->spec==NGX_CONF_UNSET_PTR) child->spec=parent->spec;
  ngx_conf_merge_msec_value
    (child->servlet_wait,parent->servlet_wait,3000);

  while (scan<lim) {
    ngx_keyval_t kv=*scan++;
    if (!(array_has_key(cparams,kv.key))) {
      ngx_keyval_t *nkv=ngx_array_push(cparams);
      nkv->key.data=ngx_pstrdup(p,&(kv.key));
      nkv->key.len=kv.key.len;
      nkv->value.data=ngx_pstrdup(p,&(kv.value));
      nkv->value.len=kv.value.len;}}

  return NGX_CONF_OK;
}

static int array_has_key(ngx_array_t *params,ngx_str_t key) {
  ngx_keyval_t *elts=params->elts, *lim=elts+params->nelts;
  while (elts<lim) {
    if ((elts->key.len==key.len)&&
	(memcmp(elts->key.data,key.data,key.len)==0))
      return 1;
    else elts++;}
  return 0;
}

/* Request handlers */

static ngx_int_t knocgi_create_request(ngx_http_request_t *r)
{
  struct BUFF *cgibuff=buff_create(r->pool,2048);
  ngx_buf_t *b; ngx_chain_t *chain; int cgi_len;
  struct KNOCGI_CONFIG *config=
    ngx_http_get_module_loc_conf(r,ngx_http_knocgi_module);
  if (cgibuff==NULL) return NGX_ERROR;
  if (config==NULL) return NGX_ERROR;
  /* Not handling post right now */
  write_cgidata(r,cgibuff,&(config->params));
  cgi_len=cgibuff->ptr-cgibuff->buf;
  b=ngx_create_temp_buf(r->pool,cgi_len);
  if (!(b)) return NGX_ERROR;
  else {
    memcpy(b->start,cgibuff->buf,cgi_len);
     b->last=b->start+cgi_len;}
  chain=ngx_alloc_chain_link(r->pool);
  if (!(chain)) return NGX_ERROR;
  else {
    chain->buf=b;
    r->upstream->request_bufs=chain;}
  return NGX_OK;
}

static ngx_int_t knocgi_reinit_request(ngx_http_request_t *r)
{
  return NGX_OK;
}

static void knocgi_abort_request(ngx_http_request_t *r)
{
    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                   "abort http knocgi request");
}


static void knocgi_finalize_request(ngx_http_request_t *r,ngx_int_t rc)
{
    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                   "finalize http knocgi request");
}

static ngx_int_t knocgi_process_headers(ngx_http_request_t *r)
{
    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                   "finalize http knocgi request");
    return NGX_OK;
}

static ngx_int_t knocgi_filter_init(void *data)
{
  return NGX_OK;
}

static ngx_int_t knocgi_filter(void *data, ssize_t bytes)
{
  /*
    ngx_http_knocgi_ctx_t  *ctx = data;

    u_char               *last;
    ngx_buf_t            *b;
    ngx_chain_t          *cl, **ll;
    ngx_http_upstream_t  *u;

    u = ctx->request->upstream;
    b = &u->buffer;

    if (u->length == (ssize_t) ctx->rest) {

        if (ngx_strncmp(b->last,
                   ngx_http_memcached_end + NGX_HTTP_MEMCACHED_END - ctx->rest,
                   bytes)
            != 0)
        {
            ngx_log_error(NGX_LOG_ERR, ctx->request->connection->log, 0,
                          "memcached sent invalid trailer");

            u->length = 0;
            ctx->rest = 0;

            return NGX_OK;
        }

        u->length -= bytes;
        ctx->rest -= bytes;

        if (u->length == 0) {
            u->keepalive = 1;
        }

        return NGX_OK;
    }

    for (cl = u->out_bufs, ll = &u->out_bufs; cl; cl = cl->next) {
        ll = &cl->next;
    }

    cl = ngx_chain_get_free_buf(ctx->request->pool, &u->free_bufs);
    if (cl == NULL) {
        return NGX_ERROR;
    }

    cl->buf->flush = 1;
    cl->buf->memory = 1;

    *ll = cl;

    last = b->last;
    cl->buf->pos = last;
    b->last += bytes;
    cl->buf->last = b->last;
    cl->buf->tag = u->output.tag;

    ngx_log_debug4(NGX_LOG_DEBUG_HTTP, ctx->request->connection->log, 0,
                   "memcached filter bytes:%z size:%z length:%z rest:%z",
                   bytes, b->last - b->pos, u->length, ctx->rest);

    if (bytes <= (ssize_t) (u->length - NGX_HTTP_MEMCACHED_END)) {
        u->length -= bytes;
        return NGX_OK;
    }

    last += (size_t) (u->length - NGX_HTTP_MEMCACHED_END);

    if (ngx_strncmp(last, ngx_http_memcached_end, b->last - last) != 0) {
        ngx_log_error(NGX_LOG_ERR, ctx->request->connection->log, 0,
                      "memcached sent invalid trailer");

        b->last = last;
        cl->buf->last = last;
        u->length = 0;
        ctx->rest = 0;

        return NGX_OK;
    }

    ctx->rest -= b->last - last;
    b->last = last;
    cl->buf->last = last;
    u->length = ctx->rest;

    if (u->length == 0) {
        u->keepalive = 1;
    }
  */

    return NGX_OK;
}

/* The main event handler */

static ngx_int_t knocgi_handler(ngx_http_request_t *r)
{
  /* time_t started=time(NULL), connected, requested, computed, responded; */
  ngx_int_t rc;
  ngx_http_upstream_t *u;
  ngx_http_knocgi_ctx_t *ctx;
  struct KNOCGI_CONFIG *config=
    ngx_http_get_module_loc_conf(r,ngx_http_knocgi_module);
  if (config==NULL)
    return NGX_HTTP_INTERNAL_SERVER_ERROR;
  if (ngx_http_upstream_create(r) != NGX_OK)
    return NGX_HTTP_INTERNAL_SERVER_ERROR;
  else u=r->upstream;
  u->conf=&(config->upstream);
  ctx=ngx_pcalloc(r->pool,sizeof(ngx_http_knocgi_ctx_t));
  if (!(ctx)) return NGX_HTTP_INTERNAL_SERVER_ERROR;
  ctx->request=r; time(&(ctx->started));
  ngx_http_set_ctx(r,ctx,ngx_http_knocgi_module);
  u->peer.log=r->connection->log;
  u->peer.log_error=NGX_ERROR_ERR;
  u->output.tag=(ngx_buf_tag_t) &ngx_http_knocgi_module;

  /* setup handlers */
  u->create_request=knocgi_create_request;
  u->reinit_request=knocgi_reinit_request;
  u->process_header=knocgi_process_headers;
  u->abort_request = knocgi_abort_request;
  u->finalize_request = knocgi_finalize_request;

  r->upstream=u;

  rc=ngx_http_read_client_request_body(r,ngx_http_upstream_init);

  if (rc>=NGX_HTTP_SPECIAL_RESPONSE) return rc;
  else return NGX_DONE;
}

static char *servlet_set(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
  char *loc=conf;
  unsigned char **spec_ptr=(unsigned char **)(loc+cmd->offset);
  ngx_str_t *value=cf->args->elts;
  ngx_http_core_loc_conf_t *coreconf=
    ngx_http_conf_get_module_loc_conf(cf,ngx_http_core_module);
  unsigned char *spec=ngx_pstrdup(cf->pool,value);
  if (strcmp((char *)spec,"none")==0) {
    *spec_ptr=NGX_CONF_UNSET_PTR;}
  else {
    coreconf->handler=knocgi_handler;
    *spec_ptr=spec;}

  return NGX_CONF_OK;
}

#define KNOCGI_CONFIG_TAKE0						\
  (NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_NOARGS)
#define KNOCGI_CONFIG_TAKE1						\
  (NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1)
#define KNOCGI_CONFIG_TAKE2						\
  (NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE2)
#define KNOCGI_CONFIG_TAKE3						\
  (NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE3)
#define KNOCGI_CONFIG_UPS1			\
  (NGX_HTTP_UPS_CONF,NGX_CONF_TAKE1)

static ngx_command_t ngx_http_knocgi_cmds[] = {
  {ngx_string("KnocgiletParam"),KNOCGI_CONFIG_TAKE2,
   ngx_conf_set_keyval_slot,
   NGX_HTTP_LOC_CONF_OFFSET,
   offsetof(ngx_http_knocgi_config,params),
   NULL},
  {ngx_string("Knocgilet"),KNOCGI_CONFIG_TAKE1,
   servlet_set,
   NGX_HTTP_LOC_CONF_OFFSET,
   offsetof(ngx_http_knocgi_config,spec),
   NULL},
  {ngx_string("KnocgiletWait"),KNOCGI_CONFIG_TAKE1,
   ngx_conf_set_msec_slot,
   NGX_HTTP_LOC_CONF_OFFSET,
   offsetof(ngx_http_knocgi_config,servlet_wait),
   NULL},
  ngx_null_command
};

static ngx_http_module_t ngx_http_knocgier_module_ctx = {
  NULL,                              /* preconfiguration */
  NULL,                              /* preconfiguration */
  NULL,
  NULL,
  NULL,
  NULL,
  create_knocgi_config,
  merge_knocgi_config
};

ngx_module_t ngx_http_knocgi_module = {
  NGX_MODULE_V1,
  &ngx_http_knocgier_module_ctx,
  ngx_http_knocgi_cmds,
  NGX_HTTP_MODULE,
  NULL,                          /* init master */
  NULL,                          /* init module */
  NULL,                          /* init process */
  NULL,                          /* init thread */
  NULL,                          /* exit thread */
  NULL,                          /* exit process */
  NULL,                          /* exit master */
  NGX_MODULE_V1_PADDING
};

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "cd /src/nginx-1.7.9; make" ***
   ;;;  End: ***
*/

