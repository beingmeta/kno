/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* sqlite.c
   This implements Kno bindings to sqlite3.
   Copyright (C) 2007-2019 beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/eval.h"
#include "kno/cprims.h"

#include "kno/zeromq.h"

#include "libu8/u8printf.h"

#include "zeromq_sockopts.h"

static u8_condition ZeroMQ_Error=_("ZeroMQ error");

KNO_EXPORT int kno_init_zeromq(void) KNO_LIBINIT_FN;

kno_ptr_type kno_zeromq_type;

static size_t default_recv_len = 10000;

static lispval nowait_symbol;

static struct KNO_HASHTABLE sockopts_table;

static void *zmq_default_ctx = NULL;
static U8_MUTEX_DECL(default_ctx_lock);
static void *init_default_ctx()
{
  u8_lock_mutex(&default_ctx_lock);
  if (zmq_default_ctx == NULL)
    zmq_default_ctx = zmq_ctx_new();
  u8_unlock_mutex(&default_ctx_lock);
  return zmq_default_ctx;
}

/* Symbols to constants */

static lispval publish_symbol, subscribe_symbol, request_symbol, reply_symbol;
static lispval push_symbol, pull_symbol, pair_symbol, stream_symbol;
static lispval dealer_symbol, router_symbol, xpub_symbol, xsub_symbol;

static int get_socket_type(lispval symbol)
{
  if (symbol == request_symbol) return ZMQ_REQ;
  else if (symbol == reply_symbol) return ZMQ_REP;
  else if (symbol == publish_symbol) return ZMQ_PUB;
  else if (symbol == subscribe_symbol) return ZMQ_SUB;
  else if (symbol == pair_symbol) return ZMQ_PAIR;
  else if (symbol == stream_symbol) return ZMQ_STREAM;
  else if (symbol == dealer_symbol) return ZMQ_DEALER;
  else if (symbol == router_symbol) return ZMQ_ROUTER;
  else if (symbol == push_symbol) return ZMQ_PUSH;
  else if (symbol == pull_symbol) return ZMQ_PULL;
  else if (symbol == xpub_symbol) return ZMQ_XPUB;
  else if (symbol == xsub_symbol) return ZMQ_XSUB;
  else return -1;
}

/* ZMQ objects */

static lispval zmq_make(zmq_type type,void *ptr)
{
  struct KNO_ZEROMQ *zmq = u8_alloc(KNO_ZEROMQ);
  KNO_INIT_FRESH_CONS(zmq,kno_zeromq_type);
  zmq->zmq_type = type;
  zmq->zmq_ptr  = ptr;
  return (lispval) zmq;
}

static void recycle_zeromq(struct KNO_RAW_CONS *c)
{
  int rv = 0;
  struct KNO_ZEROMQ *zmq = (kno_zeromq) c;
  switch (zmq->zmq_type) {
  case zmq_context_type:
    rv = zmq_ctx_destroy(zmq->zmq_ptr); break;
  case zmq_socket_type:
    rv = 0;
  }
  if (rv != 0) {
    u8_log(LOG_WARN,"ZeroMQ/Recycle/Failed","rv=%d",rv);}
  if (!(KNO_STATIC_CONSP(c))) u8_free(c);
}
static int unparse_zeromq(struct U8_OUTPUT *out,lispval x)
{
  struct KNO_ZEROMQ *zmq = (kno_zeromq) x;
  u8_string typename = "unknown", id = zmq->zmq_id;
  switch (zmq->zmq_type) {
  case zmq_context_type:
    typename = "context"; break;
  case zmq_socket_type:
    typename = "socket"; break;}
  if (id)
    u8_printf(out,"#<ZeroMQ/%s %s #!0x%llx>",typename,id,x);
  else u8_printf(out,"#<ZeroMQ/%s #!0x%llx>",typename,x);
  return 1;
}

/* Utility functions and macros */

static lispval zmq_error(u8_context cxt,lispval obj)
{
  int err = zmq_errno();
  const char *errmsg = zmq_strerror(err);
  return kno_err(ZeroMQ_Error,cxt,errmsg,obj);
}

#define ZMQ_TYPEP(x,ztype) \
  ( (KNO_TYPEP(x,kno_zeromq_type)) && \
    ( ((kno_zeromq)x)->zmq_type == (ztype) ) )
#define ZMQ_CHECK_TYPE(x,ztype,caller)			\
  kno_zeromq x ## _zmq;	void * x ## _ptr;			\
  if (! (PRED_FALSE( (KNO_TYPEP(x,kno_zeromq_type)) &&		\
		     ( (((kno_zeromq)x)->zmq_type) == (ztype) ) )) )	\
    return kno_err(kno_TypeError,caller,# ztype,x);		   \
  else {x ## _zmq = (kno_zeromq) x; x ## _ptr = x ## _zmq ->zmq_ptr; }

/* Contexts */

DCLPRIM("ZMQ/CONTEXT",zmq_context_prim,0,
	"Creates a ZeroMQ context object");
static lispval zmq_context_prim()
{
  void *cxt = zmq_ctx_new();
  if (cxt)
    return zmq_make(zmq_context_type,cxt);
  else return kno_err(ZeroMQ_Error,"zmq_context_prim",NULL,KNO_VOID);
}

DCLPRIM("ZMQ/SHUTDOWN!",zmq_shutdown_prim,0,
	"Shuts down a ZeroMQ context");
static lispval zmq_shutdown_prim(lispval v)
{
  ZMQ_CHECK_TYPE(v,zmq_context_type,"ZMQ/SHUTDOWN!");
  int rv = zmq_ctx_shutdown(v_zmq->zmq_ptr);
  if (rv == 0)
    return KNO_VOID;
  else return zmq_error("ZMQ/SHUTDOWN!",v);
}

DCLPRIM("ZMQ/TERMINATE",zmq_terminate_prim,0,
	"Terminates a ZeroMQ context");
static lispval zmq_terminate_prim(lispval v)
{
  ZMQ_CHECK_TYPE(v,zmq_context_type,"ZMQ/TERMINATE!");
  int rv = zmq_term(v_zmq->zmq_ptr);
  if (rv == 0)
    return KNO_VOID;
  else return zmq_error("ZMQ/TERMINATE!",v);
}

/* Getting contexts for operations */

static void *get_zmq_ctx(lispval v)
{
  if ( (VOIDP(v)) || (KNO_FALSEP(v)) )
    if (zmq_default_ctx)
      return zmq_default_ctx;
    else return init_default_ctx();
  else if (KNO_TYPEP(v,kno_zeromq_type)) {
    struct KNO_ZEROMQ *kz = (kno_zeromq) v;
    if (kz->zmq_type == zmq_context_type)
      return kz->zmq_ptr;}
  kno_seterr(kno_TypeError,"zmq_cxt",NULL,v);
  return NULL;
}

/* Sockets */

DCLPRIM("ZMQ/SOCKET",zmq_socket_prim,MIN_ARGS(1)|MAX_ARGS(2),
	"Creates a ZMQ socket")
static lispval zmq_socket_prim(lispval typename,lispval ctx_arg)
{
  void *ctx = get_zmq_ctx(ctx_arg);
  if (ctx == NULL)
    return KNO_ERROR_VALUE;
  int sock_type = get_socket_type(typename);
  if (sock_type < 0)
    return kno_err("InvalidZmqSocketType","zmq_socket_prim",NULL,typename);
  void *sockptr = zmq_socket(ctx,sock_type);
  if (sockptr)
    return zmq_make(zmq_socket_type,sockptr);
  else return zmq_error("ZMQ/SOCKET",typename);
}

DCLPRIM("ZMQ/CLOSE",zmq_close_prim,MIN_ARGS(1)|MAX_ARGS(1),
	"Closes a ZMQ socket")
static lispval zmq_close_prim(lispval s)
{
  ZMQ_CHECK_TYPE(s,zmq_socket_type,"ZMQ/BIND!");
  int rv = zmq_close(s_zmq->zmq_ptr);
  if (rv)
    return kno_err("ZMQ/CloseFailed","zmq_close_prim",NULL,s);
  else return KNO_FALSE;
}

DCLPRIM("ZMQ/BIND!",zmq_bind_prim,MIN_ARGS(2)|MAX_ARGS(2),
	"Binds a ZMQ socket to an address")
static lispval zmq_bind_prim(lispval s,lispval address)
{
  ZMQ_CHECK_TYPE(s,zmq_socket_type,"ZMQ/BIND!");
  if (!(KNO_STRINGP(address)))
    return kno_err(kno_TypeError,"zmq_bind_prim","address string",address);
  u8_string addr = KNO_CSTRING(address);
  int rv = zmq_bind(s_zmq->zmq_ptr,addr);
  if (rv == 0)
    return kno_incref(s);
  else return zmq_error("ZMQ/BIND!",address);
}

DCLPRIM("ZMQ/UNBIND!",zmq_unbind_prim,MIN_ARGS(2)|MAX_ARGS(2),
	"Unbinds a ZMQ socket to an address")
static lispval zmq_unbind_prim(lispval s,lispval address)
{
  ZMQ_CHECK_TYPE(s,zmq_socket_type,"ZMQ/UNBIND!");
  if (!(KNO_STRINGP(address)))
    return kno_err(kno_TypeError,"zmq_unbind_prim","address string",address);
  u8_string addr = KNO_CSTRING(address);
  int rv = zmq_unbind(s_zmq->zmq_ptr,addr);
  if (rv == 0)
    return kno_incref(s);
  else return zmq_error("ZMQ/UNBIND!",address);
}

DCLPRIM("ZMQ/CONNECT!",zmq_connect_prim,MIN_ARGS(2)|MAX_ARGS(2),
	"Connects a ZMQ socket to an address")
static lispval zmq_connect_prim(lispval s,lispval address)
{
  ZMQ_CHECK_TYPE(s,zmq_socket_type,"ZMQ/CONNECT!");
  if (!(KNO_STRINGP(address)))
    return kno_err(kno_TypeError,"zmq_connect_prim","address string",address);
  u8_string addr = KNO_CSTRING(address);
  int rv = zmq_connect(s_zmq->zmq_ptr,addr);
  if (rv == 0)
    return kno_incref(s);
  else return zmq_error("ZMQ/CONNECT!",address);
}

DCLPRIM("ZMQ/DISCONNECT!",zmq_disconnect_prim,MIN_ARGS(2)|MAX_ARGS(2),
	"Disconnects a ZMQ socket to an address")
static lispval zmq_disconnect_prim(lispval s,lispval address)
{
  ZMQ_CHECK_TYPE(s,zmq_socket_type,"ZMQ/DISCONNECT!");
  if (!(KNO_STRINGP(address)))
    return kno_err(kno_TypeError,"zmq_disconnect_prim","address string",address);
  u8_string addr = KNO_CSTRING(address);
  int rv = zmq_disconnect(s_zmq->zmq_ptr,addr);
  if (rv == 0)
    return kno_incref(s);
  else return zmq_error("ZMQ/DISCONNECT!",address);
}

/* Sockopts */

DCLPRIM("ZMQ/GETOPT",zmq_getsockopt_prim,MIN_ARGS(2)|MAX_ARGS(2),
	"Gets a socket option")
static lispval zmq_getsockopt_prim(lispval socket,lispval optname)
{
  ZMQ_CHECK_TYPE(socket,zmq_socket_type,"ZMQ/DISCONNECT!");
  lispval optcode_val = kno_hashtable_get(&sockopts_table,optname,VOID);
  if (VOIDP(optcode_val))
    return kno_type_error("sockopt","zmq_getsockpt_prim",optname);
  int optcode = KNO_INT(optcode_val);
  switch (optcode) {
    /* Integer values */
  case ZMQ_BACKLOG: case ZMQ_CONNECT_TIMEOUT: case ZMQ_TYPE:
  case ZMQ_EVENTS: case ZMQ_FD: case ZMQ_HANDSHAKE_IVL:
  case ZMQ_IMMEDIATE: case ZMQ_INVERT_MATCHING:
  case ZMQ_IPV4ONLY: case ZMQ_IPV6:
  case ZMQ_LINGER: case ZMQ_MECHANISM:
  case ZMQ_MULTICAST_HOPS: case ZMQ_MULTICAST_MAXTPDU:
  case ZMQ_PLAIN_SERVER: case ZMQ_USE_FD: case ZMQ_RATE:
  case ZMQ_RCVBUF: case ZMQ_RCVHWM: case ZMQ_RCVMORE:
  case ZMQ_RCVTIMEO: case ZMQ_RECONNECT_IVL:
  case ZMQ_RECONNECT_IVL_MAX: case ZMQ_RECOVERY_IVL:
  case ZMQ_SNDBUF: case ZMQ_SNDHWM: case ZMQ_SNDTIMEO:
  case ZMQ_TCP_KEEPALIVE: case ZMQ_TCP_KEEPALIVE_CNT:
  case ZMQ_TCP_KEEPALIVE_IDLE: case ZMQ_TCP_KEEPALIVE_INTVL:
  case ZMQ_TCP_MAXRT: case ZMQ_THREAD_SAFE: case ZMQ_TOS: {
    int val; ssize_t sz = sizeof(val);
    int rv = zmq_getsockopt(socket_ptr,optcode,&val,&sz);
    if (rv == 0)
      return KNO_INT(val);
    else return zmq_error(KNO_SYMBOL_NAME(optname),socket);}
  case ZMQ_VMCI_BUFFER_SIZE:
  case ZMQ_VMCI_BUFFER_MIN_SIZE: case ZMQ_VMCI_BUFFER_MAX_SIZE:
  case ZMQ_AFFINITY: case ZMQ_MAXMSGSIZE: {
    long long val; ssize_t sz = sizeof(val);
    int rv = zmq_getsockopt(socket_ptr,optcode,&val,&sz);
    if (rv == 0)
      return KNO_INT(val);
    else return zmq_error(KNO_SYMBOL_NAME(optname),socket);}
  case ZMQ_LAST_ENDPOINT:
  case ZMQ_PLAIN_PASSWORD: case ZMQ_PLAIN_USERNAME:
  case ZMQ_SOCKS_PROXY: case ZMQ_ZAP_DOMAIN: {
    char *val = NULL; ssize_t sz = -1;
    int rv = zmq_getsockopt(socket_ptr,optcode,&val,&sz);
    if (rv == 0) {
      if ( (val) && (sz >= 0) ) {
	if (u8_validate(val,sz))
	  return kno_make_string(NULL,sz,val);
	else return kno_make_packet(NULL,sz,val);}
      else KNO_FALSE;}
    else return zmq_error(KNO_SYMBOL_NAME(optname),socket);}
  case ZMQ_ROUTING_ID:
  case ZMQ_CURVE_PUBLICKEY: case ZMQ_CURVE_SECRETKEY: case ZMQ_CURVE_SERVERKEY: {
    char *val = NULL; ssize_t sz = -1;
    int rv = zmq_getsockopt(socket_ptr,optcode,&val,&sz);
    if (rv == 0) {
      if ( (val) && (sz >= 0) )
	return kno_make_packet(NULL,sz,val);
      else KNO_FALSE;}
    else return zmq_error(KNO_SYMBOL_NAME(optname),socket);}
  default:
    return KNO_FALSE;}
}

DCLPRIM("ZMQ/SETOPT!",zmq_setsockopt_prim,MIN_ARGS(3)|MAX_ARGS(3),
	"Sets a socket option")
static lispval zmq_setsockopt_prim(lispval socket,lispval optname,
				   lispval value)
{
  ZMQ_CHECK_TYPE(socket,zmq_socket_type,"SOCKOPT/SET!");
  lispval optcode_val = kno_hashtable_get(&sockopts_table,optname,VOID);
  if (VOIDP(optcode_val))
    return kno_type_error("sockopt","zmq_setsockpt_prim",optname);
  int optcode = KNO_INT(optcode_val);
  switch (optcode) {
    /* Integer values */
  case ZMQ_BACKLOG: case ZMQ_CONNECT_TIMEOUT: case ZMQ_TYPE:
  case ZMQ_EVENTS: case ZMQ_FD: case ZMQ_HANDSHAKE_IVL:
  case ZMQ_IMMEDIATE: case ZMQ_INVERT_MATCHING:
  case ZMQ_IPV4ONLY: case ZMQ_IPV6:
  case ZMQ_LINGER: case ZMQ_MECHANISM:
  case ZMQ_MULTICAST_HOPS: case ZMQ_MULTICAST_MAXTPDU:
  case ZMQ_PLAIN_SERVER: case ZMQ_USE_FD: case ZMQ_RATE:
  case ZMQ_RCVBUF: case ZMQ_RCVHWM: case ZMQ_RCVMORE:
  case ZMQ_RCVTIMEO: case ZMQ_RECONNECT_IVL:
  case ZMQ_RECONNECT_IVL_MAX: case ZMQ_RECOVERY_IVL:
  case ZMQ_SNDBUF: case ZMQ_SNDHWM: case ZMQ_SNDTIMEO:
  case ZMQ_TCP_KEEPALIVE: case ZMQ_TCP_KEEPALIVE_CNT:
  case ZMQ_TCP_KEEPALIVE_IDLE: case ZMQ_TCP_KEEPALIVE_INTVL:
  case ZMQ_TCP_MAXRT: case ZMQ_THREAD_SAFE: case ZMQ_TOS: {
    if (!(KNO_FIXNUMP(value)))
      return kno_type_error("fixnum","zmq_setsockopt_prim",value);
    void *valptr = (void *) (KNO_INT(value));
    int rv = zmq_setsockopt(socket_ptr,optcode,valptr,sizeof(valptr));
    if (rv == 0)
      return KNO_TRUE;
    else return zmq_error(KNO_SYMBOL_NAME(optname),socket);}
  case ZMQ_VMCI_BUFFER_SIZE: case ZMQ_VMCI_BUFFER_MIN_SIZE: 
  case ZMQ_VMCI_BUFFER_MAX_SIZE:
  case ZMQ_AFFINITY: case ZMQ_MAXMSGSIZE: {
    if (!((KNO_FIXNUMP(value)) || (KNO_BIGINTP(value))))
      return kno_type_error("integer","zmq_setsockopt_prim",value);
    void *valptr = (void *) (KNO_INT(value));
    int rv = zmq_setsockopt(socket_ptr,optcode,valptr,sizeof(valptr));
    if (rv == 0)
      return KNO_TRUE;
    else return zmq_error(KNO_SYMBOL_NAME(optname),socket);}
  case ZMQ_LAST_ENDPOINT:
  case ZMQ_PLAIN_PASSWORD: case ZMQ_PLAIN_USERNAME:
  case ZMQ_SOCKS_PROXY: case ZMQ_ZAP_DOMAIN: {
    unsigned char *bytes; size_t sz;
    if (KNO_STRINGP(value)) {
      bytes = (unsigned char *) KNO_CSTRING(value);
      sz = KNO_STRLEN(value);}
    else if (KNO_SYMBOLP(value)) {
      bytes = (unsigned char *) KNO_SYMBOL_NAME(value);
      sz = strlen(bytes);}
    else return kno_type_error("string","zmq_setsockopt_prim",value);
    int rv = zmq_setsockopt(socket_ptr,optcode,bytes,sz);
    if (rv == 0)
      return KNO_TRUE;
    else return zmq_error(KNO_SYMBOL_NAME(optname),socket);}
  case ZMQ_ROUTING_ID:
  case ZMQ_CURVE_PUBLICKEY: case ZMQ_CURVE_SECRETKEY: case ZMQ_CURVE_SERVERKEY: {
    unsigned char *bytes = NULL; size_t sz;
    if ( (KNO_PACKETP(value)) || (KNO_SECRETP(value)) ) {
      bytes = (unsigned char *) KNO_PACKET_DATA(value);
      sz = KNO_PACKET_LENGTH(value);
      int rv = zmq_setsockopt(socket_ptr,optcode,bytes,sz);
      if (rv == 0)
	return KNO_TRUE;
      else KNO_FALSE;}
    else return kno_type_error("packet",KNO_SYMBOL_NAME(optname),socket);}
  default:
    return KNO_FALSE;}
}

/* High level operations */

DCLPRIM("ZMQ/OPEN",zmq_open_prim,MIN_ARGS(2)|MAX_ARGS(3),
	"Opens a ZMQ socket on an address")
static lispval zmq_open_prim(lispval address,lispval typename,lispval cxt)
{
  void *ctx = get_zmq_ctx(cxt);
  if (ctx == NULL)
    return KNO_ERROR_VALUE;
  int sock_type = get_socket_type(typename);
  if (sock_type < 0)
    return kno_err("InvalidZmqSocketType","zmq_socket_prim",NULL,typename);
  if (!(KNO_STRINGP(address)))
    return kno_err(kno_TypeError,"zmq_open_prim","address string",address);
  void *sockptr = zmq_socket(ctx,sock_type);
  if (sockptr == NULL)
    return zmq_error("ZMQ/OPEN",address);
  u8_string addr = KNO_CSTRING(address);
  int rv = zmq_connect(sockptr,addr);
  if (rv == 0)
    return zmq_make(zmq_socket_type,sockptr);
  else return zmq_error("ZMQ/OPEN",address);
}

DCLPRIM("ZMQ/LISTEN",zmq_listen_prim,MIN_ARGS(2)|MAX_ARGS(3),
	"Opens a ZMQ socket on an address")
static lispval zmq_listen_prim(lispval address,lispval type,lispval cxt)
{
  void *ctx = get_zmq_ctx(cxt);
  if (ctx == NULL)
    return KNO_ERROR_VALUE;
  int sock_type = get_socket_type(type);
  if (sock_type < 0)
    return kno_err("InvalidZmqSocketType","zmq_socket_prim",NULL,type);
  if (!(KNO_STRINGP(address)))
    return kno_err(kno_TypeError,"zmq_open_prim","address string",address);
  void *sockptr = zmq_socket(ctx,sock_type);
  if (sockptr == NULL)
    return zmq_error("ZMQ/LISTEN",address);
  u8_string addr = KNO_CSTRING(address);
  int rv = zmq_bind(sockptr,addr);
  if (rv == 0)
    return zmq_make(zmq_socket_type,sockptr);
  else return zmq_error("ZMQ/LISTEN",address);
}

/* Sending and receiving */

static void free_lisp_wrapper(void *data,void *vptr)
{
  lispval val = (lispval) vptr;
  kno_decref(val);
}

DCLPRIM("ZMQ/SEND",zmq_send_prim,MIN_ARGS(2)|MAX_ARGS(3),
	"Sends a string or packet to a socket")
static lispval zmq_send_prim(lispval s,lispval data,lispval opts)
{
  ZMQ_CHECK_TYPE(s,zmq_socket_type,"ZMQ/DISCONNECT!");
  zmq_msg_t msg;
  unsigned char *bytes; size_t len; int flags = 0;
  if (KNO_PACKETP(data)) {
    bytes = (unsigned char *) KNO_PACKET_DATA(data);
    len   = KNO_PACKET_LENGTH(data);}
  else if (KNO_STRINGP(data)) {
    bytes = (unsigned char *) KNO_CSTRING(data);
    len   = KNO_STRLEN(data);}
  else return kno_err("string or packet","zmq_send_prim",NULL,data);
  if (KNO_TRUEP(opts)) flags |= ZMQ_DONTWAIT;
  int rv = zmq_msg_init_data(&msg,bytes,len,free_lisp_wrapper,(void *)data);
  if (rv)
    return zmq_error("ZMQ/SEND(msg)",data);
  kno_incref(data);
  ssize_t n_bytes = zmq_msg_send(&msg,s_zmq->zmq_ptr,flags);
  zmq_msg_close(&msg);
  if (n_bytes>=0)
    return KNO_INT(n_bytes);
  else return zmq_error("ZMQ/SEND!",s);
}

DCLPRIM("ZMQ/RECV",zmq_recv_prim,MIN_ARGS(1)|MAX_ARGS(2),
	"Receives a packet from a socket")
static lispval zmq_recv_prim(lispval s,lispval data,lispval opts)
{
  ZMQ_CHECK_TYPE(s,zmq_socket_type,"ZMQ/DISCONNECT!");
  zmq_msg_t msg;
  size_t buflen = default_recv_len;
  int flags = 0;
  if (KNO_TRUEP(opts))
    flags |= ZMQ_DONTWAIT;
  else if (KNO_TABLEP(opts)) {
    lispval lenval = kno_getopt(opts,KNOSYM_BUFSIZE,KNO_VOID);
    if (KNO_FIXNUMP(lenval))
      buflen = KNO_INT(lenval);
    else kno_decref(lenval);
    if (kno_testopt(opts,nowait_symbol,KNO_VOID))
      flags |= ZMQ_DONTWAIT;}
  int rv = zmq_msg_init_size(&msg,buflen);
  if (rv)
    return zmq_error("ZMQ/RECV!(msg)",data);
  /* TODO: Need to handle multi-part messages here */
  ssize_t n_bytes = zmq_msg_recv(&msg,s_zmq->zmq_ptr,flags);
  lispval result = VOID;
  if (n_bytes>=0)
    result = kno_make_packet(NULL,n_bytes,zmq_msg_data(&msg));
  else result = zmq_error("ZMQ/RECV!",s);
  zmq_msg_close(&msg);
  return result;
}

/* Initializing symbols */

static void init_symbols()
{
  publish_symbol = kno_intern("publish");
  subscribe_symbol = kno_intern("subscribe");
  request_symbol = kno_intern("request");
  reply_symbol = kno_intern("reply");
  push_symbol = kno_intern("push");
  pull_symbol = kno_intern("pull");
  pair_symbol = kno_intern("pair");
  stream_symbol = kno_intern("stream");
  dealer_symbol = kno_intern("dealer");
  router_symbol = kno_intern("router");
  xpub_symbol = kno_intern("xpub");
  xsub_symbol = kno_intern("xsub");
  nowait_symbol = kno_intern("nowait");
}

static void init_sockopts()
{
  kno_init_hashtable(&sockopts_table,250,NULL);
  int i = 0; while (initial_sockopts[i].name) {
    lispval symbol = kno_intern(initial_sockopts[i].name);
    lispval intval = KNO_INT(initial_sockopts[i].code);
    kno_hashtable_store(&sockopts_table,symbol,intval);
    i++;}
}

/* Initialization */

static long long int zeromq_init = 0;

KNO_EXPORT int kno_init_zeromq()
{
  lispval module;
  if (zeromq_init) return 0;
  else zeromq_init = u8_millitime();
  module = kno_new_cmodule("zeromq",0,kno_init_zeromq);

  u8_init_mutex(&default_ctx_lock);
  kno_zeromq_type = kno_register_cons_type("ZeroMQ");

  kno_recyclers[kno_zeromq_type]=recycle_zeromq;
  kno_unparsers[kno_zeromq_type]=unparse_zeromq;

  init_symbols();
  init_sockopts();

  DECL_PRIM(zmq_context_prim,0,module);
  DECL_PRIM(zmq_shutdown_prim,1,module);
  DECL_PRIM(zmq_terminate_prim,1,module);

  DECL_PRIM(zmq_socket_prim,2,module);
  DECL_PRIM(zmq_close_prim,1,module);
  DECL_PRIM(zmq_open_prim,3,module);
  DECL_PRIM(zmq_listen_prim,3,module);

  DECL_PRIM(zmq_getsockopt_prim,2,module);
  DECL_PRIM(zmq_setsockopt_prim,3,module);

  DECL_PRIM(zmq_bind_prim,2,module);
  DECL_PRIM(zmq_unbind_prim,2,module);
  DECL_PRIM(zmq_connect_prim,2,module);
  DECL_PRIM(zmq_disconnect_prim,2,module);

  DECL_PRIM(zmq_send_prim,3,module);
  DECL_PRIM(zmq_recv_prim,3,module);

  kno_finish_module(module);

  u8_register_source_file(_FILEINFO);

  return 1;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/

