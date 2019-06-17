/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* sqlite.c
   This implements Kno bindings to sqlite3.
   Copyright (C) 2007-2019 beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1

static int zeromq_loglevel = 4;
#define U8_LOGLEVEL (zeromq_loglevel)

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/eval.h"
#include "kno/cprims.h"

#include "kno/zeromq.h"

#include "libu8/u8printf.h"
#include "libu8/u8convert.h"

#include "zeromq_sockopts.h"

#define ZMQ_SOCK_REF(x,i) KNO_ZMQ_SOCK_REF(x,i)
#define ZMQ_SOCKETS(x)    KNO_ZMQ_SOCKETS(x)

static u8_condition ZeroMQ_Error = _("ZeroMQ error");
static u8_condition ZeroMQ_WrongThread =
  _("Using ZeroMQ socket from different thread");

KNO_EXPORT int kno_init_zeromq(void) KNO_LIBINIT_FN;

kno_ptr_type kno_zeromq_type;

static struct KNO_HASHTABLE sockopts_table;

static U8_MUTEX_DECL(zeromq_ctx_lock);

void *kno_zeromq_ctx = NULL;

#ifndef KNO_INIT_ZMQ_SOCKETS_PER_THREAD
#define KNO_INIT_ZMQ_SOCKETS_PER_THREAD 16
#endif

static kno_zmq_thread_data init_zmq_thread_data(void);

#if KNO_USE__THREAD
__thread kno_zmq_thread_data kno_zmq_thread_data = NULL;
#define get_zmq_thread_data() \
  ( (kno_zmq_thread_data) ? (kno_zmq_thread_data) : (init_zmq_thread_data()) )
#else
u8_tld_key kno_zmq_thread_data_key = 0;
#define get_zmq_thread_data()                            \
  ( (u8_tld_get(kno_zmq_thread_data_key)) ?                 \
    (u8_tld_get(kno_zmq_thread_data_key)) :                 \
    (init_zmq_thread_data()) )
#endif

KNO_EXPORT kno_zmq_thread_data kno_thread_zsockets()
{
  return get_zmq_thread_data();
}

/* ZeroMQ contexts */

static void destroy_zeromq_ctx()
{
  if (kno_zeromq_ctx) {
    void *ctx;
    u8_lock_mutex(&zeromq_ctx_lock);
    ctx = kno_zeromq_ctx;
    kno_zeromq_ctx = NULL;
    u8_unlock_mutex(&zeromq_ctx_lock);
    zmq_ctx_destroy(ctx);}
}

KNO_EXPORT void *kno_init_zeromq_ctx()
{
  int initialized = 0;
  u8_lock_mutex(&zeromq_ctx_lock);
  if (kno_zeromq_ctx == NULL) {
    kno_zeromq_ctx = zmq_ctx_new();
    atexit(destroy_zeromq_ctx);
    initialized = 1;}
  u8_unlock_mutex(&zeromq_ctx_lock);
  if (initialized)
    u8_logf(LOG_DEBUG,"ZeroMQ/Init","Done");
  else NO_ELSE;
  return kno_zeromq_ctx;
}

/* Symbols to constants */

static lispval publish_symbol, subscribe_symbol, request_symbol, reply_symbol;
static lispval push_symbol, pull_symbol, pair_symbol, stream_symbol;
static lispval dealer_symbol, router_symbol, xpub_symbol, xsub_symbol;
static lispval nowait_symbol, convert_symbol, wait_symbol;


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

static ssize_t zmq_drop_socket(struct KNO_ZEROMQ *socket);
static ssize_t zmq_add_socket(struct KNO_ZEROMQ *socket);

static struct KNO_ZEROMQ *zmq_make(zmq_type type,void *ptr)
{
  struct KNO_ZEROMQ *zmq = u8_alloc(KNO_ZEROMQ);
  if (zmq == NULL) return NULL;
  KNO_INIT_FRESH_CONS(zmq,kno_zeromq_type);
  zmq->zmq_thread   = 0;
  zmq->zmq_type     = type;
  zmq->zmq_subtype  = KNO_VOID;
  zmq->zmq_flags    = 0;
  zmq->zmq_ptr      = ptr;
  if (type == zmq_socket_type) {
    ssize_t rv = zmq_add_socket(zmq);
    if (rv<0) {
      u8_free(zmq);
      u8_seterr("ZeroMQ_AddSocketFailed","zmq_make",NULL);
      return NULL;}
    zmq->zmq_thread  = pthread_self();}
  u8_logf(LOG_DEBUG,"ZeroMQ/Object",
          "%q in thread %lld",(lispval)zmq,u8_threadid());
  return zmq;
}

static lispval zmq_make_lisp(zmq_type type,void *ptr,lispval subtype)
{
  struct KNO_ZEROMQ *zmq = zmq_make(type,ptr);
  if (zmq) {
    zmq->zmq_subtype = subtype;
    return (lispval) zmq;}
  else return KNO_ERROR;
}

static void recycle_zeromq(struct KNO_RAW_CONS *c)
{
  int rv = 0;
  struct KNO_ZEROMQ *zmq = (kno_zeromq) c;
  u8_logf(LOG_DEBUG,"ZeroMQ/Recycle",
          "Recycling %q in thread %lld",(lispval)zmq,u8_threadid());
  switch (zmq->zmq_type) {
  case zmq_socket_type: {
    int linger = 500;
    rv = zmq_setsockopt(zmq->zmq_ptr,ZMQ_LINGER,&linger,sizeof(linger));
    if (rv == 0) rv = zmq_close(zmq->zmq_ptr);
    if (rv == 0) rv = zmq_drop_socket(zmq);
    break;}
  }
  if (rv < 0) {
    u8_log(LOG_CRIT,"ZeroMQ/Recycle/Failed","rv=%d",rv);}
  else if (!(KNO_STATIC_CONSP(c)))
    u8_free(c);
  else NO_ELSE;
}
static int unparse_zeromq(struct U8_OUTPUT *out,lispval x)
{
  struct KNO_ZEROMQ *zmq = (kno_zeromq) x;
  u8_string typename = "unknown", id = zmq->zmq_id;
  lispval subtype = zmq->zmq_subtype;
  switch (zmq->zmq_type) {
  case zmq_socket_type:
    typename = "socket";
    break;}
  u8_printf(out,"#<ZeroMQ/%s",typename);
  if (!(KNO_VOIDP(subtype)))
    u8_printf(out,"/%q",subtype);
  if (id) u8_printf(out," '%s'",id);
  u8_printf(out," #!0x%llx>",x);
  return 1;
}

/* Utility functions and macros */

static lispval zmq_error(u8_context cxt,lispval obj)
{
  int err = zmq_errno();
  const char *errmsg = zmq_strerror(err);
  u8_logf(LOG_ERROR,"ZeroMQ","Error %s in %s on %q",errmsg,cxt,obj);
  return kno_err(ZeroMQ_Error,cxt,errmsg,obj);
}

#define ZMQ_TYPEP(x,ztype) \
  ( (KNO_TYPEP(x,kno_zeromq_type)) && \
    ( ((kno_zeromq)x)->zmq_type == (ztype) ) )
#define ZMQ_CHECK_TYPE(x,ztype,caller)                                  \
  kno_zeromq U8_MAYBE_UNUSED x ## _zmq;                                 \
  void U8_MAYBE_UNUSED * x ## _ptr;                                     \
  if (! (PRED_FALSE( (KNO_TYPEP(x,kno_zeromq_type)) &&                  \
                     ( (((kno_zeromq)x)->zmq_type) == (ztype) ) )) )    \
    return kno_err(kno_TypeError,caller,# ztype,x);                     \
  else {                                                                \
    x ## _zmq = (kno_zeromq) x;                                         \
    x ## _ptr = x ## _zmq ->zmq_ptr;                                    \
    if ( ( ztype == zmq_socket_type ) &&                                \
         ( (x ## _zmq->zmq_thread) != (pthread_self()) ) ) {             \
      u8_byte buf[128];                                                 \
      return kno_err(ZeroMQ_WrongThread,caller,                         \
                     u8_bprintf(buf,"S0x%llx from T0x%llx used in T0x%llx", \
                                (KNO_LONGVAL(x)),                       \
                                (KNO_LONGVAL(x ## _zmq->zmq_thread)),   \
                                (KNO_LONGVAL(pthread_self()))),         \
                     KNO_VOID);}}

#define ZMQ_CHECK_SOCKET(socket,context)        \


/* Sockets */

DCLPRIM("ZMQ/SOCKET",zmq_socket_prim,MIN_ARGS(1)|MAX_ARGS(2),
	"Creates a ZMQ socket")
static lispval zmq_socket_prim(lispval typename)
{
  void *ctx = KNO_ZMQ_CTX;
  if (ctx == NULL)
    return KNO_ERROR_VALUE;
  int sock_type = get_socket_type(typename);
  if (sock_type < 0)
    return kno_err("InvalidZmqSocketType","zmq_socket_prim",NULL,typename);
  void *sockptr = zmq_socket(ctx,sock_type);
  if (sockptr) {
    struct KNO_ZEROMQ *sock = zmq_make(zmq_socket_type,sockptr);
    if (sock) {
      sock->zmq_subtype = typename;
      return (lispval) sock;}
    else return zmq_error("ZMQ/SOCKET",typename);}
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
	"Unbinds a ZMQ socket from an address")
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
      else return KNO_FALSE;}
    else return zmq_error(KNO_SYMBOL_NAME(optname),socket);}
  case ZMQ_ROUTING_ID:
  case ZMQ_CURVE_PUBLICKEY: case ZMQ_CURVE_SECRETKEY: case ZMQ_CURVE_SERVERKEY: {
    char *val = NULL; ssize_t sz = -1;
    int rv = zmq_getsockopt(socket_ptr,optcode,&val,&sz);
    if (rv == 0) {
      if ( (val) && (sz >= 0) )
	return kno_make_packet(NULL,sz,val);
      else return KNO_FALSE;}
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
      else return KNO_FALSE;}
    else return kno_type_error("packet",KNO_SYMBOL_NAME(optname),socket);}
  default:
    return KNO_FALSE;}
}

/* High level operations */

DCLPRIM("ZMQ/OPEN",zmq_open_prim,MIN_ARGS(2)|MAX_ARGS(2),
	"Opens a ZMQ socket on an address")
static lispval zmq_open_prim(lispval address,lispval typename)
{
  void *ctx = KNO_ZMQ_CTX;
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
    return zmq_make_lisp(zmq_socket_type,sockptr,typename);
  else return zmq_error("ZMQ/OPEN",address);
}

DCLPRIM("ZMQ/LISTEN",zmq_listen_prim,MIN_ARGS(2)|MAX_ARGS(2),
	"Opens a ZMQ socket on an address")
static lispval zmq_listen_prim(lispval address,lispval type)
{
  void *ctx = KNO_ZMQ_CTX;
  if (ctx == NULL)
    return KNO_ERROR_VALUE;
  int sock_type = get_socket_type(type);
  if (sock_type < 0)
    return kno_err("InvalidZmqSocketType","zmq_socket_prim",NULL,type);
  if (!(KNO_STRINGP(address)))
    return kno_err(kno_TypeError,"zmq_listen_prim","address string",address);
  void *sockptr = zmq_socket(ctx,sock_type);
  if (sockptr == NULL)
    return zmq_error("ZMQ/LISTEN",address);
  u8_string addr = KNO_CSTRING(address);
  int rv = zmq_bind(sockptr,addr);
  if (rv == 0)
    return zmq_make_lisp(zmq_socket_type,sockptr,type);
  else return zmq_error("ZMQ/LISTEN",address);
}

/* Sending and receiving */

static void free_lisp_wrapper(void *data,void *vptr)
{
  lispval val = (lispval) vptr;
  kno_decref(val);
}

DCLPRIM("ZMQ/SEND!",zmq_send_prim,MIN_ARGS(2)|MAX_ARGS(3),
	"Sends a string or packet to a socket")
static lispval zmq_send_prim(lispval s,lispval data,lispval opts)
{
  ZMQ_CHECK_TYPE(s,zmq_socket_type,"ZMQ/SEND!");
  zmq_msg_t msg;
  unsigned char *bytes; size_t len;
  int flags = 0, free_bytes = 0, decref_data = 0, free_converter = 0;
  lispval converter = VOID;
  if (KNO_SYMBOLP(opts))
    converter = opts;
  else if (KNO_TABLEP(opts)) {
    converter = kno_getopt(opts,convert_symbol,VOID);
    if (KNO_CONSP(converter)) free_converter = 1;}
  else NO_ELSE;

  if (converter == KNOSYM_DTYPE) {
    struct KNO_OUTBUF out = { 0 };
    KNO_INIT_BYTE_OUTPUT(&out,2048);
    len = kno_write_dtype(&out,data);
    if (len < 0) {
      kno_close_outbuf(&out);
      return KNO_ERROR;}
    else bytes = out.buffer;}
  else if (KNO_PACKETP(data)) {
    bytes = (unsigned char *) KNO_PACKET_DATA(data);
    len   = KNO_PACKET_LENGTH(data);
    kno_incref(data);
    decref_data = 1;}
  else if (KNO_STRINGP(data)) {
    bytes = (unsigned char *) KNO_CSTRING(data);
    len   = KNO_STRLEN(data);
    kno_incref(data);
    decref_data = 1;}
  else return kno_err("string or packet","zmq_send_prim",NULL,data);
  if (!((KNO_TRUEP(opts)) ||
        ( (KNO_TABLEP(opts)) && (kno_testopt(opts,wait_symbol,KNO_VOID)) ) ) )
    flags |= ZMQ_DONTWAIT;
  int rv = (decref_data) ?
    (zmq_msg_init_data(&msg,bytes,len,free_lisp_wrapper,(void *)data)) :
    (zmq_msg_init_data(&msg,bytes,len,NULL,NULL));
  if (rv) {
    if (free_bytes) u8_free(bytes);
    if (free_converter) kno_decref(converter);
    return zmq_error("ZMQ/SEND!",data);}
  ssize_t n_bytes = zmq_msg_send(&msg,s_zmq->zmq_ptr,flags);
  while ( (n_bytes < 0) && (errno == EAGAIN) ) {
    errno = 0;
    n_bytes = zmq_msg_send(&msg,s_zmq->zmq_ptr,flags);}
  zmq_msg_close(&msg);
  if (free_bytes) u8_free(bytes);
  if (free_converter) kno_decref(converter);
  if (n_bytes>=0) {
    U8_CLEAR_ERRNO();
    return KNO_INT(n_bytes);}
  else return zmq_error("ZMQ/SEND!",s);
}

DCLPRIM("ZMQ/RECV",zmq_recv_prim,MIN_ARGS(1)|MAX_ARGS(2),
        "Receives a packet from a socket")
static lispval zmq_recv_prim(lispval s,lispval opts)
{
  ZMQ_CHECK_TYPE(s,zmq_socket_type,"ZMQ/RECV");
  int flags = 0, wait = 1;
  if ( (KNO_FALSEP(opts)) ||
       ( (KNO_TABLEP(opts)) && (kno_testopt(opts,nowait_symbol,KNO_VOID)) ) )
    wait = 0;
  if (!(wait)) flags |= ZMQ_DONTWAIT;
  KNO_DECL_OUTBUF(out,1024);
  int more = 1, n_parts = 0;
  size_t more_size = sizeof (more);
  while (more) {
    /* Create an empty ØMQ message to hold the message part */
    zmq_msg_t part;
    int rc = zmq_msg_init (&part);
    if (rc) {
      kno_close_outbuf(&out);
      return zmq_error("ZMQ/RECV(initmsg)",s);}
    /* Wait for some data */
    int part_len = zmq_msg_recv (&part, s_zmq->zmq_ptr, flags);
    if ( part_len < 0 ) {
      kno_close_outbuf(&out);
      return zmq_error("ZMQ/RECV(recv)",s);}
    /* Check if there's more */
    rc = zmq_getsockopt (s_zmq->zmq_ptr, ZMQ_RCVMORE, &more, &more_size);
    if (rc) {
      kno_close_outbuf(&out);
      return zmq_error("ZMQ/RECV(morep)",s);}
    kno_write_bytes(&out,zmq_msg_data(&part),zmq_msg_size(&part));
    zmq_msg_close (&part);
    n_parts++;}

  lispval converter = VOID; int free_converter = 0;
  if ( (KNO_SYMBOLP(opts)) || (KNO_APPLICABLEP(opts)) )
    converter = opts;
  else if (KNO_TABLEP(opts)) {
    converter = kno_getopt(opts,convert_symbol,VOID);
    if (KNO_CONSP(converter)) free_converter = 1;}
  else NO_ELSE;

  lispval result = VOID;
  unsigned char *bytes = out.buffer;
  ssize_t n_bytes = out.bufwrite-out.buffer;
  if ( converter == KNOSYM_PACKET )
    result = kno_make_packet(NULL,n_bytes,bytes);
  else if ( converter == KNOSYM_UTF8 )
    result = kno_make_string(NULL,n_bytes,bytes);
  else if ( converter == KNOSYM_STRING ) {
    if (u8_validp(bytes))
      result = kno_make_string(NULL,n_bytes,bytes);
    else {
      u8_log(LOGWARN,"ZMQ/RECV(non UTF-8 string)",
             "Converting result to latin0");
      u8_string converted = u8_make_string(latin0_encoding,bytes,bytes+n_bytes);
      result = kno_init_string(NULL,-1,converted);}}
  else if (KNO_STRINGP(converter)) {
    u8_encoding enc = u8_get_encoding(KNO_CSTRING(converter));
    if (enc) {
      u8_string converted  = u8_make_string(enc,bytes,bytes+n_bytes);
      result = kno_init_string(NULL,-1,converted);}
    else result =kno_err("Unknown character encoding","zmq_recv",
                         KNO_CSTRING(converter),s);}
  else if (KNO_APPLICABLEP(converter)) {
    lispval packet = kno_make_packet(NULL,n_bytes,bytes);
    result = kno_apply(converter,1,&packet);
    kno_decref(packet);}
  else result = kno_make_packet(NULL,n_bytes,bytes);
  kno_close_outbuf(&out);
  if (free_converter) kno_decref(converter);
  U8_CLEAR_ERRNO();
  return result;
}

/* Utility functions */

DCLPRIM("ZEROMQ?",zeromqp_prim,MIN_ARGS(1)|MAX_ARGS(2),
        "`(ZEROMQ? *obj*) Returns true if *obj* is a ZeroMQ object.")
static lispval zeromqp_prim(lispval obj,lispval typesym)
{
  if (KNO_TYPEP(obj,kno_zeromq_type)) {
    if (!(KNO_SYMBOLP(typesym)))
      return KNO_TRUE;
    else {
      struct KNO_ZEROMQ *zmq = (kno_zeromq) obj;
      if (zmq->zmq_subtype == typesym)
        return KNO_TRUE;
      else return KNO_FALSE;}}
  else return KNO_FALSE;
}

DCLPRIM("ZMQ/TYPE",zeromq_type_prim,MIN_ARGS(1)|MAX_ARGS(1),
        "`(ZEROMQ/TYPE *obj*) Returns the ZeroMQ type of *obj* or "
        "#f if it's not a ZEROMQ object")
static lispval zeromq_type_prim(lispval obj)
{
  if (KNO_TYPEP(obj,kno_zeromq_type)) {
    struct KNO_ZEROMQ *zmq = (kno_zeromq) obj;
    if (zmq->zmq_type == zmq_socket_type)
      return zmq->zmq_subtype;
    else return KNO_INT(zmq->zmq_type);}
  else return KNO_FALSE;
}

/* Initializing the per-thread socket table */

static int init_zmq_sockets_per_thread = KNO_INIT_ZMQ_SOCKETS_PER_THREAD;

static kno_zmq_thread_data init_zmq_thread_data()
{
#if KNO_USE__THREAD
  kno_zmq_thread_data sockets = kno_zmq_thread_data;
#else
  kno_zmq_thread_data sockets = u8_tld_get(kno_zmq_thread_data_key);
#endif
  if (sockets) {
    if (((sockets)->data_for_thread) == (pthread_self()))
      return sockets;
    else {
      kno_seterr("Thread/ZeroMQ_Error","init_zmq_thread_data",NULL,VOID);
      return NULL;}}
  ssize_t alloc_size = sizeof(struct KNO_ZMQ_THREAD_DATA) +
    (sizeof(kno_zeromq)*(init_zmq_sockets_per_thread-1));
  sockets = u8_malloc(alloc_size); memset(sockets,0,alloc_size);
  sockets->data_for_thread = pthread_self();
  sockets->sockets_len = init_zmq_sockets_per_thread;
  sockets->open_socket = 0;
  sockets->last_socket = -1;
#if KNO_USE__THREAD
  kno_zmq_thread_data = sockets;
#else
  u8_tld_set(kno_zmq_thread_data_key,sockets);
#endif
  return sockets;
}

/* Managing the per-thread socket table */

static kno_zmq_thread_data grow_zmq_thread_data(kno_zmq_thread_data perthread);

static ssize_t zmq_add_socket(struct KNO_ZEROMQ *socket)
{
  kno_zmq_thread_data info = get_zmq_thread_data();
  if (info == NULL) {
    kno_seterr("No thread-local sockets","zmq_add_socket",NULL,KNO_VOID);
    return -1;}
  struct KNO_ZEROMQ **sockets = ZMQ_SOCKETS(info);
  ssize_t off = -1;
  if (info->open_socket < 0) {
    /* Grow the table */
    ssize_t len = info->sockets_len;
    kno_zmq_thread_data new_info = grow_zmq_thread_data(info);
    if (new_info) {
      info = new_info;
      off = len;}
    else return -1;}
  else off = info->open_socket;
  /* Save the socket and the index it is stored at */
  sockets[off] = socket;
  socket->zmq_thread_off = off;
  ssize_t i = off+1, len = info->sockets_len;
  while ( (i < len) && (sockets[i]) ) i++;
  if (i < len) {
    info->open_socket = i;
    if (off > info->last_socket)
      info->last_socket = off;}
  else info->open_socket = -1;
  return off;
}

static ssize_t zmq_drop_socket(struct KNO_ZEROMQ *socket)
{
  kno_zmq_thread_data info = get_zmq_thread_data();
  struct KNO_ZEROMQ **sockets = ZMQ_SOCKETS(info);
  ssize_t off = socket->zmq_thread_off;
  if (off >= 0) {
    struct KNO_ZEROMQ *zmq = sockets[off];
    if ( zmq != socket) {
      u8_byte buf[128];
      kno_seterr("CorruptedThreadSocketTable","zmq_drop_socket",
                 u8_bprintf(buf,"[@%llu] = 0x%llx != 0x%llx, not touching it",
                            off,zmq,socket),
                 (lispval)socket);
      return -1;}
    else {
      sockets[off] = NULL;
      if ( ( (info->open_socket) < 0) ||
           ( (off) < (info->open_socket) ) )
        info->open_socket = off;
      if ( off == info->last_socket ) {
        ssize_t j = off-1;
        while ( (j>=0) && (sockets[j]) ) j--;
        info->last_socket = j;}}}
  return 0;
}

static kno_zmq_thread_data grow_zmq_thread_data(kno_zmq_thread_data perthread)
{
  ssize_t len = perthread->sockets_len;
  ssize_t new_len = len + init_zmq_sockets_per_thread;
  kno_zmq_thread_data new_perthread =
    u8_zalloc(sizeof(struct KNO_ZMQ_THREAD_DATA)+
              ((sizeof(struct KNO_ZEROMQ))*new_len));
  if (new_perthread) {
    memcpy(new_perthread,perthread,
           sizeof(struct KNO_ZMQ_THREAD_DATA)+
           ((sizeof(struct KNO_ZEROMQ))*len));
#if KNO_USE__THREAD
    kno_zmq_thread_data = new_perthread;
#else
    u8_tld_set(kno_zmq_thread_data_key,new_perthread);
#endif
    u8_free(perthread);
    return new_perthread;}
  else {
    u8_seterr(u8_MallocFailed,"grow_thread_table",NULL);
    return NULL;}
}

/* Thread cleanup, closing sockets */

static void kno_zeromq_thread_cleanup()
{
  /* If this hasn't been initalized, nothing could have been saved */
  if (kno_zeromq_ctx == NULL) return;
  kno_zmq_thread_data info = get_zmq_thread_data();
  if (info == NULL) return;
#if KNO_USE__THREAD
    kno_zmq_thread_data = NULL;
#else
    u8_tld_set(kno_zmq_thread_data_key,NULL);
#endif
  ssize_t i = 0, len = info->sockets_len;
  if (len>0)
    u8_logf(LOG_DEBUG,"ZeroMQ/ThreadCleanup",
            "%lld sockets for thread %lld",len,u8_threadid());
  else u8_logf(LOG_DELUGE,"ZeroMQ/ThreadCleanup",
               "%lld sockets for thread %lld",len,u8_threadid());
  struct KNO_ZEROMQ **sockets = ZMQ_SOCKETS(info);
  while (i<len) {
    struct KNO_ZEROMQ *zmq = sockets[i];
    if (zmq == NULL) {i++; continue;}
    else if (zmq->zmq_thread_off < 0) {}
    else if (zmq->zmq_thread_off != i)
      u8_log(LOGCRIT,"CorruptedZMQThreadInfo",
             "%llx [%lld] != %lld",
             zmq,i,zmq->zmq_thread_off);
    else {
      if (zmq->zmq_type == zmq_socket_type) {
        int one = 0;
        int rv = zmq_setsockopt(zmq->zmq_ptr,ZMQ_LINGER,&one,sizeof(one));
        if (rv<0) {
          int err = errno; errno = 0; u8_string errstring = u8_strerror(err);
          u8_log(LOG_ERROR,"SetSockOptFailed",
                 "%s on socket 0x%llx/0x%llx [%s] @%d, trying to close anyway",
                 errstring,zmq,zmq->zmq_ptr,zmq->zmq_id,i);
          if (errno) errno=0;}
        rv = zmq_close(zmq->zmq_ptr);
        if (rv < 0) {
          int err = errno; errno = 0; u8_string errstring = u8_strerror(err);
          u8_log(LOG_ERROR,"ZMQCloseFailed",
                 "%s on socket 0x%llx/0x%llx [%s] @%d",
                 errstring,zmq,zmq->zmq_ptr,zmq->zmq_id,i);}}}
    zmq->zmq_thread_off = -1;
    sockets[i] = NULL;
    i++;}
  u8_free(info);
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
  convert_symbol = kno_intern("convert");
  wait_symbol = kno_intern("wait");
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

  kno_zeromq_type = kno_register_cons_type("ZeroMQ");

  u8_init_mutex(&zeromq_ctx_lock);
  u8_register_threadexit(kno_zeromq_thread_cleanup);

#if ! KNO_USE__THREAD
  u8_new_threadkey(&kno_zmq_thread_data_key,NULL);
  u8_tld_set(kno_zmq_thread_data_key,(void *)NULL);
#endif

  kno_recyclers[kno_zeromq_type]=recycle_zeromq;
  kno_unparsers[kno_zeromq_type]=unparse_zeromq;

  init_symbols();
  init_sockopts();

  DECL_PRIM(zmq_socket_prim,1,module);
  DECL_PRIM(zmq_close_prim,1,module);
  DECL_PRIM(zmq_open_prim,2,module);
  DECL_PRIM(zmq_listen_prim,2,module);

  DECL_PRIM(zeromqp_prim,2,module);
  DECL_PRIM(zeromq_type_prim,1,module);

  DECL_PRIM(zmq_getsockopt_prim,2,module);
  DECL_PRIM(zmq_setsockopt_prim,3,module);

  DECL_PRIM(zmq_bind_prim,2,module);
  DECL_PRIM(zmq_unbind_prim,2,module);
  DECL_PRIM(zmq_connect_prim,2,module);
  DECL_PRIM(zmq_disconnect_prim,2,module);

  DECL_PRIM(zmq_send_prim,3,module);
  DECL_PRIM(zmq_recv_prim,2,module);

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

