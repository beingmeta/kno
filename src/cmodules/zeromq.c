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

static lispval zmq_socket_types;

static u8_condition ZeroMQ_Error=_("ZeroMQ error");

kno_ptr_type kno_zeromq_type;

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
  u8_string typename = "unknown";
  switch (zmq->zmq_type) {
  case zmq_context_type:
    typename = "context"; break;
  case zmq_socket_type:
    typename = "socket"; break;}
  u8_printf(out,"#<ZeroMQ/%s #!0x%llx>",typename,x);
  return 1;
}

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
  kno_zeromq x ## _zmq;					\
  if (! (PRED_FALSE( (KNO_TYPEP(x,kno_zeromq_type)) &&		\
		     ( (((kno_zeromq)x)->zmq_type) == (ztype) ) )) )	\
    return kno_err(kno_TypeError,caller,# ztype,x);		   \
  else x ## _zmq = (kno_zeromq) x

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

/* Sockets */

DCLPRIM("ZMQ/SOCKET",zmq_socket_prim,MIN_ARGS(2)|MAX_ARGS(2),
	"Creates a ZMQ socket")
static lispval zmq_socket_prim(lispval cxt,lispval typename)
{
  ZMQ_CHECK_TYPE(cxt,zmq_context_type,"ZMQ/SOCKET");
  lispval typespec = kno_get(zmq_socket_types,typename,KNO_VOID);
  if (VOIDP(typespec))
    return kno_err("InvalidZmqSocketType","zmq_socket_prim",NULL,typename);
  void *sockptr = zmq_socket(cxt_zmq,FIX2INT(typespec));
  if (sockptr)
    return zmq_make(zmq_socket_type,sockptr);
  else return zmq_error("ZMQ/SOCKET",typename);
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
    return KNO_TRUE;
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
    return KNO_TRUE;
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
    return KNO_TRUE;
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
    return KNO_TRUE;
  else return zmq_error("ZMQ/DISCONNECT!",address);
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

  kno_recyclers[kno_zeromq_type]=recycle_zeromq;
  kno_unparsers[kno_zeromq_type]=unparse_zeromq;

  DECL_PRIM(zmq_context_prim,0,module);
  DECL_PRIM(zmq_shutdown_prim,1,module);
  DECL_PRIM(zmq_terminate_prim,1,module);

  DECL_PRIM(zmq_socket_prim,2,module);
  DECL_PRIM(zmq_bind_prim,2,module);
  DECL_PRIM(zmq_unbind_prim,2,module);
  DECL_PRIM(zmq_connect_prim,2,module);
  DECL_PRIM(zmq_disconnect_prim,2,module);

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

