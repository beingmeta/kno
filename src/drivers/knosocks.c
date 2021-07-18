/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_LOGLEVEL (u8_merge_loglevels(local_loglevel,knosocks_loglevel))
static int knosocks_loglevel = 5;
static int local_loglevel = -1;

#include "kno/knosource.h"
#include "kno/defines.h"
#include "kno/lisp.h"
#include "kno/numbers.h"
#include "kno/support.h"
#include "kno/tables.h"
#include "kno/streams.h"
#include "kno/services.h"
#include "kno/storage.h"
#include "kno/ports.h"
#include "kno/cprims.h"

#include <libu8/libu8.h>
#include <libu8/u8printf.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8timefns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8fileio.h>
#include <libu8/u8netfns.h>
#include <libu8/u8srvfns.h>
#include <libu8/u8rusage.h>
#include <libu8/u8uuid.h>

#include "kno/knosocks.h"

#include <strings.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <signal.h>
#include <stdio.h>
#include <math.h>

#if HAVE_SYS_WAIT_H
#include <sys/wait.h>
#endif

#ifndef KNO_DTBLOCK_THRESH
#define KNO_DTBLOCK_THRESH 256
#endif

#ifndef KNO_BACKTRACE_WIDTH
#define KNO_BACKTRACE_WIDTH 120
#endif

KNO_EXPORT int kno_init_knosocks(void) KNO_LIBINIT_FN;

KNO_EXPORT int kno_update_file_modules(int force);

lispval knosocks_base_module, knosocks_env = KNO_EMPTY_LIST;

#define nobytes(in,nbytes) (RARELY(!(kno_request_bytes(in,nbytes))))
#define havebytes(in,nbytes) (USUALLY(kno_request_bytes(in,nbytes)))

static int default_async_mode = 0;
static int default_stateful = 1;
static int default_stealsockets = 1; /* TODO: set default back */

static int debug_maxelts = 32, debug_maxchars = 80;

static lispval getxrefs_symbol;

DEF_KNOSYM(xrefs); DEF_KNOSYM(clientid); DEF_KNOSYM(serverid);
DEF_KNOSYM(appid); DEF_KNOSYM(execid);
DEF_KNOSYM(serverfn); DEF_KNOSYM(listen); DEF_KNOSYM(protocol);
DEF_KNOSYM(knosock); DEF_KNOSYM(knosocks); DEF_KNOSYM(async);
DEF_KNOSYM(stealsockets); DEF_KNOSYM(stateful);
DEF_KNOSYM(logtrans); DEF_KNOSYM(logeval); DEF_KNOSYM(logerrs);
DEF_KNOSYM(logstack); DEF_KNOSYM(uselog);
DEF_KNOSYM(data); DEF_KNOSYM(password);

static int getboolopt(lispval opts,lispval sym,int dflt)
{
  lispval v = kno_getopt(opts,sym,KNO_VOID);
  if ( (KNO_VOIDP(v)) || (KNO_DEFAULTP(v)) ) return dflt;
  if ( (KNO_FALSEP(v)) || ( v == (KNO_FIXNUM_ZERO) ) ) return 0;
  kno_decref(v);
  return 1;
}

static int getintopt(lispval opts,lispval sym,int dflt)
{
  lispval val = kno_getopt(opts,sym,KNO_VOID);
  if (KNO_VOIDP(val)) return dflt;
  else if (KNO_FIXNUMP(val))
    return KNO_FIX2INT(val);
  else if (KNO_TRUEP(val))
    return 1;
  else if (KNO_FALSEP(val))
    return 0;
  else {
    kno_decref(val);
    return dflt;}
}

/* Thread local knosocks client info */

#if ((KNO_THREADS_ENABLED)&&(KNO_USE_TLS))
static u8_tld_key _curclient_key;
#define cur_client ((knosocks_client)u8_tld_get(_curclient_key))
#define set_cur_client(cl) u8_tld_set(_curclient_key,(cl))
#elif ((KNO_THREADS_ENABLED)&&(HAVE_THREAD_STORAGE_CLASS))
static __thread struct KNOSOCKS_CLIENT *cur_client;
#define set_cur_client(cl) cur_client = (cl)
#else
static struct KNOSOCKS_CLIENT *cur_client;
#define set_cur_client(cl) cur_client = (cl)
#endif

/* Thread-local binding for the current client */

#if ((KNO_THREADS_ENABLED)&&(KNO_USE_TLS))
u8_tld_key _knosockd_curclient_key;
#elif ((KNO_THREADS_ENABLED)&&(HAVE_THREAD_STORAGE_CLASS))
__thread struct KNOSOCKS_CLIENT *knosockd_current;
#else
struct KNOSOCKS_CLIENT *knosockd_current;
#endif

/* Various exceptions */

static u8_condition BadPortSpec=_("Bad port spec");
static u8_condition BadRequest=_("Bad client request");
static u8_condition NoServers=_("NoServers");
static u8_condition ServerStartup=_("Startup(knosocksd)");
static u8_condition ServerShutdown=_("Shutdown(knosocksd)");
static u8_condition ServerConfig=_("Knod/CONFIG");

static u8_string default_password = NULL;

static u8_condition Incoming=_("Incoming"), Outgoing=_("Outgoing");

/* This is the server struct used to establish the server. */
static int default_server_flags=
  U8_SERVER_LOG_LISTEN|U8_SERVER_LOG_CONNECT|U8_SERVER_ASYNC;

/*  Total number of queued requests, served threads, etc. */
static int default_max_queue = 128, default_init_clients = 32;
static int default_n_threads = 4;
/* This is the backlog of connection requests not transactions.
   It is passed as the argument to listen() */
static int default_max_backlog = -1;
/* This is the maximum number of concurrent connections allowed.
   Note that this currently is handled by libu8. */
static int default_max_clients = 0;
/* This is how long to wait for clients to finish when shutting down the
   server.  Note that the server stops listening for new connections right
   away, so we can start another server.  */
static int default_shutdown_grace = 30; /* 30 seconds */
/* Controlling trace activity: logeval prints expressions, logtrans reports
   transactions (request/response pairs). */
static int default_logeval = 0, default_logerrs = 0;
static int default_logtrans = 0, default_logstack = 0;
static int default_backtrace_width = KNO_BACKTRACE_WIDTH;

/* KNOSocks clients */

static kno_service knosocks_open
(lispval spec,kno_service_handler handler,lispval opts)
{
  if (!(KNO_STRINGP(spec))) return NULL;
  u8_string server = KNO_CSTRING(spec);
  if ( (strstr(server,"(protocol=")) &&
       (!(strstr(server,"(protocol=knosock"))) )
    return NULL;
  if ( (kno_testopt(opts,KNOSYM(protocol),KNO_VOID)) &&
       (!((kno_testopt(opts,KNOSYM(protocol),KNOSYM(knosocks)))||
	  (kno_testopt(opts,KNOSYM(protocol),KNOSYM(knosock)))) ) )
    return NULL;
  /* Start out by parsing the address */
  if ((*server)==':') {
    lispval server_id = kno_config_get(server+1);
    if (STRINGP(server_id))
      server = CSTRING(server_id);
    else  {
      kno_seterr("ServerNotConfigured","open_server",server,opts);
      return NULL;}}
  enum KNO_WIRE_PROTOCOL protocol = xtype_protocol;
  u8_string service_spec, service_addr = NULL, service_opts = NULL;
  if ((service_opts=strchr(server,'('))) {
    service_spec = u8_slice(server,service_opts);}
  else service_spec = u8_strdup(server);
  /* Then try to connect, just to see if that works */
  u8_socket socket = u8_connect_x(service_spec,&service_addr);
  int err = (socket<0);
  lispval xrefs = KNO_VOID;
  /* Now do some initialization */
  if (!err) {
    if (protocol==xtype_protocol) {
      int flags = KNO_STREAM_SOCKET;
      struct KNO_PAIR _req;
      KNO_INIT_STATIC_CONS((&_req),kno_pair_type);
      _req.car = getxrefs_symbol;
      _req.cdr = KNO_EMPTY_LIST;
      lispval req = (lispval) (&_req);
      struct KNO_STREAM _stream, *stream =
	kno_init_stream(&_stream,server,socket,flags,5000);
      ssize_t rv = kno_write_xtype(kno_writebuf(stream),req,NULL);
      if (rv>=0) rv = kno_flush_stream(stream);
      if (rv<0) err = 1; else {
	lispval result = kno_read_xtype(kno_readbuf(stream),NULL);
	if (KNO_ABORTED(result)) err=1;
	else if (KNO_FALSEP(result))
	  xrefs = KNO_FALSE;
	else if (KNO_VECTORP(result))
	  xrefs = result;
	else {
	  u8_logf(LOGERR,"BadXrefs","Bad XREFS result from %s: %q",
		  server,result);
	  kno_decref(result);}}
      kno_close_stream(stream,KNO_STREAM_FREEDATA);}
    close(socket);}
  if (err) {
    /* If connecting fails, signal an error rather than creating
       the service connection pool. */
    u8_byte buf[70];
    kno_seterr(kno_ConnectionFailed,"knosocks_open",
	       u8_bprintf(buf,"%s=%s",service_spec,service_addr),
	       VOID);
    if (service_spec) u8_free(service_spec);
    if (service_addr) u8_free(service_addr);
    return NULL;}
  else close(socket);
  struct KNOSOCKS_SERVICE *service = u8_alloc(struct KNOSOCKS_SERVICE);
  KNO_INIT_CONS(service,kno_service_type);
  service->service_id      = u8_strdup(server);
  service->service_addr    = service_addr;
  service->service_spec    = service_spec;
  service->handlers        = handler;
  service->protocol = protocol;
  if (protocol == xtype_protocol) {
    if (KNO_VECTORP(xrefs)) {
      int len = KNO_VECTOR_LENGTH(xrefs), atomicp = 1;
      lispval *refs = u8_alloc_n(len,lispval);
      lispval *elts = KNO_VECTOR_ELTS(xrefs);
      int i = 0; while (i<len) {
	lispval elt = elts[i];
	if ( (atomicp) && (KNO_CONSP(elt)) ) atomicp=0;
	kno_incref(elt);
	refs[i++]=elt;}
      int flags = XTYPE_REFS_READ_ONLY |
	( (atomicp) ? (0) : (XTYPE_REFS_CONS_ELTS));
      kno_init_xrefs(&(service->xrefs),flags,-1,len,len,0,refs,NULL);}
    else kno_init_xrefs(&(service->xrefs),0,-1,0,0,0,NULL,NULL);}
  else memset((&(service->xrefs)),0,sizeof(struct XTYPE_REFS));
  /* And create a connection pool */
  int minsock = kno_getfixopt(opts,"minsock",0);
  int maxsock = kno_getfixopt(opts,"maxsock",0);
  int initsock = kno_getfixopt(opts,"initsock",0);
  service->connpool =
    u8_open_connpool(service->service_id,minsock,maxsock,initsock);
  /* If creating the connection pool fails for some reason,
     cleanup and return an error value. */
  if (service->connpool == NULL) {
    u8_free(service->service_id);
    u8_free(service->service_addr);
    u8_free(service);
    return NULL;}
  return (kno_service) service;
}

static lispval knosocks_apply
(struct KNO_SERVICE *srv,lispval op,int n,kno_argvec args,lispval opts)
{
  struct KNO_STREAM stream;
  struct KNOSOCKS_SERVICE *service = (knosocks_service) srv;
  enum KNO_WIRE_PROTOCOL protocol = service->protocol;
  int xtyped = (protocol == xtype_protocol);
  u8_connpool cpool = service->connpool;
  lispval expr = NIL, result;
  u8_socket conn = u8_get_connection(cpool);
  if (conn<0) return KNO_ERROR;
  kno_init_stream(&stream,NULL,conn,KNO_STREAM_SOCKET,kno_network_bufsize);
  int i = n-1; while (i>=0) {
    lispval arg = args[i--];
    if (KNO_PAIRP(arg))
      expr = kno_init_pair(NULL,kno_make_list(2,KNOSYM_QUOTE,kno_incref(arg)),
			   expr);
    else expr = kno_init_pair(NULL,kno_incref(arg),expr);}
  expr = kno_init_pair(NULL,op,expr);
  int try = (protocol == xtype_protocol) ?
    (kno_write_xtype(kno_writebuf(&stream),expr,&(service->xrefs))<0) :
    (kno_write_dtype(kno_writebuf(&stream),expr)<0);
  if ( (try<0) || (kno_flush_stream(&stream)<0)) {
    /* Need to reconnect */
    kno_clear_errors(1);
    if ((conn = u8_reconnect(cpool,conn))<0) {
      if (conn>0) u8_discard_connection(cpool,conn);
      return KNO_ERROR;}
    if (try<0)
      try = (xtyped) ?
	(kno_write_xtype(kno_writebuf(&stream),expr,&(service->xrefs))<0) :
	(kno_write_dtype(kno_writebuf(&stream),expr)<0);
    if (try<0) return KNO_ERROR;
    else if (kno_flush_stream(&stream)<0) return KNO_ERROR;
    else NO_ELSE;}
  /* Now, read the result */
  result = (xtyped) ? (kno_read_xtype(kno_readbuf(&stream),&(service->xrefs))) :
    (kno_read_dtype(kno_readbuf(&stream)));
  if (KNO_EQ(result,KNO_EOD)) {
    kno_clear_errors(1);
    if (((conn = u8_reconnect(cpool,conn))<0) ||
	(kno_write_dtype(kno_writebuf(&stream),expr)<0) ||
	(kno_flush_stream(&stream)<0)) {
      if (conn>0) u8_discard_connection(cpool,conn);
      return KNO_ERROR;}
    else result = (xtyped) ?
	   (kno_read_xtype(kno_readbuf(&stream),&(service->xrefs))) :
	   (kno_read_dtype(kno_readbuf(&stream)));
    if (KNO_EQ(result,KNO_EOD)) {
      if (conn>0) u8_discard_connection(cpool,conn);
      return kno_err(kno_UnexpectedEOD,"knosocks_apply",
		     service->service_id,expr);}}
  kno_decref(expr);
  kno_close_stream(&stream,KNO_STREAM_FREEDATA);
  u8_return_connection(cpool,conn);
  return result;
}

static int knosocks_close(struct KNO_SERVICE *srv)
{
  struct KNOSOCKS_SERVICE *service = (knosocks_service) srv;
  u8_connpool closed = u8_close_connpool(service->connpool,0);
  if (closed) return 1; else return 0;
}

static lispval knosocks_ctl(struct KNO_SERVICE *srv,lispval op,
			    int n,kno_argvec args)
{
  return KNO_FALSE;
}

static int knosocks_recycler(struct KNO_SERVICE *srv)
{
  struct KNOSOCKS_SERVICE *service = (knosocks_service) srv;
  u8_close_connpool(service->connpool,0);
  kno_recycle_xrefs(&(service->xrefs));
  u8_free(service->service_id); service->service_id=NULL;
  u8_free(service->service_spec); service->service_spec=NULL;
  u8_free(service->service_addr); service->service_addr=NULL;
  kno_decref(srv->opts); srv->opts=KNO_VOID;
  kno_decref(srv->spec); srv->spec=KNO_VOID;
  srv->handlers=NULL;
  return 1;
}

struct KNO_SERVICE_HANDLERS knosocks_handlers =
  {
   "knosocks", /* service_type name */
   NULL, /* more handlers */
   sizeof(struct KNO_SERVICE_HANDLERS), /* struct_size */
   knosocks_open, /* open */
   knosocks_apply, /* apply */
   knosocks_close, /* close */
   knosocks_ctl, /* control */
   knosocks_recycler}; /* more handlers */


/* Server methods */

/* This creates the client structure when called by the server loop. */
static u8_client simply_accept(u8_server srv,u8_socket sock,
			       struct sockaddr *addr,size_t len)
{
  if (sock<0) {
    u8_seterr("BadSocket","simply_accept",u8_strdup(srv->serverid));
    return NULL;}
  knosocks_server ks = (knosocks_server) (srv->serverdata);
  knosocks_client client = (knosocks_client)
    u8_client_init(NULL,sizeof(KNOSOCKS_CLIENT),addr,len,sock,srv);
  kno_init_stream(&(client->client_stream),
		  client->idstring,sock,KNO_STREAM_SOCKET,
		  KNO_NETWORK_BUFSIZE);
  /* To help debugging, move the client->idstring (libu8)
     into the stream's id (knostorage). */
  if (ks->stateful)
    client->client_data = kno_make_slotmap(7,0,NULL);
  else client->client_data = KNO_FALSE;
  client->client_server    = ks;
  client->client_started   = time(NULL);
  client->client_reqstart  = -1;
  client->client_lastlive  = ((time_t)(-1));
  client->client_livetime  = 0;
  client->client_runtime   = 0;
  client->client_log = NULL;
  if (ks->server_xrefs.xt_n_refs)
    client->client_xrefs = &(ks->server_xrefs);
  else client->client_xrefs = NULL;
  client->client_env = kno_incref(ks->server_env);
  u8_getuuid(client->client_uuid);
  u8_set_nodelay(sock,1);
  return (u8_client) client;
}

/* This serves a request on a client. If the libu8 server code
   is functioning properly, it will only be called on connections
   with data on them, so there shouldn't be too much waiting. */
static int execserver(u8_client ucl)
{
  lispval expr;
  knosocks_client client = (knosocks_client)ucl;
  knosocks_server server = client->client_server;
  int local_loglevel =
    (client->client_loglevel>=0) ? (client->client_loglevel) :
    (server->sockserver.server_loglevel);
  enum KNO_WIRE_PROTOCOL protocol = server->server_protocol;
  kno_stream stream = &(client->client_stream);
  kno_inbuf inbuf = kno_readbuf(stream);
  int async = ((server->async)&&((client->server->flags)&U8_SERVER_ASYNC));
  int xtyped = (protocol == xtype_protocol);
  xtype_refs xrefs = (xtyped) ? (client->client_xrefs) : (NULL);

  /* Set the signal mask for the current thread.  By default, this
     only accepts synchronoyus signals. */
  /* Note that this is called on every loop, but we're presuming it's
     really fast. */
  pthread_sigmask(SIG_SETMASK,kno_default_sigmask,NULL);

  if ((client->reading>0)&&(u8_client_finished(ucl))) {
    /* When in async mode, this indicates that all the expected data has
       been buffered  */
    if (xtyped)
      expr = kno_read_xtype(kno_readbuf(stream),xrefs);
    else expr = kno_read_dtype(kno_readbuf(stream));}
  else if ((client->writing>0)&&(u8_client_finished(ucl))) {
    /* ??? Again, in async mode, all of the output has been written */
    struct KNO_RAWBUF *buf = kno_streambuf(stream);
    /* Reset the stream */
    buf->bufpoint = buf->buffer;
    /* Update the stream if we were doing asynchronous I/O */
    if ((client->buf == buf->buffer)&&(client->len))
      buf->buflim = buf->bufpoint+client->len;
    /* And report that we're finished */
    return 0;}
  else if ((client->reading>0)||(client->writing>0))
    /* These should never happen, but let's be complete */
    return 1;
  else if (async) {
    /* See if we can use asynchronous reading */
    if (nobytes(inbuf,1)) expr = KNO_EOD;
    else if ( (xtyped) && ((*(inbuf->bufread)) == dt_block) ) {
      int U8_MAYBE_UNUSED xtcode = kno_read_byte(inbuf);
      int nbytes = kno_read_varint(inbuf);
      if (kno_has_bytes(inbuf,nbytes))
	expr = kno_read_xtype(inbuf,xrefs);
      else {
	struct KNO_RAWBUF *rawbuf = kno_streambuf(stream);
	/* Allocate enough space */
	kno_grow_inbuf(inbuf,nbytes);
	/* Set up the client for async input */
	if (u8_client_read(ucl,rawbuf->buffer,nbytes,
			   rawbuf->buflim-rawbuf->buffer))
	  expr = kno_read_xtype(inbuf,xrefs);
	else return 1;}}
    else if ( (!(xtyped)) && ((*(inbuf->bufread)) == dt_block) ) {
      int U8_MAYBE_UNUSED dtcode = kno_read_byte(inbuf);
      int nbytes = kno_read_4bytes(inbuf);
      if (kno_has_bytes(inbuf,nbytes))
	expr = kno_read_dtype(inbuf);
      else {
	struct KNO_RAWBUF *rawbuf = kno_streambuf(stream);
	/* Allocate enough space */
	kno_grow_inbuf(inbuf,nbytes);
	/* Set up the client for async input */
	if (u8_client_read(ucl,rawbuf->buffer,nbytes,
			   rawbuf->buflim-rawbuf->buffer))
	  expr = kno_read_dtype(inbuf);
	else return 1;}}
    /* TODO: Add an async case for xtypes */
    else if (xtyped)
      expr = kno_read_xtype(inbuf,xrefs);
    else expr = kno_read_dtype(inbuf);}
  else if (xtyped)
    expr = kno_read_xtype(inbuf,xrefs);
  else expr = kno_read_dtype(inbuf);
  if (expr == KNO_EOD) {
    u8_client_closed(ucl);
    return 0;}
  else if (KNO_ABORTP(expr)) {
    u8_logf(LOG_ERR,BadRequest,
	    "%s[%d]: Received bad request %q",
	    client->idstring,client->n_trans,expr);
    kno_clear_errors(1);
    u8_client_close(ucl);
    return -1;}
  else {
    lispval reqdata = (KNO_TABLEP(client->client_data)) ?
      (kno_incref(client->client_data)) :
      (kno_make_slotmap(7,0,NULL));
    kno_reset_threadvars();
    set_cur_client(client);
    kno_set_server_data(server->server_opts);
    kno_use_reqinfo(reqdata);
    kno_decref(reqdata);
    kno_outbuf outbuf = kno_writebuf(stream);
    /* Currently, kno_write_xtype writes the whole thing at once,
       so we just use that. */
    outbuf->bufwrite = outbuf->buffer;
    lispval value;
    int tracethis = ((server->logtrans) &&
		     ((client->n_trans==1) ||
		      (((client->n_trans)%(server->logtrans))==0)));
    int trans_id = client->n_trans, sock = client->socket;
    double xstart = (u8_elapsed_time()), elapsed = -1.0;
    if (knosocks_loglevel >= LOG_DEBUG)
      u8_logf(LOG_DEBUG,Incoming,"%s[%d/%d]: > %q",
	      client->idstring,sock,trans_id,expr);
    else if (knosocks_loglevel >= LOG_INFO)
      u8_logf(LOG_INFO,Incoming,
	      "%s[%d/%d]: Received request for execution",
	      client->idstring,sock,trans_id);
    value = kno_exec(expr,server->server_env,kno_stackptr);
    elapsed = u8_elapsed_time()-xstart;
    if (KNO_ABORTED(value)) {
      u8_exception ex = u8_erreify(), scan = ex;
      while (scan) {
	struct KNO_EXCEPTION *exo = kno_exception_object(scan);
	lispval irritant = (exo) ? (exo->ex_irritant) : (KNO_VOID);
	if ( (knosocks_loglevel >= LOG_ERR) || (tracethis) ) {
	  if ((scan->u8x_details) && (!(KNO_VOIDP(irritant))))
	    u8_logf(LOG_ERR,Outgoing,
		    "%s[%d/%d]: %m@%s (%s) %q returned in %fs",
		    client->idstring,sock,trans_id,
		    scan->u8x_cond,scan->u8x_context,
		    scan->u8x_details,irritant,
		    elapsed);
	  else if (scan->u8x_details)
	    u8_logf(LOG_ERR,Outgoing,
		    "%s[%d/%d]: %m@%s (%s) returned in %fs",
		    client->idstring,sock,trans_id,
		    scan->u8x_cond,scan->u8x_context,scan->u8x_details,
		    elapsed);
	  else if (!(KNO_VOIDP(irritant)))
	    u8_logf(LOG_ERR,Outgoing,
		    "%s[%d/%d]: %m@%s -- %q returned in %fs",
		    client->idstring,sock,trans_id,
		    scan->u8x_cond,scan->u8x_context,irritant,elapsed);
	  else u8_logf(LOG_ERR,Outgoing,
		       "%s[%d/%d]: %m@%s -- %q returned in %fs",
		       client->idstring,sock,trans_id,
		       scan->u8x_cond,scan->u8x_context,elapsed);}
	scan = scan->u8x_prev;}
      value = kno_get_exception(ex);
      kno_incref(value);
      u8_free_exception(ex,1);}
    else if (knosocks_loglevel >= LOG_DEBUG)
      u8_logf(LOG_DEBUG,Outgoing,
	      "%s[%d/%d]: < %q in %f",
	      client->idstring,sock,trans_id,value,elapsed);
    else if ( (knosocks_loglevel >= LOG_INFO) || (tracethis) )
      u8_logf(LOG_INFO,Outgoing,"%s[%d/%d]: Request executed in %fs",
	      client->idstring,sock,trans_id,elapsed);
    client->client_livetime += elapsed;
    if ( (outbuf->bufwrite) > (outbuf->buffer) ) {
      if (async) {
	struct KNO_RAWBUF *rawbuf = (struct KNO_RAWBUF *)inbuf;
	size_t n_bytes = rawbuf->bufpoint-rawbuf->buffer;
	u8_client_write(ucl,rawbuf->buffer,n_bytes,0);
	rawbuf->bufpoint = rawbuf->buffer;
	kno_decref(expr);
	kno_decref(value);
	return 1;}
      else kno_flush_stream(stream);}
    else if ( (kno_use_dtblock) && (!(xtyped)) ) {
      size_t start_off = outbuf->bufwrite-outbuf->buffer;
      size_t nbytes = kno_write_dtype(outbuf,value);
      if (nbytes>KNO_DTBLOCK_THRESH) {
	unsigned char headbuf[6];
	if (kno_needs_space(outbuf,5+nbytes) == 0)
	  return -1;
	size_t head_off = outbuf->bufwrite-outbuf->buffer;
	unsigned char *buf = outbuf->buffer;
	kno_write_byte(outbuf,dt_block);
	kno_write_4bytes(outbuf,nbytes);
	memcpy(headbuf,buf+head_off,5);
	memmove(buf+start_off+5,buf+start_off,nbytes);
	memcpy(buf+start_off,headbuf,5);
	outbuf->bufwrite = buf+5+nbytes;}
      else kno_write_dtype(outbuf,value);
      if (async) {
	struct KNO_RAWBUF *rawbuf = (struct KNO_RAWBUF *)inbuf;
	size_t n_bytes = rawbuf->bufpoint-rawbuf->buffer;
	u8_client_write(ucl,rawbuf->buffer,n_bytes,0);
	rawbuf->bufpoint = rawbuf->buffer;
	kno_decref(expr);
	kno_decref(value);
	return 1;}
      else kno_flush_stream(stream);}
    else {
      if (xtyped)
	kno_write_xtype(outbuf,value,xrefs);
      else kno_write_dtype(outbuf,value);
      kno_flush_stream(stream);}
    time(&(client->client_lastlive));
    set_cur_client(NULL);
    kno_set_server_data(KNO_FALSE);
    kno_use_reqinfo(KNO_FALSE);
    if (tracethis)
      u8_logf(LOG_INFO,Outgoing,"%s[%d/%d]: Response sent after %fs",
	      client->idstring,sock,trans_id,u8_elapsed_time()-xstart);
    kno_decref(expr);
    kno_decref(value);
    return 0;}
}

static int close_knoclient(u8_client ucl)
{
  knosocks_client client = (knosocks_client)ucl;
  kno_close_stream(&(client->client_stream),KNO_STREAM_FREEDATA);
  kno_decref(client->client_data);
  kno_decref(client->client_env);
  ucl->socket = -1;
  return 1;
}

static ssize_t get_xrefs_len(lispval xrefs)
{
  ssize_t sum = 0;
  KNO_DO_CHOICES(xref_init,xrefs) {
    if (KNO_VECTORP(xref_init))
      sum += KNO_VECTOR_LENGTH(xref_init);
    else sum++;}
  return sum;
}

KNO_EXPORT
struct KNOSOCKS_SERVER *new_knosocks_listener
(u8_string serverid,lispval listen,
 lispval opts,lispval env,
 lispval data)
{
  struct KNOSOCKS_SERVER *server = u8_alloc(struct KNOSOCKS_SERVER);
  u8_server s = u8_init_server
    (&(server->sockserver),
     simply_accept, /* acceptfn */
     execserver, /* handlefn */
     NULL, /* donefn */
     close_knoclient, /* closefn */
     U8_SERVER_INIT_CLIENTS,
     kno_getfixopt(opts,"initclients",default_init_clients),
     U8_SERVER_NTHREADS,
     kno_getfixopt(opts,"nthreads",default_n_threads),
     U8_SERVER_BACKLOG,
     kno_getfixopt(opts,"maxbacklog",default_max_backlog),
     U8_SERVER_MAX_QUEUE,
     kno_getfixopt(opts,"maxqueue",default_max_queue),
     U8_SERVER_MAX_CLIENTS,
     kno_getfixopt(opts,"maxclients",default_max_clients),
     U8_SERVER_LOGLEVEL,
     kno_getfixopt(opts,"loglevel",-1),
     U8_SERVER_FLAGS,
     kno_getfixopt(opts,"server_flags",default_server_flags),
     U8_SERVER_END_INIT);
  s->serverid = u8_strdup(serverid);
  int max_xrefs = kno_getfixopt(opts,"xrefsmax",-1);
  if ( (kno_testopt(opts,KNOSYM(xrefs),KNO_VOID)) &&
       (!(kno_testopt(opts,KNOSYM(xrefs),KNO_FIXNUM_ZERO))) ) {
    lispval xrefs = kno_getopt(opts,KNOSYM(xrefs),KNO_VOID);
    ssize_t xrefs_len = get_xrefs_len(xrefs);
    lispval *xrefs_vec = u8_alloc_n(xrefs_len,lispval);
    int xrefs_flags = XTYPE_REFS_ADD_OIDS | XTYPE_REFS_ADD_SYMS;
    kno_init_xrefs(&(server->server_xrefs),xrefs_flags,
		   -1,0,xrefs_len,max_xrefs,
		   xrefs_vec,NULL);
    KNO_DO_CHOICES(xref_init,xrefs) {
      if (KNO_VECTORP(xrefs)) {
	lispval *inits = KNO_VECTOR_ELTS(xrefs);
	int i = 0, lim =KNO_VECTOR_LENGTH(xrefs);
	while (i<lim) {
	  lispval elt = inits[i++];
	  if ( (OIDP(elt)) || (SYMBOLP(elt)) )
	    kno_add_xtype_ref(elt,&(server->server_xrefs));}}
      else if ( (KNO_OIDP(xref_init)) || (KNO_SYMBOLP(xref_init)) )
	kno_add_xtype_ref(xref_init,&(server->server_xrefs));}
    server->server_xrefs.xt_refs_flags |= XTYPE_REFS_READ_ONLY;
    server->server_xrefs.xt_refs_max = server->server_xrefs.xt_n_refs;}
  else kno_init_xrefs(&(server->server_xrefs),XTYPE_REFS_READ_ONLY,
		      -1,0,0,0,NULL,NULL);
  u8_init_mutex(&(server->server_lock));
  lispval base_env    = kno_incref(knosocks_env);
  server->async       = getintopt(opts,KNOSYM(async),default_async_mode);
  server->stateful    = getintopt(opts,KNOSYM(stateful),default_stateful);
  server->logeval     = getintopt(opts,KNOSYM(logeval),default_logeval);
  server->logtrans    = getintopt(opts,KNOSYM(logtrans),default_logtrans);
  server->logerrs     = getintopt(opts,KNOSYM(logerrs),default_logerrs);
  server->logstack    = getintopt(opts,KNOSYM(logstack),default_logstack);
  server->server_data = kno_incref(data);
  lispval config_data = (KNO_TABLEP(data)) ? (kno_incref(data)) :
    (kno_make_slotmap(7,0,NULL));
  if (!(KNO_TABLEP(data))) kno_store(config_data,KNOSYM(data),data);
  server->server_opts = kno_make_pair(config_data,opts);
  server->server_env  = kno_exec_extend(env,base_env);
  u8_now(&(server->server_started));
  u8_getuuid(server->server_uuid);
  server->server_wrapper = KNO_VOID;
  server->stealsockets   = getboolopt(opts,KNOSYM(stealsockets),default_stealsockets);
  lispval password = kno_getopt(opts,KNOSYM(password),KNO_VOID);
  if (KNO_STRINGP(password))
    server->server_password = u8_strdup(KNO_CSTRING(password));
  else if ( (KNO_PACKETP(password)) || (KNO_SECRETP(password)) ) {
    struct KNO_STRING *s = (kno_string) password;
    int len = s->str_bytelen;
    u8_byte *password_string = u8_malloc(len+1);
    memcpy(password_string,s->str_bytes,len);
    password_string[len]='\0';
    server->server_password = password_string;}
  else server->server_password = NULL;
  kno_decref(password);

  lispval log = kno_getopt(opts,KNOSYM(uselog),KNO_VOID);
  kno_decref(log);

  s->serverdata = server;

  if (kno_testopt(opts,KNOSYM(serverfn),KNO_VOID))
    server->server_fn = kno_getopt(opts,KNOSYM(serverfn),KNO_VOID);
  else server->server_fn = KNO_VOID;

  if ( (KNO_CONSP(listen)) || (KNO_UINTP(listen)) )
    knosocks_listen(server,listen);
  else if (!(KNO_FALSEP(listen)))
    u8_add_server(s,serverid,-1);
  else NO_ELSE;

  return server;
}

/* Server flags */

static lispval config_get_server_flag(lispval var,void *data)
{
  kno_ptrbits bigmask = (kno_ptrbits)data;
  unsigned int mask = (unsigned int)(bigmask&0xFFFFFFFF);
  unsigned int flags = default_server_flags;
  if ((flags)&(mask))
    return KNO_TRUE;
  else return KNO_FALSE;
}

static int config_set_server_flag(lispval var,lispval val,void *data)
{
  kno_ptrbits bigmask = (kno_ptrbits)data;
  unsigned int mask = (bigmask&0xFFFFFFFF);
  int flags = default_server_flags;
  if (FALSEP(val))
    default_server_flags = flags&(~(mask));
  else if ((STRINGP(val))&&(STRLEN(val)==0))
    default_server_flags = flags&(~(mask));
  else if (STRINGP(val)) {
    u8_string s = CSTRING(val);
    int bool = kno_boolstring(CSTRING(val),-1);
    if (bool<0) {
      int guess = (((s[0]=='y')||(s[0]=='Y'))?(1):
		   ((s[0]=='N')||(s[0]=='n'))?(0):
		   (-1));
      if (guess<0) {
	u8_logf(LOG_WARN,ServerConfig,
		"Unknown boolean setting %s for %q",s,var);
	return kno_reterr(kno_TypeError,"setserverflag","boolean value",val);}
      else u8_logf(LOG_WARN,ServerConfig,
		   "Unfamiliar boolean setting %s for %q, assuming %s",
		   s,var,((guess)?("true"):("false")));
      if (!(guess<0)) bool = guess;}
    if (bool) default_server_flags = flags|mask;
    else default_server_flags = flags&(~(mask));}
  else default_server_flags = flags|mask;
  return 1;
}

/* Initializing the server */

KNO_EXPORT int knosocks_listen(knosocks_server srv,lispval spec)
{
  u8_server u8s = &(srv->sockserver);
  if (KNO_FIXNUMP(spec)) {
    if (KNO_UINTP(spec))
      return u8_add_server(u8s,NULL,FIX2INT(spec));
    else kno_seterr(BadPortSpec,"knosocks_listen",u8s->serverid,spec);}
  else if (KNO_STRINGP(spec)) {
    u8_string port = KNO_CSTRING(spec);
    if (u8_file_existsp(port)) {
      if (!(u8_socketp(port)))
	kno_seterr("NotASocket","knosocks_listen",u8s->serverid,spec);
      else if (srv->stealsockets) {
	int rv = u8_removefile(port);
	if (rv<0)
	  kno_seterr("CantStealSocket","knosocks_listen",u8s->serverid,spec);
	else return u8_add_server(u8s,port,0);}
      else kno_seterr("SocketExists","knosocks_listen",u8s->serverid,spec);}
    else return u8_add_server(u8s,port,0);}
  else if (KNO_CHOICEP(spec)) {
    int count = 0;
    KNO_DO_CHOICES(sub,spec) {
      int listening = knosocks_listen(srv,sub);
      if (listening<0)
	u8_logf(LOGERR,BadPortSpec,
		"Couldn't listen to %q for %s",
		sub,u8s->serverid);
      else count += listening;}
    return count;}
  else kno_seterr("BadListener","knosocks_listen",u8s->serverid,spec);
  return -1;
}

KNO_EXPORT int knosocks_start(struct KNOSOCKS_SERVER *server)
{
  u8_server s = &(server->sockserver);
  if (s->n_servers == 0) {
    lispval listen = kno_getopt(server->server_opts,KNOSYM(listen),KNO_VOID);
    if (KNO_VOIDP(listen)) {
      kno_seterr(NoServers,"knosocks_start",s->serverid,KNO_VOID);
      return -1;}
    int rv = knosocks_listen(server,listen);
    kno_decref(listen);
    if (rv<0) return rv;
    else if (s->n_servers == 0) {
      kno_seterr(NoServers,"knosocks_start",s->serverid,KNO_VOID);
      return -1;}}
  u8_logf(-LOG_NOTICE,ServerStartup,
	  "Starting knosocks server %s listening on %d addresses",
	  s->serverid,s->n_servers);
  u8_server_loop(s);
  u8_logf(-LOG_NOTICE,ServerShutdown,"Exited knosocks server %s",
	  s->serverid);
  return 1;
}

KNO_EXPORT int knosocks_shutdown(struct KNOSOCKS_SERVER *server,double grace)
{
  u8_server s = &(server->sockserver);
  if ( (s->flags) & (U8_SERVER_CLOSED) )
    return 1;
  else if (grace<0)
    grace = default_shutdown_grace;
  else NO_ELSE;
  u8_logf(LOG_INFO,ServerShutdown,
	  "Shutting down knosocks server %s with a grace period of %f",
	  s->serverid,grace);
  int msecs = (int) ceil(grace*1000000);
  u8_shutdown_server(s,msecs);
  return 0;
}

KNO_EXPORT void knosocks_recycle(struct KNOSOCKS_SERVER *server,double grace)
{
  int rv = knosocks_shutdown(server,grace);
  if (rv<0) {
    int errnum = errno; errno = 0;
    u8_exception ex = u8_erreify();
    if ( (errnum) && (ex) )
      u8_logf(LOGERR,"KnosocksShutdownError",
	      "errno=%d(%s) %s; err: %s <%s> %s%s%s",
	      errnum,u8_strerror(errnum),
	      server->sockserver.serverid,
	      ex->u8x_cond,ex->u8x_context,
	      U8OPTSTR("(",ex->u8x_details,")"));
    else if (errnum)
      u8_logf(LOGERR,"KnosocksShutdownError","errno=%d(%s) %s",
	      errnum,u8_strerror(errnum),
	      server->sockserver.serverid);
    else if (ex)
      u8_logf(LOGERR,"KnosocksShutdownError",
	      "%s; err: %s <%s> %s%s%s",
	      server->sockserver.serverid,
	      ex->u8x_cond,ex->u8x_context,
	      U8OPTSTR("(",ex->u8x_details,")"));
    else u8_logf(LOGERR,"KnosocksShutdownError",
		 "Mysterious error (no errno/exception) on %s",
		 server->sockserver.serverid);
    if (ex) u8_free_exception(ex,1);}
  if (server->server_password) u8_free(server->server_password);
  kno_decref(server->server_fn);
  kno_decref(server->server_opts);
  kno_decref(server->server_env);
  kno_decref(server->server_data);
  kno_recycle_xrefs(&(server->server_xrefs));
  u8_free(server);
}

/* Getting status */

KNO_EXPORT lispval knosocks_status(knosocks_server srv)
{
  u8_server u8s = &(srv->sockserver);
  lispval result = kno_init_slotmap(NULL,0,NULL);
  struct U8_SERVER_STATS stats, livestats, curstats;

  kno_store(result,kno_intern("nthreads"),KNO_INT(u8s->n_threads));
  kno_store(result,kno_intern("nqueued"),KNO_INT(u8s->n_queued));
  kno_store(result,kno_intern("nbusy"),KNO_INT(u8s->n_busy));
  kno_store(result,kno_intern("nclients"),KNO_INT(u8s->n_clients));
  kno_store(result,kno_intern("totaltrans"),KNO_INT(u8s->n_trans));
  kno_store(result,kno_intern("totalconn"),KNO_INT(u8s->n_accepted));

  u8_server_statistics((u8_server)srv,&stats);
  u8_server_livestats((u8_server)srv,&livestats);
  u8_server_curstats((u8_server)srv,&curstats);

  kno_store(result,kno_intern("nactive"),KNO_INT(stats.n_active));
  kno_store(result,kno_intern("nreading"),KNO_INT(stats.n_reading));
  kno_store(result,kno_intern("nwriting"),KNO_INT(stats.n_writing));
  kno_store(result,kno_intern("nxbusy"),KNO_INT(stats.n_busy));

  if (stats.tcount>0) {
    kno_store(result,kno_intern("transavg"),
	      kno_make_flonum(((double)stats.tsum)/
			      (((double)stats.tcount))));
    kno_store(result,kno_intern("transmax"),KNO_INT(stats.tmax));
    kno_store(result,kno_intern("transcount"),KNO_INT(stats.tcount));}

  if (stats.qcount>0) {
    kno_store(result,kno_intern("queueavg"),
	      kno_make_flonum(((double)stats.qsum)/
			      (((double)stats.qcount))));
    kno_store(result,kno_intern("queuemax"),KNO_INT(stats.qmax));
    kno_store(result,kno_intern("queuecount"),KNO_INT(stats.qcount));}

  if (stats.rcount>0) {
    kno_store(result,kno_intern("readavg"),
	      kno_make_flonum(((double)stats.rsum)/
			      (((double)stats.rcount))));
    kno_store(result,kno_intern("readmax"),KNO_INT(stats.rmax));
    kno_store(result,kno_intern("readcount"),KNO_INT(stats.rcount));}

  if (stats.wcount>0) {
    kno_store(result,kno_intern("writeavg"),
	      kno_make_flonum(((double)stats.wsum)/
			      (((double)stats.wcount))));
    kno_store(result,kno_intern("writemax"),KNO_INT(stats.wmax));
    kno_store(result,kno_intern("writecount"),KNO_INT(stats.wcount));}

  if (stats.xcount>0) {
    kno_store(result,kno_intern("execavg"),
	      kno_make_flonum(((double)stats.xsum)/
			      (((double)stats.xcount))));
    kno_store(result,kno_intern("execmax"),KNO_INT(stats.xmax));
    kno_store(result,kno_intern("execcount"),KNO_INT(stats.xcount));}

  if (livestats.tcount>0) {
    kno_store(result,kno_intern("live/transavg"),
	      kno_make_flonum(((double)livestats.tsum)/
			      (((double)livestats.tcount))));
    kno_store(result,kno_intern("live/transmax"),KNO_INT(livestats.tmax));
    kno_store(result,kno_intern("live/transcount"),
	      KNO_INT(livestats.tcount));}

  if (livestats.qcount>0) {
    kno_store(result,kno_intern("live/queueavg"),
	      kno_make_flonum(((double)livestats.qsum)/
			      (((double)livestats.qcount))));
    kno_store(result,kno_intern("live/queuemax"),KNO_INT(livestats.qmax));
    kno_store(result,kno_intern("live/queuecount"),
	      KNO_INT(livestats.qcount));}

  if (livestats.rcount>0) {
    kno_store(result,kno_intern("live/readavg"),
	      kno_make_flonum(((double)livestats.rsum)/
			      (((double)livestats.rcount))));
    kno_store(result,kno_intern("live/readmax"),KNO_INT(livestats.rmax));
    kno_store(result,kno_intern("live/readcount"),
	      KNO_INT(livestats.rcount));}

  if (livestats.wcount>0) {
    kno_store(result,kno_intern("live/writeavg"),
	      kno_make_flonum(((double)livestats.wsum)/
			      (((double)livestats.wcount))));
    kno_store(result,kno_intern("live/writemax"),KNO_INT(livestats.wmax));
    kno_store(result,kno_intern("live/writecount"),
	      KNO_INT(livestats.wcount));}

  if (livestats.xcount>0) {
    kno_store(result,kno_intern("live/execavg"),
	      kno_make_flonum(((double)livestats.xsum)/
			      (((double)livestats.xcount))));
    kno_store(result,kno_intern("live/execmax"),KNO_INT(livestats.xmax));
    kno_store(result,kno_intern("live/execcount"),
	      KNO_INT(livestats.xcount));}

  if (curstats.tcount>0) {
    kno_store(result,kno_intern("cur/transavg"),
	      kno_make_flonum(((double)curstats.tsum)/
			      (((double)curstats.tcount))));
    kno_store(result,kno_intern("cur/transmax"),KNO_INT(curstats.tmax));
    kno_store(result,kno_intern("cur/transcount"),
	      KNO_INT(curstats.tcount));}

  if (curstats.qcount>0) {
    kno_store(result,kno_intern("cur/queueavg"),
	      kno_make_flonum(((double)curstats.qsum)/
			      (((double)curstats.qcount))));
    kno_store(result,kno_intern("cur/queuemax"),KNO_INT(curstats.qmax));
    kno_store(result,kno_intern("cur/queuecount"),
	      KNO_INT(curstats.qcount));}

  if (curstats.rcount>0) {
    kno_store(result,kno_intern("cur/readavg"),
	      kno_make_flonum(((double)curstats.rsum)/
			      (((double)curstats.rcount))));
    kno_store(result,kno_intern("cur/readmax"),KNO_INT(curstats.rmax));
    kno_store(result,kno_intern("cur/readcount"),
	      KNO_INT(curstats.rcount));}

  if (curstats.wcount>0) {
    kno_store(result,kno_intern("cur/writeavg"),
	      kno_make_flonum(((double)curstats.wsum)/
			      (((double)curstats.wcount))));
    kno_store(result,kno_intern("cur/writemax"),KNO_INT(curstats.wmax));
    kno_store(result,kno_intern("cur/writecount"),
	      KNO_INT(curstats.wcount));}

  if (curstats.xcount>0) {
    kno_store(result,kno_intern("cur/execavg"),
	      kno_make_flonum(((double)curstats.xsum)/
			      (((double)curstats.xcount))));
    kno_store(result,kno_intern("cur/execmax"),KNO_INT(curstats.xmax));
    kno_store(result,kno_intern("cur/execcount"),
	      KNO_INT(curstats.xcount));}

  return result;
}

/* Some primitive methods */

DEFC_PRIM("xrefs",getxrefs_prim,KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
	  "(xrefs) returns the XREFS table of the client.")
static lispval getxrefs_prim()
{
  xtype_refs refs = cur_client->client_xrefs;
  kno_stream stream = &(cur_client->client_stream);
  kno_outbuf out = kno_writebuf(stream);
  if (refs) {
    int len = refs->xt_n_refs;
    kno_write_byte(out,xt_vector);
    kno_write_varint(out,len);
    lispval *elts = refs->xt_refs;
    int i = 0; while (i<len) {
      lispval xref = elts[i++];
      kno_write_xtype(out,xref,NULL);}
    return KNO_VOID;}
  else return KNO_FALSE;
}

DEFC_PRIM("clientid",clientid_prim,KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
	  "(clientid) returns the UUID for the current client")
static lispval clientid_prim()
{
  if (cur_client)
    return kno_make_uuid(NULL,cur_client->client_uuid);
  else return KNO_FALSE;
}

DEFC_PRIM("supportedp",supportedp_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "(supportedp *op*) returns true if *op* is in the current clients "
	  "environment",
	  {"op",kno_symbol_type,KNO_VOID})
static lispval supportedp_prim(lispval op)
{
  knosocks_client cl = cur_client;
  if (cl) {
    lispval handler = kno_getopt(cl->client_env,op,KNO_VOID);
    int applicable = (KNO_APPLICABLEP(handler));
    kno_decref(handler);
    if (applicable) return KNO_TRUE; else return KNO_FALSE;}
  else return KNO_FALSE;
}

DEFC_PRIM("shutdown",shutdown_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	  "(shutdown [*password*]) shuts down the server if *password* "
	  "is accepted",
	  {"password",kno_any_type,KNO_VOID})
static lispval shutdown_prim(lispval op)
{
  knosocks_client cl = cur_client;
  if (cl == NULL) return KNO_FALSE;
  knosocks_server srv = cl->client_server;
  if (srv == NULL) return KNO_FALSE;
  u8_string expected = (srv->server_password) ? (srv->server_password) :
    (default_password);
  if (KNO_VOIDP(op)) {
    if (expected) return knostring("Password required");}
  else if (KNO_STRINGP(op)) {
    if ( (!(expected)) || (strcmp(expected,KNO_CSTRING(op))) )
      return knostring("incorrect password");}
  else return knostring("incorrect password");
  int rv = knosocks_shutdown(srv,30);
  if (rv) return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("serverid",serverid_prim,KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
	  "(serverid) returns the UUID for the current client")
static lispval serverid_prim()
{
  if (cur_client) {
    knosocks_server server = cur_client->client_server;
    return kno_make_uuid(NULL,server->server_uuid);}
  else return KNO_FALSE;
}

DEFC_PRIM("now",now_prim,KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
	  "(now) returns the current time")
static lispval now_prim()
{
  struct U8_XTIME now; u8_now(&now);
  return kno_make_timestamp(&now);
}

DEFC_PRIM("uptime",uptime_prim,KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
	  "(uptime) returns the current time")
static lispval uptime_prim()
{
  if (cur_client) {
    knosocks_server server = cur_client->client_server;
    struct U8_XTIME now; u8_now(&now);
    double secs = u8_xtime_diff(&now,&(server->server_started));
    return kno_make_double(secs);}
  else return KNO_FALSE;
}

DEFC_PRIM("sessionid",sessionid_prim,KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
	  "(sessionid) returns the session (process) ID for the server")
static lispval sessionid_prim()
{
  u8_string id = u8_sessionid();
  return knostring(id);
}

/* Accessing server loop state */

KNO_EXPORT knosocks_client knosocks_getclient()
{
  return cur_client;
}
KNO_EXPORT knosocks_server knosocks_getserver()
{
  return cur_client->client_server;
}
KNO_EXPORT void knosocks_setclient(knosocks_client cl)
{
  set_cur_client(cl);
}

/* Setup */

static void init_symbols()
{
  KNOSYM(xrefs); KNOSYM(clientid); KNOSYM(serverid);
  KNOSYM(appid); KNOSYM(execid);
  KNOSYM(serverfn); KNOSYM(listen); KNOSYM(protocol);
  KNOSYM(knosock); KNOSYM(knosocks); KNOSYM(async);
  KNOSYM(stealsockets); KNOSYM(stateful);
  KNOSYM(logtrans); KNOSYM(logeval); KNOSYM(logerrs); KNOSYM(logstack);
  getxrefs_symbol = kno_intern("xrefs");
}

static void init_configs()
{
  kno_register_config
    ("STEALSOCKETS",
     _("Remove existing socket files with extreme prejudice"),
     kno_boolconfig_get,kno_boolconfig_set,&default_stealsockets);
  kno_register_config
    ("KNOSOCKS:LOGLEVEL",_("Loglevel for knod itself"),
     kno_intconfig_get,kno_loglevelconfig_set,&knosocks_loglevel);
  kno_register_config
    ("BACKLOG",
     _("Number of pending connection requests allowed"),
     kno_intconfig_get,kno_intconfig_set,&default_max_backlog);
  kno_register_config
    ("MAXQUEUE",_("Max number of requests to keep queued"),
     kno_intconfig_get,kno_intconfig_set,&default_max_queue);
  kno_register_config
    ("INITCLIENTS",
     _("Number of clients to prepare for/grow by"),
     kno_intconfig_get,kno_intconfig_set,&default_init_clients);
  kno_register_config
    ("SERVERTHREADS",_("Number of threads in the thread pool"),
     kno_intconfig_get,kno_intconfig_set,&default_n_threads);
  kno_register_config
    ("LOGEVAL",_("Whether to log each request and response"),
     kno_boolconfig_get,kno_boolconfig_set,&default_logeval);
  kno_register_config
    ("LOGTRANS",_("Whether to log each transaction"),
     kno_intconfig_get,kno_boolconfig_set,&default_logtrans);
  kno_register_config
    ("LOGERRS",
     _("Whether to log errors returned by the server to clients"),
     kno_boolconfig_get,kno_boolconfig_set,&default_logerrs);
  kno_register_config
    ("LOGSTACK",
     _("Whether to include a detailed backtrace when logging errors"),
     kno_boolconfig_get,kno_boolconfig_set,&default_logstack);
  kno_register_config
    ("BACKTRACEWIDTH",_("Line width for logged backtraces"),
     kno_intconfig_get,kno_intconfig_set,&default_backtrace_width);
  kno_register_config
    ("U8LOGCONNECT",
     _("Whether to have libu8 log each connection"),
     config_get_server_flag,config_set_server_flag,
     (void *)(U8_SERVER_LOG_CONNECT));
  kno_register_config
    ("U8LOGTRANSACT",
     _("Whether to have libu8 log each transaction"),
     config_get_server_flag,config_set_server_flag,
     (void *)(U8_SERVER_LOG_TRANSACT));
#ifdef U8_SERVER_LOG_TRANSFER
  kno_register_config
    ("U8LOGTRANSFER",
     _("Whether to have libu8 log all data transfers"),
     config_get_server_flag,config_set_server_flag,
     (void *)(U8_SERVER_LOG_TRANSFER));
#endif
  kno_register_config
    ("U8ASYNC",
     _("Whether to support thread-asynchronous transactions"),
     config_get_server_flag,config_set_server_flag,
     (void *)(U8_SERVER_ASYNC));

  kno_register_config
    ("DEBUGMAXCHARS",
     _("Max number of string characters to display in debug messages"),
     kno_intconfig_get,kno_intconfig_set,
     &debug_maxchars);
  kno_register_config
    ("DEBUGMAXELTS",
     _("Max number of object elements to display in debug messages"),
     kno_intconfig_get,kno_intconfig_set,
     &debug_maxelts);
  kno_register_config
    ("ASYNCMODE",_("Whether to run in asynchronous mode"),
     kno_boolconfig_get,kno_boolconfig_set,&default_async_mode);
  kno_register_config
    ("GRACEFULDEATH",
     _("How long (μs) to wait for tasks during shutdown"),
     kno_intconfig_get,kno_intconfig_set,&default_shutdown_grace);
}

/* Initialization */

static long long int knosocks_initialized = 0;

static void link_local_cprims()
{
  lispval module = knosocks_base_module;
  KNO_LINK_CPRIM("xrefs",getxrefs_prim,0,module);
  KNO_LINK_CPRIM("clientid",clientid_prim,0,module);
  KNO_LINK_CPRIM("serverid",serverid_prim,0,module);
  KNO_LINK_CPRIM("now",now_prim,0,module);
  KNO_LINK_CPRIM("sessionid",sessionid_prim,0,module);
  KNO_LINK_CPRIM("uptime",uptime_prim,0,module);
  KNO_LINK_CPRIM("supported?",supportedp_prim,1,module);
  KNO_LINK_ALIAS("supportedp",supportedp_prim,module);
  KNO_LINK_CPRIM("shutdown",shutdown_prim,1,module);
}

int kno_init_knosocks_c()
{
  if (knosocks_initialized) return 0;

  knosocks_base_module = kno_make_hashtable(NULL,99);

  knosocks_env = kno_conspair
    (kno_incref(knosocks_base_module),knosocks_env);

#if ((KNO_THREADS_ENABLED)&&(KNO_USE_TLS))
  u8_new_threadkey(&_curclient_key,NULL);
#endif

  init_symbols();
  init_configs();
  link_local_cprims();
  u8_register_source_file(_FILEINFO);

  kno_register_service_handler(&knosocks_handlers);

  knosocks_initialized = u8_millitime();

  return 1;
}
