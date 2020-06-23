/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

static int default_server_loglevel;
#define U8_LOGLEVEL knod_loglevel

#include "kno/knosource.h"
#include "kno/defines.h"
#include "kno/lisp.h"
#include "kno/numbers.h"
#include "kno/support.h"
#include "kno/tables.h"
#include "kno/storage.h"
#include "kno/eval.h"
#include "kno/ports.h"
#include "kno/cprims.h"

#include <libu8/libu8.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8timefns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8fileio.h>
#include <libu8/u8netfns.h>
#include <libu8/u8srvfns.h>
#include <libu8/u8rusage.h>

#include <strings.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <signal.h>
#include <stdio.h>

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

static int knod_loglevel = LOG_NOTIFY;

static int daemonize = 0, foreground = 0, pidwait = 1;

static long long state_files_written = 0;

#define nobytes(in,nbytes) (PRED_FALSE(!(kno_request_bytes(in,nbytes))))
#define havebytes(in,nbytes) (PRED_TRUE(kno_request_bytes(in,nbytes)))

static int default_async_mode = 1;
static int default_auto_reload = 0;
static int server_shutdown = 0;
static int default_stealsockets = 0;

static int debug_maxelts = 32, debug_maxchars = 80;

static void shutdown_onsignal(int sig,siginfo_t *info,void *data);

DEF_KNOSYM(serverfn);

/* Various exceptions */
static u8_condition BadPortSpec=_("Bad port spec");
static u8_condition BadRequest=_("Bad client request");
static u8_condition NoServers=_("NoServers");
static u8_condition Startup=_("Startup");
static u8_condition ServerAbort=_("Knod/ABORT");
static u8_condition ServerConfig=_("Knod/CONFIG");
static u8_condition ServerStartup=_("Knod/STARTUP");
static u8_condition ServerShutdown=_("Knod/SHUTDOWN");

static const sigset_t *server_sigmask;

static u8_condition Incoming=_("Incoming"), Outgoing=_("Outgoing");

static u8_string pid_file = NULL, nid_file = NULL, cmd_file = NULL, inject_file = NULL;

/* This is the global lisp environment for all servers.
   It is modified to include various modules, including dbserv. */
static kno_lexenv working_env = NULL, server_env = NULL;

/* This is the server struct used to establish the server. */
static int default_server_flags=
  U8_SERVER_LOG_LISTEN|U8_SERVER_LOG_CONNECT|U8_SERVER_ASYNC;

/* When the server started, used by UPTIME */
static struct U8_XTIME boot_time;

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
static int default_shutdown_grace = 30000000; /* 30 seconds */
/* Controlling trace activity: logeval prints expressions, logtrans reports
   transactions (request/response pairs). */
static int default_logeval = 0, default_logerrs = 0;
static int default_logtrans = 0, default_logstack = 0;
static int default_backtrace_width = KNO_BACKTRACE_WIDTH;

static int no_storage_api = 0;

/* Types */

static struct KNO_LEXENV default_server_env;

typedef struct KNOSOCKS_SERVER {
  U8_SERVER_FIELDS;
  lispval server_env, server_opts;
  u8_string server_prefix;
  unsigned char logeval, logtrans, logerrs, logstack;
  unsigned char async, reload, storage, stealsockets;
  unsigned char loglevel;
  int shutdown_grace;
  u8_mutex server_lock;
  lispval server_fn;} KNOSOCKS_SERVER;
typedef struct KNOSOCKS_SERVER *knosocks_server;

/* This represents a live client connection and its environment. */
typedef struct KNOSOCKS_CLIENT {
  U8_CLIENT_FIELDS;
  struct KNOSOCKS_SERVER *client_server;
  struct KNO_STREAM client_stream;
  lispval client_data;
  time_t lastlive; double elapsed;} KNOSOCKS_CLIENT;
typedef struct KNOSOCKS_CLIENT *knosocks_client;

/* Managing your dependent (for restarting servers) */

static struct sigaction sigaction_ignore;
static struct sigaction sigaction_shutdown;

static void sigactions_init()
{
  memset(&sigaction_ignore,0,sizeof(sigaction_ignore));
  sigaction_ignore.sa_handler = SIG_IGN;

  memset(&sigaction_shutdown,0,sizeof(sigaction_ignore));
  sigaction_shutdown.sa_sigaction = shutdown_onsignal;
  sigaction_shutdown.sa_flags = SA_SIGINFO;
}

/* Configuration
   This uses the CONFIG facility to setup the server.  Some
   config options just set static variables which control the server,
   while others actually call functions to initialize the server.
*/

/* Cleaning up state files */

/* The server currently writes two state files: a pid file indicating
   the server's process id, and an nid file indicating the ports on which
   the server is listening. */

/* Initializing the server */

typedef int (*server_initfn)(knosocks_server s,lispval val);

static int run_server_inits(knosocks_server s,
			    server_initfn fn,
			    u8_string init_type,
			    lispval inits,
			    int errok)
{
  int result = 0;
  KNO_DO_CHOICES(initval,inits) {
    if (KNO_VECTORP(initval)) {
      lispval *elts = KNO_VECTOR_ELTS(initval);
      lispval *limit = elts+KNO_VECTOR_LENGTH(initval);
      while (elts<limit) {
	int rv = fn(s,*elts);
	if (rv<0) {
	  if (errok) {
	    u8_log(LOGCRIT,"InitFailed",
		   "For %s init on %s: %q",init_type,s->serverid,
		   *elts);
	    kno_clear_errors(1);}
	  else goto abort_init;}
	else {result++; elts++;}}}
    else if (KNO_PAIRP(initval)) {
      lispval scan = initval;
      while (KNO_PAIRP(scan)) {
	lispval car = KNO_CAR(scan);
	scan = KNO_CDR(scan);
	int rv = fn(s,car);
	if (rv<0) {
	  if (errok) {
	    u8_log(LOGCRIT,"InitFailed",
		   "For %s init on %s: %q",init_type,s->serverid,car);
	    kno_clear_errors(1);}
	  else goto abort_init;}
	else result++;}}
    else {
      int rv = fn(s,initval);
      if (rv<0) {
	if (errok) {
	  u8_log(LOGCRIT,"InitFailed",
		 "For %s init on %s: %q",init_type,s->serverid,
		 initval);
	  kno_clear_errors(1);}
	else goto abort_init;}}}
  return result;
 abort_init:
  return -(result+1);
}

static int server_init_opt(knosocks_server s,
			   server_initfn fn,
			   lispval opts,
			   u8_string optname)
{
  lispval sym = kno_intern(optname);
  lispval inits = kno_getopt(opts,sym,KNO_EMPTY);
  int rv = run_server_inits(s,fn,optname,inits,1);
  kno_decref(inits);
  return rv;
}

static int server_listen(knosocks_server srv,lispval spec)
{
  if (KNO_FIXNUMP(spec)) {
    if (KNO_UINTP(spec))
      return u8_add_server((u8_server)srv,NULL,FIX2INT(spec));
    else kno_seterr("BadListenArg","server_listen",srv->serverid,spec);}
  else if (KNO_STRINGP(spec)) {
    u8_string port = KNO_CSTRING(spec);
    if (u8_file_existsp(port)) {
      if (!(u8_socketp(port)))
	kno_seterr("NotASocket","server_listen",srv->serverid,spec);
      else if (srv->stealsockets) {
	int rv = u8_removefile(port);
	if (rv<0)
	  kno_seterr("CantStealSocket","server_listen",srv->serverid,spec);
	else return u8_add_server((u8_server)srv,port,0);}
      else kno_seterr("SocketExists","server_listen",srv->serverid,spec);}
    else return u8_add_server((u8_server)srv,port,0);}
  else kno_seterr("BadListener","server_listen",srv->serverid,spec);
  return -1;
}

/* Core functions */

struct KNOSOCKS_SERVER *start_server(struct KNOSOCKS_SERVER *server)
{
  int rv = server_init_opt(server,server_listen,server->server_opts,"listen");
  if (rv<0) return NULL;
  u8_server_loop((u8_server)server);
  return server;
}

/* This creates the client structure when called by the server loop. */
static u8_client simply_accept(u8_server srv,u8_socket sock,
			       struct sockaddr *addr,size_t len)
{
  knosocks_client client = (knosocks_client)
    u8_client_init(NULL,sizeof(KNOSOCKS_CLIENT),addr,len,sock,srv);
  kno_init_stream(&(client->client_stream),
		  client->idstring,sock,KNO_STREAM_SOCKET,
		  KNO_NETWORK_BUFSIZE);
  /* To help debugging, move the client->idstring (libu8)
     into the stream's id (knostorage). */
  client->client_data = kno_make_slotmap(7,0,NULL);
  client->elapsed = 0; client->lastlive = ((time_t)(-1));
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
  kno_stream stream = &(client->client_stream);
  kno_inbuf inbuf = kno_readbuf(stream);
  int async = ((server->async)&&((client->server->flags)&U8_SERVER_ASYNC));

  /* Set the signal mask for the current thread.  By default, this
     only accepts synchronoyus signals. */
  /* Note that this is called on every loop, but we're presuming it's
     really fast. */
  pthread_sigmask(SIG_SETMASK,server_sigmask,NULL);

  if (server->reload) kno_update_file_modules(0);
  if ((client->reading>0)&&(u8_client_finished(ucl))) {
    expr = kno_read_dtype(kno_readbuf(stream));}
  else if ((client->writing>0)&&(u8_client_finished(ucl))) {
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
    else if ((*(inbuf->bufread)) == dt_block) {
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
    else expr = kno_read_dtype(inbuf);}
  else expr = kno_read_dtype(inbuf);
  kno_reset_threadvars();
  if (expr == KNO_EOD) {
    u8_client_closed(ucl);
    return 0;}
  else if (KNO_ABORTP(expr)) {
    u8_log(LOG_ERR,BadRequest,
	   "%s[%d]: Received bad request %q",
	   client->idstring,client->n_trans,expr);
    kno_clear_errors(1);
    u8_client_close(ucl);
    return -1;}
  else {
    kno_outbuf outbuf = kno_writebuf(stream);
    lispval value;
    int tracethis = ((server->logtrans) &&
		     ((client->n_trans==1) ||
		      (((client->n_trans)%(server->logtrans))==0)));
    int trans_id = client->n_trans, sock = client->socket;
    double xstart = (u8_elapsed_time()), elapsed = -1.0;
    if (knod_loglevel >= LOG_DEBUG)
      u8_log(-LOG_DEBUG,Incoming,"%s[%d/%d]: > %q",
	     client->idstring,sock,trans_id,expr);
    else if (knod_loglevel >= LOG_INFO)
      u8_log(-LOG_INFO,Incoming,
	     "%s[%d/%d]: Received request for execution",
	     client->idstring,sock,trans_id);
    value = kno_exec(expr,client->client_server->server_env,NULL);
    elapsed = u8_elapsed_time()-xstart;
    if (KNO_ABORTP(value)) {
      u8_exception ex = u8_erreify();
      while (ex) {
	struct KNO_EXCEPTION *exo = kno_exception_object(ex);
	lispval irritant = (exo) ? (exo->ex_irritant) : (KNO_VOID);
	if ( (knod_loglevel >= LOG_ERR) || (tracethis) ) {
	  if ((ex->u8x_details) && (!(KNO_VOIDP(irritant))))
	    u8_logf(LOG_ERR,Outgoing,
		    "%s[%d/%d]: %m@%s (%s) %q returned in %fs",
		    client->idstring,sock,trans_id,
		    ex->u8x_cond,ex->u8x_context,
		    ex->u8x_details,irritant,
		    elapsed);
	  else if (ex->u8x_details)
	    u8_logf(LOG_ERR,Outgoing,
		    "%s[%d/%d]: %m@%s (%s) returned in %fs",
		    client->idstring,sock,trans_id,
		    ex->u8x_cond,ex->u8x_context,ex->u8x_details,elapsed);
	  else if (!(KNO_VOIDP(irritant)))
	    u8_logf(LOG_ERR,Outgoing,
		    "%s[%d/%d]: %m@%s -- %q returned in %fs",
		    client->idstring,sock,trans_id,
		    ex->u8x_cond,ex->u8x_context,irritant,elapsed);
	  else u8_logf(LOG_ERR,Outgoing,
		       "%s[%d/%d]: %m@%s -- %q returned in %fs",
		       client->idstring,sock,trans_id,
		       ex->u8x_cond,ex->u8x_context,elapsed);
	  if ( (exo) && (kno_dump_exception) )
	    kno_dump_exception((lispval)exo);}
	ex = ex->u8x_prev;}
      u8_free_exception(ex,1);}
    else if (knod_loglevel >= LOG_DEBUG)
      u8_log(-LOG_DEBUG,Outgoing,
	     "%s[%d/%d]: < %q in %f",
	     client->idstring,sock,trans_id,value,elapsed);
    else if ( (knod_loglevel >= LOG_INFO) || (tracethis) )
      u8_log(-LOG_INFO,Outgoing,"%s[%d/%d]: Request executed in %fs",
	     client->idstring,sock,trans_id,elapsed);
    client->elapsed = client->elapsed+elapsed;
    /* Currently, kno_write_dtype writes the whole thing at once,
       so we just use that. */
    outbuf->bufwrite = outbuf->buffer;
    if (kno_use_dtblock) {
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
	kno_swapcheck();
	return 1;}
      else kno_flush_stream(stream);}
    else {
      kno_write_dtype(outbuf,value);
      kno_flush_stream(stream);}
    time(&(client->lastlive));
    if (tracethis)
      u8_log(LOG_INFO,Outgoing,"%s[%d/%d]: Response sent after %fs",
	     client->idstring,sock,trans_id,u8_elapsed_time()-xstart);
    kno_decref(expr);
    kno_decref(value);
    kno_swapcheck();
    return 0;}
}

static int close_knoclient(u8_client ucl)
{
  knosocks_client client = (knosocks_client)ucl;
  kno_close_stream(&(client->client_stream),0);
  kno_decref(client->client_data);
  ucl->socket = -1;
  return 1;
}

/* Handling signals, etc. */

static u8_condition shutdown_reason = "fate";

#if 0
static void run_shutdown_procs()
{
  lispval procs = shutdown_procs; shutdown_procs = KNO_EMPTY;
  KNO_DO_CHOICES(proc,procs) {
    if (KNO_APPLICABLEP(proc)) {
      lispval shutval, value;
      if (normal_exit) shutval = KNO_FALSE; else shutval = KNO_TRUE;
      u8_log(LOG_WARN,ServerShutdown,"Calling shutdown procedure %q",proc);
      value = kno_apply(proc,1,&shutval);
      if (KNO_ABORTP(value)) {
	u8_log(LOG_CRIT,ServerShutdown,
	       "Error from shutdown procedure %q",proc);
	kno_clear_errors(1);}
      else if (KNO_CONSP(value)) {
	U8_FIXED_OUTPUT(val,1000);
	kno_unparse(valout,value);
	u8_log(LOG_WARN,ServerShutdown,
	       "Finished shutdown procedure %q => %s",
	       proc,val.u8_outbuf);}
      else u8_log(LOG_WARN,ServerShutdown,
		  "Finished shutdown procedure %q => %q",proc,value);
      kno_decref(value);}
    else u8_log(LOG_WARN,"BadShutdownProc",
		"The value %q isn't applicable",proc);}
  kno_decref(procs);
}
#endif

static void shutdown_server(knosocks_server srv,u8_string why)
{
  u8_log(LOGWARN,"ServerShutdown",
	 "Shutting down %s because %s",
	 srv->serverid,U8OPT(why,"fate"));
  u8_server_shutdown((u8_server)srv,srv->shutdown_grace);
}

static lispval shutdown_prim(knosocks_server srv,lispval why)
{
  if (shutdown_reason)
    return KNO_FALSE;
  if (KNO_SYMBOLP(why))
    shutdown_server(srv,KNO_SYMBOL_NAME(why));
  else if (KNO_STRINGP(why))
    shutdown_server(srv,KNO_CSTRING(why));
  else shutdown_server(srv,"knosock/shutdown");
  return KNO_TRUE;
}

#if 0
static void shutdown_onsignal(int sig,siginfo_t *info,void *data)
{
  if (server_shutdown) {
    u8_log(LOG_CRIT,"shutdown_server_onsignal",
	   "Already shutdown but received signal %d",
	   sig);
    return;}
  else server_shutdown=1;
#ifdef SIGHUP
  if (sig == SIGHUP) {
    shutdown_server("SIGHUP");
    return;}
#endif
#ifdef SIGHUP
  if (sig == SIGQUIT) {
    shutdown_server("SIGQUIT");
    return;}
#endif
#ifdef SIGHUP
  if (sig == SIGTERM) {
    shutdown_server("SIGTERM");
    return;}
#endif
  shutdown_server("signal");
}
#endif

/* Miscellaneous Server functions */

static lispval get_boot_time()
{
  return kno_make_timestamp(&boot_time);
}

static lispval get_uptime()
{
  struct U8_XTIME now; u8_now(&now);
  return kno_init_flonum(NULL,u8_xtime_diff(&now,&boot_time));
}

static lispval get_server_status(knosocks_server srv)
{
  lispval result = kno_init_slotmap(NULL,0,NULL);
  struct U8_SERVER_STATS stats, livestats, curstats;

  kno_store(result,kno_intern("nthreads"),KNO_INT(srv->n_threads));
  kno_store(result,kno_intern("nqueued"),KNO_INT(srv->n_queued));
  kno_store(result,kno_intern("nbusy"),KNO_INT(srv->n_busy));
  kno_store(result,kno_intern("nclients"),KNO_INT(srv->n_clients));
  kno_store(result,kno_intern("totaltrans"),KNO_INT(srv->n_trans));
  kno_store(result,kno_intern("totalconn"),KNO_INT(srv->n_accepted));

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

#if 0
static lispval asyncok()
{
  if ((async_mode)&&(dtype_server.flags&U8_SERVER_ASYNC))
    return KNO_TRUE;
  else return KNO_FALSE;
}
#endif

static lispval boundp_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval symbol = kno_get_arg(expr,1);
  if (!(SYMBOLP(symbol)))
    return kno_err(kno_SyntaxError,"boundp_evalfn",NULL,kno_incref(expr));
  else {
    lispval val = kno_symeval(symbol,env);
    if (KNO_VOIDP(val)) return KNO_FALSE;
    else if (val == KNO_UNBOUND) return KNO_FALSE;
    else {
      kno_decref(val); return KNO_TRUE;}}
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
	u8_log(LOG_WARN,ServerConfig,"Unknown boolean setting %s for %q",s,var);
	return kno_reterr(kno_TypeError,"setserverflag","boolean value",val);}
      else u8_log(LOG_WARN,ServerConfig,
		  "Unfamiliar boolean setting %s for %q, assuming %s",
		  s,var,((guess)?("true"):("false")));
      if (!(guess<0)) bool = guess;}
    if (bool) default_server_flags = flags|mask;
    else default_server_flags = flags&(~(mask));}
  else default_server_flags = flags|mask;
  return 1;
}

/* Initializing serverse */

struct KNOSOCKS_SERVER *init_server(struct KNOSOCKS_SERVER *server,
				    u8_string server_prefix,
				    kno_lexenv base_env,
				    lispval opts)
{
  if (server == NULL)
    server = u8_alloc(struct KNOSOCKS_SERVER);
  server = (knosocks_server) u8_init_server
    ((u8_server)server,
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
     U8_SERVER_FLAGS,
     kno_getfixopt(opts,"server_flags",default_server_flags),
     U8_SERVER_END_INIT);
  u8_init_mutex(&(server->server_lock));
  server->server_opts = kno_incref(opts);
  // server->xserverfn = server_loopfn;
  if (kno_testopt(opts,KNOSYM(serverfn),KNO_VOID))
    server->server_fn = kno_getopt(opts,KNOSYM(serverfn),KNO_VOID);
  return server;
}

KNO_EXPORT int kno_init_dbserv(void);

static void init_configs()
{
  kno_register_config
    ("STEALSOCKETS",
     _("Remove existing socket files with extreme prejudice"),
     kno_boolconfig_get,kno_boolconfig_set,&default_stealsockets);
  kno_register_config
    ("KNOD:LOGLEVEL",_("Loglevel for knod itself"),
     kno_intconfig_get,kno_loglevelconfig_set,&default_server_loglevel);
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
  kno_register_config
    ("AUTORELOAD",
     _("Whether to automatically reload changed files"),
     kno_boolconfig_get,kno_boolconfig_set,&default_auto_reload);
  kno_register_config
    ("NOSTORAGEAPI",
     _("Whether to disable exported Kno DB API"),
     kno_boolconfig_get,kno_boolconfig_set,&no_storage_api);
}

static void link_local_cprims()
{
#if 0
  kno_idefn(core_module,
	    kno_make_cprim0("BOOT-TIME",get_boot_time,0,
			    "Returns the time this daemon was started"));
  kno_idefn(core_module,
	    kno_make_cprim0("UPTIME",get_uptime,0,
			    "Returns how long this daemon has been running"));
  kno_idefn(core_module,
	    kno_make_cprim0
	    ("ASYNCOK?",asyncok,0,
	     "Returns true if the daemon can use async I/O processing"));
  kno_idefn(core_module,
	    kno_make_cprim0("SERVER-STATUS",get_server_status,0,
			    "Returns the status of the server"));
#endif
}

/* Initialization */

static long long int knosocks_init = 0;

static lispval knosocks_module;

KNO_EXPORT int kno_init_knosocks()
{
  if (knosocks_init) return 0;
  knosocks_module = kno_new_cmodule("knosocks",0,kno_init_knosocks);

  init_configs();

  link_local_cprims();

  knosocks_init = u8_millitime();

  kno_finish_module(knosocks_module);

  u8_register_source_file(_FILEINFO);

  return 1;
}

