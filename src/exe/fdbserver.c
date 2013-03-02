/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2013 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#include "framerd/dtype.h"
#include "framerd/numbers.h"
#include "framerd/tables.h"
#include "framerd/fddb.h"
#include "framerd/eval.h"
#include "framerd/ports.h"

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

#include "fdversion.h"

FD_EXPORT void fd_init_texttools(void);
FD_EXPORT void fd_init_tagger(void);

#define nobytes(in,nbytes) (FD_EXPECT_FALSE(!(fd_needs_bytes(in,nbytes))))
#define havebytes(in,nbytes) (FD_EXPECT_TRUE(fd_needs_bytes(in,nbytes)))

static int async_mode=1;

static int debug_maxelts=32, debug_maxchars=80;

/* Various exceptions */
static fd_exception BadPortSpec=_("Bad port spec");
static fd_exception BadRequest=_("Bad client request");
static u8_condition NoServers=_("NoServers");
static u8_condition ServerStartup=_("ServerStart");
static u8_condition ServerShutdown=_("ServerShutdown");

static u8_condition Incoming=_("Incoming"), Outgoing=_("Outgoing");

/* This is the global lisp environment for all servers.
   It is modified to include various modules, including dbserv. */
static fd_lispenv server_env;

/* This is the server struct used to establish the server. */
static struct U8_SERVER dtype_server;
static int server_flags=
  U8_SERVER_LOG_LISTEN|U8_SERVER_LOG_CONNECT|U8_SERVER_ASYNC;

/* When the server started, used by UPTIME */
static struct U8_XTIME boot_time;

/*  Total number of queued requests, served threads, etc. */
static int max_queue=128, init_clients=32, n_threads=4, server_initialized=0;
/* This is the backlog of connection requests not transactions.
   It is passed as the argument to listen() */
static int max_backlog=-1;
/* This is the maximum number of concurrent connections allowed.
   Note that this currently is handled by libu8. */
static int max_conn=0;
/* This is how long to wait for clients to finish when shutting down the
   server.  Note that the server stops listening for new connections right
   away, so we can start another server.  */
static int shutdown_grace=30000000; /* 30 seconds */
/* Controlling trace activity: logeval prints expressions, logtrans reports
   transactions (request/response pairs). */
static int logeval=0, logerrs=0, logtrans=0, logbacktrace=0;

#if FD_THREADS_ENABLED
static u8_mutex init_server_lock;
#endif

static void init_server(void);

/* Configuration
   This uses the CONFIG facility to setup the server.  Some
    config options just set static variables which control the server,
    while others actually call functions to initialize the server.
*/

/* Configuring which ports you're serving.  A port is typically
    a fixnum or a touchtone encoded string and the server will listen
    on all the addresses known for the current hostname.  To listen
    on an additional address (or to limit the port to a particular address,
    specify "port@host".  This can be especially useful to make sure
    the server is listening on localhost (e.g. port@localhost) which
    is often not aliased to the hostname. */
   
static int n_ports=0;

static int config_serve_port(fdtype var,fdtype val,void MAYBE_UNUSED *data)
{
  if (server_initialized==0) init_server();
  if (n_ports<0) return -1;
  else if (FD_FIXNUMP(val)) {
    int retval=u8_add_server(&dtype_server,NULL,FD_FIX2INT(val));
    if (retval<0) n_ports=-1;
    else n_ports=n_ports+retval;
    return retval;}
  else if (FD_STRINGP(val)) {
    int retval=u8_add_server(&dtype_server,FD_STRDATA(val),0);
    if (retval<0) n_ports=-1;
    else n_ports=n_ports+retval;
    return retval;}
  else {
    fd_seterr(BadPortSpec,"config_serve_port",NULL,val);
    return -1;}
}

static fdtype config_get_ports(fdtype var,void MAYBE_UNUSED *data)
{
  fdtype results=FD_EMPTY_CHOICE;
  int i=0, lim=dtype_server.n_servers;
  while (i<lim) {
    fdtype id=fdtype_string(dtype_server.server_info[i].idstring);
    FD_ADD_TO_CHOICE(results,id); i++;}
  return results;
}

/* Configuring dtype_server fields */

static fdtype config_get_dtype_server_flag(fdtype var,void *data)
{
  fd_ptrbits bigmask=(fd_ptrbits)data;
  unsigned int mask=(unsigned int)(bigmask&0xFFFFFFFF), flags;
  fd_lock_mutex(&init_server_lock);
  if (server_initialized) flags=dtype_server.flags;
  else flags=server_flags;
  fd_unlock_mutex(&init_server_lock);
  if ((flags)&(mask)) return FD_TRUE; else return FD_FALSE;
}

static int config_set_dtype_server_flag(fdtype var,fdtype val,void *data)
{
  fd_ptrbits bigmask=(fd_ptrbits)data;
  unsigned int mask=(bigmask&0xFFFFFFFF), *flagsp, flags;
  fd_lock_mutex(&init_server_lock);
  if (server_initialized) {
    flags=dtype_server.flags; flagsp=&(dtype_server.flags);}
  else {
    flags=server_flags; flagsp=&(server_flags);}
  if (FD_FALSEP(val))
    *flagsp=flags&(~(mask));
  else if ((FD_STRINGP(val))&&(FD_STRLEN(val)==0))
    *flagsp=flags&(~(mask));
  else if (FD_STRINGP(val)) {
    u8_string s=FD_STRDATA(val);
    int bool=fd_boolstring(FD_STRDATA(val),-1);
    if (bool<0) {
      int guess=(((s[0]=='y')||(s[0]=='Y'))?(1):
		 ((s[0]=='N')||(s[0]=='n'))?(0):
		 (-1));
      if (guess<0) {
	u8_log(LOG_WARN,"SERVERFLAG","Unknown boolean setting %s",s);
	fd_unlock_mutex(&init_server_lock);
	return fd_reterr(fd_TypeError,"setserverflag","boolean value",val);}
      else u8_log(LOG_WARN,"SERVERFLAG",
		  "Unfamiliar boolean setting %s, assuming %s",
		  s,((guess)?("true"):("false")));
      if (!(guess<0)) bool=guess;}
    if (bool) *flagsp=flags|mask;
    else *flagsp=flags&(~(mask));}
  else *flagsp=flags|mask;
  fd_unlock_mutex(&init_server_lock);
  return 1;
}

/* Configuring whether clients have access to a full Scheme interpreter
   or only explicitly exported functions.  Note that even if they have
   a full scheme interpreter, they still only have access to "safe" functions,
   and can't access the filesystem, network, etc. */
   
static int fullscheme=0;

static int config_set_fullscheme(fdtype var,fdtype val,void MAYBE_UNUSED *data)
{
  int oldval=fullscheme;
  if (FD_TRUEP(val))  fullscheme=1; else fullscheme=0;
  return (oldval!=fullscheme);
}

static fdtype config_get_fullscheme(fdtype var,void MAYBE_UNUSED *data)
{
  if (fullscheme) return FD_TRUE; else return FD_FALSE;
}

/* Cleaning up state files */

/* The server currently writes two state files: a pid file indicating
    the server's process id, and an nid file indicating the ports on which
    the server is listening. */

static u8_string pid_file=NULL, nid_file=NULL;

static void cleanup_state_files()
{
  if (pid_file) u8_removefile(pid_file);
  if (nid_file) u8_removefile(nid_file);
}

/* Core functions */

/* This represents a live client connection and its environment. */
typedef struct FD_CLIENT {
  U8_CLIENT_FIELDS;
  struct FD_DTYPE_STREAM stream;
  time_t lastlive; double elapsed;
  fd_lispenv env;} FD_CLIENT;
typedef struct FD_CLIENT *fd_client;

/* This creates the client structure when called by the server loop. */
static u8_client simply_accept(u8_server srv,u8_socket sock,
			       struct sockaddr *addr,size_t len)
{
  fd_client client=(fd_client)
    u8_client_init(NULL,sizeof(FD_CLIENT),
		   addr,len,sock,srv);
  fd_init_dtype_stream(&(client->stream),sock,4096);
  /* To help debugging, move the client->idstring (libu8)
     into the stream's id (fdb). */
  if (client->stream.id==NULL) {
    if (client->idstring)
      client->stream.id=u8_strdup(client->idstring);
    else client->stream.id=u8_strdup("fdbserver/dtypestream");}
  client->env=fd_make_env(fd_make_hashtable(NULL,16),server_env);
  client->elapsed=0; client->lastlive=((time_t)(-1));
  u8_set_nodelay(sock,1);
  return (u8_client) client;
}

/* This serves a request on a client. If the libu8 server code
   is functioning properly, it will only be called on connections
   with data on them, so there shouldn't be too much waiting. */
static int dtypeserver(u8_client ucl)
{
  fdtype expr, result;
  fd_client client=(fd_client)ucl;
  fd_dtype_stream stream=&(client->stream);
  int async=((async_mode)&&((client->server->flags)&U8_SERVER_ASYNC));
  if ((client->reading>0)&&(u8_client_finished(ucl))) { 
    stream->end=stream->ptr+client->len;
    expr=fd_dtsread_dtype(stream);}
  else if ((client->writing>0)&&(u8_client_finished(ucl))) { 
    /* Just report that the transaction is over. */
    return 0;}
  else if ((client->reading>0)||(client->writing>0))
    /* These should never happen, but let's be complete */
    return 1;
  else if (async) {
    /* See if we can use asynchronous reading */
    fd_dts_start_read(stream);
    if (nobytes((fd_byte_input)stream,1)) expr=FD_EOD;
    else if ((*(stream->ptr))==dt_block) {
      int dtcode=fd_dtsread_byte(stream);
      int nbytes=fd_dtsread_4bytes(stream);
      if (fd_has_bytes(stream,nbytes))
	expr=fd_dtsread_dtype(stream);
      else {
	/* Allocate enough space */
	fd_needs_space((struct FD_BYTE_OUTPUT *)(stream),nbytes);
	/* Set up the client for async input */
	if (u8_client_read(ucl,stream->start,nbytes,stream->end-stream->start))
	  expr=fd_dtsread_dtype(stream);
	else return 1;}}
    else expr=fd_dtsread_dtype(stream);}
  else expr=fd_dtsread_dtype(stream);
  fd_reset_threadvars();
  if (expr == FD_EOD) {
    u8_client_closed(ucl);
    return 0;}
  else if (FD_ABORTP(expr)) {
    u8_log(LOG_ERR,BadRequest,
	   "%s[%d]: Received bad request",
	   client->idstring,client->n_trans);
    fd_clear_errors(1);
    u8_client_close(ucl);
    return -1;}
  else {
    fdtype value;
    int tracethis=((logtrans) &&
		   ((client->n_trans==1) ||
		    (((client->n_trans)%logtrans)==0)));
    int trans_id=client->n_trans, sock=client->socket;
    double xstart=(u8_elapsed_time()), elapsed=-1.0;
    if (logeval)
      u8_log(LOG_INFO,Incoming,"%s[%d/%d]: > %q",
	     client->idstring,sock,trans_id,expr);
    else if (logtrans)
      u8_log(LOG_INFO,Incoming,
	     "%s[%d/%d]: Received request for execution",
	     client->idstring,sock,trans_id);
    value=fd_eval(expr,client->env);
    elapsed=u8_elapsed_time()-xstart;
    if (FD_ABORTP(value)) {
      u8_exception ex=u8_erreify(), root=u8_exception_root(ex);
      fdtype irritant=fd_exception_xdata(root);
      if ((logeval) || (logerrs) || (tracethis)) {
	if ((root->u8x_details) && (!(FD_VOIDP(irritant))))
	  u8_log(LOG_ERR,Outgoing,
		 "%s[%d/%d]: %m@%s (%s) %q returned in %fs",
		 client->idstring,sock,trans_id,
		 root->u8x_cond,root->u8x_context,
		 root->u8x_details,irritant,
		 elapsed);
	else if (root->u8x_details)
	  u8_log(LOG_ERR,Outgoing,
		 "%s[%d/%d]: %m@%s (%s) returned in %fs",
		 client->idstring,sock,trans_id,
		 root->u8x_cond,root->u8x_context,root->u8x_details,elapsed);
      	else if (!(FD_VOIDP(irritant)))
	  u8_log(LOG_ERR,Outgoing,
		 "%s[%d/%d]: %m@%s -- %q returned in %fs",
		 client->idstring,sock,trans_id,
		 root->u8x_cond,root->u8x_context,irritant,elapsed);
	else u8_log(LOG_ERR,Outgoing,
		    "%s[%d/%d]: %m@%s -- %q returned in %fs",
		    client->idstring,sock,trans_id,
		    root->u8x_cond,root->u8x_context,elapsed);
	if (logbacktrace) {
	  struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,1024);
	  out.u8_outptr=out.u8_outbuf; out.u8_outbuf[0]='\0';
	  fd_print_backtrace(&out,ex,120);
	  u8_logger(LOG_ERR,Outgoing,out.u8_outbuf);
	  if ((out.u8_streaminfo)&(U8_STREAM_OWNS_BUF))
	    u8_free(out.u8_outbuf);}}
      value=fd_make_exception
	(ex->u8x_cond,ex->u8x_context,
	 ((ex->u8x_details) ? (u8_strdup(ex->u8x_details)) : (NULL)),
	 fd_incref(irritant));
      u8_free_exception(ex,1);}
    else if (logeval)
      u8_log(LOG_INFO,Outgoing,
	     "%s[%d/%d]: < %q in %f",
	     client->idstring,sock,trans_id,value,elapsed);
    else if (tracethis)
      u8_log(LOG_INFO,Outgoing,"%s[%d/%d]: Request executed in %fs",
	     client->idstring,sock,trans_id,elapsed);
    client->elapsed=client->elapsed+elapsed;
    /* Currently, fd_dtswrite_dtype writes the whole thing at once,
       so we just use that. */
    fd_dts_start_write(stream);
    stream->ptr=stream->start;
    if (fd_use_dtblock) {
      int nbytes; unsigned char *ptr;
      fd_dtswrite_byte(stream,dt_block);
      fd_dtswrite_4bytes(stream,0);
      nbytes=fd_dtswrite_dtype(stream,value);
      ptr=stream->ptr; {
	/* Rewind temporarily to write the length information */
	stream->ptr=stream->start+1;
	fd_dtswrite_4bytes(stream,nbytes);
	stream->ptr=ptr;}}
    else fd_dtswrite_dtype(stream,value);
    if (async) {
      u8_client_write(ucl,stream->start,stream->ptr-stream->start,0);
      return 1;}
    else {
      fd_dtswrite_dtype(stream,value);
      fd_dtsflush(stream);}
    time(&(client->lastlive));
    if (tracethis)
      u8_log(LOG_INFO,Outgoing,"%s[%d/%d]: Response sent after %fs",
	     client->idstring,sock,trans_id,u8_elapsed_time()-xstart);
    fd_decref(expr); fd_decref(value);
    fd_swapcheck();
    return 0;}
}

static int close_fdclient(u8_client ucl)
{
  fd_client client=(fd_client)ucl;
  fd_dtsclose(&(client->stream),2);
  fd_decref((fdtype)((fd_client)ucl)->env);
  ucl->socket=-1;
  return 1;
}

/* Module configuration */

/* A list of exposed modules */
static fdtype module_list=FD_EMPTY_LIST;
/* This is the exposed environment. */
static fd_lispenv exposed_environment=NULL;
/* This is the shutdown procedure to be called when the
   server shutdowns. */
static fdtype shutdown_proc=FD_EMPTY_CHOICE;
static int normal_exit=0;

static fdtype config_get_modules(fdtype var,void *data)
{
  return fd_incref(module_list);
}
static int config_use_module(fdtype var,fdtype val,void *data)
{
  fdtype safe_module=fd_find_module(val,1,1), module=safe_module;
  if (FD_VOIDP(module)) {}
  else if (FD_HASHTABLEP(module)) 
    exposed_environment=
      fd_make_env(fd_incref(module),exposed_environment);
  else if (FD_PTR_TYPEP(module,fd_environment_type)) {
    FD_ENVIRONMENT *env=
      FD_GET_CONS(module,fd_environment_type,FD_ENVIRONMENT *);
    if (FD_HASHTABLEP(env->exports))
      exposed_environment=
	fd_make_env(fd_incref(env->exports),exposed_environment);}
  module=fd_find_module(val,0,1);
  if (FD_EQ(module,safe_module))
    if (FD_VOIDP(module)) return 0;
    else return 1;
  else if (FD_HASHTABLEP(module)) 
    exposed_environment=
      fd_make_env(fd_incref(module),exposed_environment);
  else if (FD_PTR_TYPEP(module,fd_environment_type)) {
    FD_ENVIRONMENT *env=
      FD_GET_CONS(module,fd_environment_type,FD_ENVIRONMENT *);
    if (FD_HASHTABLEP(env->exports))
      exposed_environment=
	fd_make_env(fd_incref(env->exports),exposed_environment);}
  module_list=fd_init_pair(NULL,fd_incref(val),module_list);
  return 1;
}

/* Handling signals, etc. */

static void shutdown_dtypeserver_onsignal(int sig)
{
  u8_log(LOG_CRIT,ServerShutdown,"Shutting down server on signal %d",sig);
  u8_server_shutdown(&dtype_server,shutdown_grace);
  if (FD_APPLICABLEP(shutdown_proc)) {
    fdtype sigval=FD_INT2DTYPE(sig), value;
    u8_log(LOG_WARNING,ServerShutdown,"Calling shutdown procedure %q",
	   shutdown_proc);
    value=fd_apply(shutdown_proc,1,&sigval);
    fd_decref(value);}
  cleanup_state_files();
  u8_log(LOG_CRIT,ServerShutdown,"Done shutting down server");
}

static void shutdown_dtypeserver_onexit()
{
  u8_log(LOG_CRIT,ServerShutdown,"Shutting down server on exit");
  u8_server_shutdown(&dtype_server,shutdown_grace);
  if (FD_APPLICABLEP(shutdown_proc)) {
    fdtype shutval, value;
    if (normal_exit) shutval=FD_FALSE; else shutval=FD_TRUE;
    u8_log(LOG_WARN,ServerShutdown,"Calling shutdown procedure %q",
	   shutdown_proc);
    value=fd_apply(shutdown_proc,1,&shutval);
    fd_decref(value);}
  cleanup_state_files();
  u8_log(LOG_WARN,ServerShutdown,"Done shutting down server");
}

/* Miscellaneous Server functions */

static fdtype get_boot_time()
{
  return fd_make_timestamp(&boot_time);
}

static fdtype get_uptime()
{
  struct U8_XTIME now; u8_now(&now);
  return fd_init_double(NULL,u8_xtime_diff(&now,&boot_time));
}

static fdtype get_server_status()
{
  fdtype result=fd_init_slotmap(NULL,0,NULL);
  struct U8_SERVER_STATS stats, livestats, curstats;

  fd_store(result,fd_intern("NTHREADS"),FD_INT2DTYPE(dtype_server.n_threads));
  fd_store(result,fd_intern("NQUEUED"),FD_INT2DTYPE(dtype_server.n_queued));
  fd_store(result,fd_intern("NBUSY"),FD_INT2DTYPE(dtype_server.n_busy));
  fd_store(result,fd_intern("NCLIENTS"),FD_INT2DTYPE(dtype_server.n_clients));
  fd_store(result,fd_intern("TOTALTRANS"),FD_INT2DTYPE(dtype_server.n_trans));
  fd_store(result,fd_intern("TOTALCONN"),FD_INT2DTYPE(dtype_server.n_accepted));

  u8_server_statistics(&dtype_server,&stats);
  u8_server_livestats(&dtype_server,&livestats);
  u8_server_curstats(&dtype_server,&curstats);

  fd_store(result,fd_intern("NACTIVE"),FD_INT2DTYPE(stats.n_active));
  fd_store(result,fd_intern("NREADING"),FD_INT2DTYPE(stats.n_reading));
  fd_store(result,fd_intern("NWRITING"),FD_INT2DTYPE(stats.n_writing));
  fd_store(result,fd_intern("NXBUSY"),FD_INT2DTYPE(stats.n_busy));

  if (stats.tcount>0) {
    fd_store(result,fd_intern("TRANSAVG"),
	     fd_make_double(((double)stats.tsum)/
			    (((double)stats.tcount))));
    fd_store(result,fd_intern("TRANSMAX"),FD_INT2DTYPE(stats.tmax));
    fd_store(result,fd_intern("TRANSCOUNT"),FD_INT2DTYPE(stats.tcount));}

  if (stats.qcount>0) {
    fd_store(result,fd_intern("QUEUEAVG"),
	     fd_make_double(((double)stats.qsum)/
			    (((double)stats.qcount))));
    fd_store(result,fd_intern("QUEUEMAX"),FD_INT2DTYPE(stats.qmax));
    fd_store(result,fd_intern("QUEUECOUNT"),FD_INT2DTYPE(stats.qcount));}

  if (stats.rcount>0) {
    fd_store(result,fd_intern("READAVG"),
	     fd_make_double(((double)stats.rsum)/
			    (((double)stats.rcount))));
    fd_store(result,fd_intern("READMAX"),FD_INT2DTYPE(stats.rmax));
    fd_store(result,fd_intern("READCOUNT"),FD_INT2DTYPE(stats.rcount));}
  
  if (stats.wcount>0) {
    fd_store(result,fd_intern("WRITEAVG"),
	     fd_make_double(((double)stats.wsum)/
			    (((double)stats.wcount))));
    fd_store(result,fd_intern("WRITEMAX"),FD_INT2DTYPE(stats.wmax));
    fd_store(result,fd_intern("WRITECOUNT"),FD_INT2DTYPE(stats.wcount));}
  
  if (stats.xcount>0) {
    fd_store(result,fd_intern("EXECAVG"),
	     fd_make_double(((double)stats.xsum)/
			    (((double)stats.xcount))));
    fd_store(result,fd_intern("EXECMAX"),FD_INT2DTYPE(stats.xmax));
    fd_store(result,fd_intern("EXECCOUNT"),FD_INT2DTYPE(stats.xcount));}

  if (livestats.tcount>0) {
    fd_store(result,fd_intern("LIVE/TRANSAVG"),
	     fd_make_double(((double)livestats.tsum)/
			    (((double)livestats.tcount))));
    fd_store(result,fd_intern("LIVE/TRANSMAX"),FD_INT2DTYPE(livestats.tmax));
    fd_store(result,fd_intern("LIVE/TRANSCOUNT"),
	     FD_INT2DTYPE(livestats.tcount));}

  if (livestats.qcount>0) {
    fd_store(result,fd_intern("LIVE/QUEUEAVG"),
	     fd_make_double(((double)livestats.qsum)/
			    (((double)livestats.qcount))));
    fd_store(result,fd_intern("LIVE/QUEUEMAX"),FD_INT2DTYPE(livestats.qmax));
    fd_store(result,fd_intern("LIVE/QUEUECOUNT"),
	     FD_INT2DTYPE(livestats.qcount));}

  if (livestats.rcount>0) {
    fd_store(result,fd_intern("LIVE/READAVG"),
	     fd_make_double(((double)livestats.rsum)/
			    (((double)livestats.rcount))));
    fd_store(result,fd_intern("LIVE/READMAX"),FD_INT2DTYPE(livestats.rmax));
    fd_store(result,fd_intern("LIVE/READCOUNT"),
	     FD_INT2DTYPE(livestats.rcount));}
  
  if (livestats.wcount>0) {
    fd_store(result,fd_intern("LIVE/WRITEAVG"),
	     fd_make_double(((double)livestats.wsum)/
			    (((double)livestats.wcount))));
    fd_store(result,fd_intern("LIVE/WRITEMAX"),FD_INT2DTYPE(livestats.wmax));
    fd_store(result,fd_intern("LIVE/WRITECOUNT"),
	     FD_INT2DTYPE(livestats.wcount));}
  
  if (livestats.xcount>0) {
    fd_store(result,fd_intern("LIVE/EXECAVG"),
	     fd_make_double(((double)livestats.xsum)/
			    (((double)livestats.xcount))));
    fd_store(result,fd_intern("LIVE/EXECMAX"),FD_INT2DTYPE(livestats.xmax));
    fd_store(result,fd_intern("LIVE/EXECCOUNT"),
	     FD_INT2DTYPE(livestats.xcount));}

    if (curstats.tcount>0) {
    fd_store(result,fd_intern("CUR/TRANSAVG"),
	     fd_make_double(((double)curstats.tsum)/
			    (((double)curstats.tcount))));
    fd_store(result,fd_intern("CUR/TRANSMAX"),FD_INT2DTYPE(curstats.tmax));
    fd_store(result,fd_intern("CUR/TRANSCOUNT"),
	     FD_INT2DTYPE(curstats.tcount));}

  if (curstats.qcount>0) {
    fd_store(result,fd_intern("CUR/QUEUEAVG"),
	     fd_make_double(((double)curstats.qsum)/
			    (((double)curstats.qcount))));
    fd_store(result,fd_intern("CUR/QUEUEMAX"),FD_INT2DTYPE(curstats.qmax));
    fd_store(result,fd_intern("CUR/QUEUECOUNT"),
	     FD_INT2DTYPE(curstats.qcount));}

  if (curstats.rcount>0) {
    fd_store(result,fd_intern("CUR/READAVG"),
	     fd_make_double(((double)curstats.rsum)/
			    (((double)curstats.rcount))));
    fd_store(result,fd_intern("CUR/READMAX"),FD_INT2DTYPE(curstats.rmax));
    fd_store(result,fd_intern("CUR/READCOUNT"),
	     FD_INT2DTYPE(curstats.rcount));}
  
  if (curstats.wcount>0) {
    fd_store(result,fd_intern("CUR/WRITEAVG"),
	     fd_make_double(((double)curstats.wsum)/
			    (((double)curstats.wcount))));
    fd_store(result,fd_intern("CUR/WRITEMAX"),FD_INT2DTYPE(curstats.wmax));
    fd_store(result,fd_intern("CUR/WRITECOUNT"),
	     FD_INT2DTYPE(curstats.wcount));}
  
  if (curstats.xcount>0) {
    fd_store(result,fd_intern("CUR/EXECAVG"),
	     fd_make_double(((double)curstats.xsum)/
			    (((double)curstats.xcount))));
    fd_store(result,fd_intern("CUR/EXECMAX"),FD_INT2DTYPE(curstats.xmax));
    fd_store(result,fd_intern("CUR/EXECCOUNT"),
	     FD_INT2DTYPE(curstats.xcount));}

  return result;
}

static fdtype asyncok()
{
  if ((async_mode)&&(dtype_server.flags&U8_SERVER_ASYNC))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype boundp_handler(fdtype expr,fd_lispenv env)
{
  fdtype symbol=fd_get_arg(expr,1);
  if (!(FD_SYMBOLP(symbol)))
    return fd_err(fd_SyntaxError,"boundp_handler",NULL,fd_incref(expr));
  else {
    fdtype val=fd_symeval(symbol,env);
    if (FD_VOIDP(val)) return FD_FALSE;
    else if (val == FD_UNBOUND) return FD_FALSE;
    else {
      fd_decref(val); return FD_TRUE;}}
}

/* State dir */

static u8_string state_dir=NULL;

/* The main() event */

static void init_server()
{
  fd_lock_mutex(&init_server_lock);
  if (server_initialized) return;
  server_initialized=1;
  u8_init_server
    (&dtype_server,
     simply_accept, /* acceptfn */
     dtypeserver, /* handlefn */
     NULL, /* donefn */
     close_fdclient, /* closefn */
     U8_SERVER_INIT_CLIENTS,init_clients,
     U8_SERVER_NTHREADS,n_threads,
     U8_SERVER_BACKLOG,max_backlog,
     U8_SERVER_MAX_QUEUE,max_queue,
     U8_SERVER_MAX_CLIENTS,max_conn,
     U8_SERVER_FLAGS,server_flags,
     U8_SERVER_END_INIT); 
  fd_unlock_mutex(&init_server_lock);
}

FD_EXPORT int fd_init_fddbserv(void);

int main(int argc,char **argv)
{
  int fd_version;
  int i=1; u8_string source_file=NULL;
  /* This is the base of the environment used to be passed to the server.
     It is augmented by the fdbserv module, all of the modules declared by
     MODULE= configurations, and either the exports or the definitions of
     the server control file from the command line.
     It starts out built on the default safe environment, but loses that if
     fullscheme is zero after configuration and file loading.  fullscheme can be
     set by the FULLSCHEME configuration parameter. */
  fd_lispenv core_env; 

  fddb_loglevel=LOG_INFO;

  fd_version=fd_init_fdscheme();

  if (fd_version<0) {
    fprintf(stderr,"Can't initialize FramerD libraries\n");
    return -1;}
  
  /* Record the startup time for UPTIME */
  u8_now(&boot_time);

  /* INITIALIZING MODULES */
  /* Normally, modules have initialization functions called when
     dynamically loaded.  However, if we are statically linked, or we
     don't have the "constructor attributes" use to declare init functions,
     we need to call some initializers explicitly. */

  /* Initialize the libu8 stdio library if it won't happen automatically. */
#if (!(HAVE_CONSTRUCTOR_ATTRIBUTES))
  u8_initialize_u8stdio();
  u8_init_chardata_c();
#endif

  /* Show more information */
  u8_log_show_date=1;
  u8_log_show_procinfo=1;
  u8_use_syslog(1);

#if ((!(HAVE_CONSTRUCTOR_ATTRIBUTES)) || (FD_TESTCONFIG))
  fd_init_fdscheme();
  fd_init_schemeio();
  fd_init_texttools();
  fd_init_tagger();
#else
  FD_INIT_SCHEME_BUILTINS();
#endif

  fd_init_fddbserv();

  fd_register_module("FDBSERV",fd_incref(fd_fdbserv_module),FD_MODULE_SAFE);
  fd_finish_module(fd_fdbserv_module);
  fd_persist_module(fd_fdbserv_module);

  fd_register_config("BACKLOG",
		     _("Number of pending connection requests allowed"),
		     fd_intconfig_get,fd_intconfig_set,&max_backlog);
  fd_register_config("MAXQUEUE",_("Max number of requests to keep queued"),
		     fd_intconfig_get,fd_intconfig_set,&max_queue);
  fd_register_config("INITCLIENTS",
		     _("Number of clients to prepare for/grow by"),
		     fd_intconfig_get,fd_intconfig_set,&init_clients);
  fd_register_config("NTHREADS",_("Number of threads in the thread pool"),
		     fd_intconfig_get,fd_intconfig_set,&n_threads);
  fd_register_config("PORT",_("port or port@host to listen on"),
		     config_get_ports,config_serve_port,NULL);
  fd_register_config("MODULE",_("modules to provide in the server environment"),
		     config_get_modules,config_use_module,NULL);
  fd_register_config("FULLSCHEME",_("whether to provide full scheme interpretation to client"), 
		     config_get_fullscheme,config_set_fullscheme,NULL);
  fd_register_config("LOGEVAL",_("Whether to log each request and response"),
		     fd_boolconfig_get,fd_boolconfig_set,&logeval);
  fd_register_config("LOGTRANS",_("Whether to log each transaction"),
		     fd_intconfig_get,fd_boolconfig_set,&logtrans);
  fd_register_config("LOGERRS",
		     _("Whether to log errors returned by the server to clients"),
		     fd_boolconfig_get,fd_boolconfig_set,&logerrs);
  fd_register_config("LOGBACKTRACE",
		     _("Whether to include a detailed backtrace when logging errors"),
		     fd_boolconfig_get,fd_boolconfig_set,&logbacktrace);
  fd_register_config("U8LOGCONNECT",
		     _("Whether to have libu8 log each connection"),
		     config_get_dtype_server_flag,config_set_dtype_server_flag,
		     (void *)(U8_SERVER_LOG_CONNECT));
  fd_register_config("U8LOGTRANSACT",
		     _("Whether to have libu8 log each transaction"),
		     config_get_dtype_server_flag,config_set_dtype_server_flag,
		     (void *)(U8_SERVER_LOG_TRANSACT));
#ifdef U8_SERVER_LOG_TRANSFERS
  fd_register_config("U8LOGTRANSFER",
		     _("Whether to have libu8 log all data transfers for fine-grained debugging"),
		     config_get_dtype_server_flag,config_set_dtype_server_flag,
		     (void *)(U8_SERVER_LOG_TRANSFERS));
#endif
  fd_register_config("U8ASYNC",
		     _("Whether to support thread-asynchronous transactions"),
		     config_get_dtype_server_flag,config_set_dtype_server_flag,
		     (void *)(U8_SERVER_ASYNC));

  fd_register_config("DEBUGMAXCHARS",
		     _("Max number of string characters to display in debug message"),
		     fd_intconfig_get,fd_intconfig_set,
		     &debug_maxchars);
  fd_register_config("DEBUGMAXELTS",
		     _("Max number of list/vector/choice elements to display in debug message"),
		     fd_intconfig_get,fd_intconfig_set,
		     &debug_maxelts);
  fd_register_config("STATEDIR",_("Where to write server pid/nid files"),
		     fd_sconfig_get,fd_sconfig_set,&state_dir);
  fd_register_config("ASYNCMODE",_("Whether to run in asynchronous mode"),
		     fd_boolconfig_get,fd_boolconfig_set,&async_mode);
  fd_register_config("GRACEFULDEATH",
		     _("How long (μs) to wait for tasks during shutdown"),
		     fd_intconfig_get,fd_intconfig_set,&shutdown_grace);

  /* Prepare for the end */
  atexit(shutdown_dtypeserver_onexit);
#ifdef SIGTERM
  signal(SIGTERM,shutdown_dtypeserver_onsignal);
#endif
#ifdef SIGQUIT
  signal(SIGQUIT,shutdown_dtypeserver_onsignal);
#endif

  /* This is a safe environment (e.g. a sandbox without file/io etc). */
  core_env=fd_safe_working_environment();

  /* We add some special functions */
  fd_defspecial((fdtype)core_env,"BOUND?",boundp_handler);
  fd_idefn((fdtype)core_env,fd_make_cprim0("BOOT-TIME",get_boot_time,0));
  fd_idefn((fdtype)core_env,fd_make_cprim0("UPTIME",get_uptime,0));
  fd_idefn((fdtype)core_env,fd_make_cprim0("ASYNCOK?",asyncok,0));
  fd_idefn((fdtype)core_env,
	   fd_make_cprim0("SERVER-STATUS",get_server_status,0));

  /* And create the exposed environment */
  exposed_environment=fd_make_env(fd_incref(fd_fdbserv_module),core_env);
  
  /* Now process all the configuration arguments and find the source file */
  while (i<argc)
    if (strchr(argv[i],'=')) 
      fd_config_assignment(argv[i++]);
    else if (source_file) i++;
    else {
      source_file=u8_fromlibc(argv[i++]);
      u8_default_appid(source_file);}

  if (source_file) {
    /* The source file is loaded into a full (non sandbox environment).
       It's exports are then exposed through the server. */
    fd_lispenv env=fd_working_environment();
    fdtype result=fd_load_source(source_file,env,NULL);
    if (FD_TROUBLEP(result)) {
      u8_exception e=u8_erreify();
      U8_OUTPUT out; U8_INIT_OUTPUT(&out,512);
      fd_print_exception(&out,e);
      fd_print_backtrace(&out,e,80);
      fputs(out.u8_outbuf,stderr);
      u8_free(out.u8_outbuf);
      u8_free(source_file);
      fd_decref(result);
      fd_decref((fdtype)env);
      return -1;}
    else {fd_decref(result); result=FD_VOID;}

    /* Store server initialization information in the configuration
       environment. */
    {
      fdtype interp=fd_lispstring(u8_fromlibc(argv[0]));
      fdtype src=fd_lispstring(u8_realpath(source_file,NULL));
      fd_config_set("INTERPRETER",interp);
      fd_config_set("SOURCE",src);
      fd_decref(interp); fd_decref(src);}

    /* If the init file did any exporting, expose those exports to clients.
       Otherwise, expose all the definitions in the init file.  Note that the clients
       won't be able to get at the unsafe "empowered" environment but that the
       procedures defined are closed in that environment. */
    if (FD_HASHTABLEP(env->exports))
      server_env=fd_make_env(fd_incref(env->exports),exposed_environment);
    else server_env=fd_make_env(fd_incref(env->bindings),exposed_environment);

    /* Handle server startup and shutdown procedures defined in the environment. */
    {
      fdtype startup_proc=fd_symeval(fd_intern("STARTUP"),env);
      shutdown_proc=fd_symeval(fd_intern("SHUTDOWN"),env);
      if (FD_VOIDP(startup_proc)) {}
      else {
	FD_DO_CHOICES(p,startup_proc) {
	  fdtype result=fd_apply(p,0,NULL);
	  if (FD_ABORTP(result)) {
	    u8_exception ex=u8_erreify(), root=ex;
	    int old_maxelts=fd_unparse_maxelts, old_maxchars=fd_unparse_maxchars;
	    U8_OUTPUT out; U8_INIT_OUTPUT(&out,512);
	    while (root->u8x_prev) root=root->u8x_prev;
	    fd_unparse_maxchars=debug_maxchars;
	    fd_unparse_maxelts=debug_maxelts;
	    fd_print_exception(&out,root);
	    fd_print_backtrace(&out,ex,80);
	    fd_unparse_maxelts=old_maxelts; fd_unparse_maxchars=old_maxchars;
	    fputs(out.u8_outbuf,stderr);
	    u8_free(out.u8_outbuf);
	    u8_free_exception(ex,1);
	    exit(fd_interr(result));}
	  else fd_decref(result);}}}

    /* You're done loading the file now. */
    fd_decref((fdtype)env);

    /* Now, write the state files.  */
    {
      u8_string bname=u8_basename(source_file,".fdz");
      u8_string fullname=u8_abspath(bname,NULL);
      u8_string appid=u8_basename(u8_appid(),"*");
      FILE *f;
      /* Get state files and write info */
      if ((state_dir) && (appid)) {
	u8_string pid_name=u8_mkstring("%s.pid",appid);
	u8_string nid_name=u8_mkstring("%s.nid",appid);
	pid_file=u8_mkpath(state_dir,pid_name);
	nid_file=u8_mkpath(state_dir,nid_name);
	u8_free(pid_name); u8_free(nid_name);}
      else {	
	pid_file=u8_string_append(fullname,".pid",NULL);
	nid_file=u8_string_append(fullname,".nid",NULL);}
      atexit(cleanup_state_files);
      /* Write the PID file */
      f=u8_fopen(pid_file,"w");
      if (f) {
	fprintf(f,"%d\n",getpid());
	fclose(f);}
      else {
	u8_log(LOG_WARN,u8_strerror(errno),
		"Couldn't write PID %d to '%s'",
	       getpid(),pid_file);
	errno=0;}
      /* Write the NID file */
      f=u8_fopen(nid_file,"w");
      if (f) {
	if (dtype_server.n_servers) {
	  int i=0; while (i<dtype_server.n_servers) {
	    fprintf(f,"%s\n",dtype_server.server_info[i].idstring);
	    i++;}}
	else fprintf(f,"temp.socket\n");
	fclose(f);}
      else {
	u8_log(LOG_WARN,u8_strerror(errno),
	       "Couldn't write NID info to '%s'",
	       getpid(),pid_file);
	if (dtype_server.n_servers) {
	  int i=0; while (i<dtype_server.n_servers) {
	    u8_log(LOG_NOTICE,ServerStartup,"%s\n",
		   dtype_server.server_info[i].idstring);
	    i++;}}
	else u8_log(LOG_NOTICE,ServerStartup,"temp.socket\n");
	errno=0;}
      u8_free(fullname); u8_free(bname);}
    u8_free(source_file);
    source_file=NULL;}
  else {
    fprintf(stderr,
	    "Usage: fdbserver [conf=val]* source_file [conf=val]*\n");
    return 1;}
  if (fullscheme==0) {
    fd_decref((fdtype)(core_env->parent)); core_env->parent=NULL;}
  if (n_ports>0) {
    u8_log(LOG_INFO,NULL,
	   "FramerD (%s) fdbserver running, %d/%d pools/indices",
	   FRAMERD_REV,fd_n_pools,
	   fd_n_primary_indices+fd_n_secondary_indices);
    u8_message
      ("beingmeta FramerD, (C) beingmeta 2004-2013, all rights reserved");
    u8_log(LOG_NOTICE,ServerStartup,"Serving on %d sockets",n_ports);
    u8_server_loop(&dtype_server); normal_exit=1;
    u8_log(LOG_NOTICE,ServerShutdown,"Exited server loop",n_ports);
    exit(0);
    return 0;}
  else if (n_ports==0) {
    u8_log(LOG_WARN,NoServers,"No servers configured, exiting..");
    exit(-1);
    return -1;}
  else {
    fd_clear_errors(1);
    shutdown_dtypeserver_onexit();
    fd_clear_errors(1);
    exit(-1);
    return -1;}
}

