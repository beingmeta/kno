/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

static int knod_loglevel;
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

#include "main.h"

KNO_EXPORT int kno_update_file_modules(int force);

#include "main.c"

static int knod_loglevel = LOG_NOTIFY;

static int daemonize = 0, foreground = 0, pidwait = 1;

static long long state_files_written = 0;

#define nobytes(in,nbytes) (PRED_FALSE(!(kno_request_bytes(in,nbytes))))
#define havebytes(in,nbytes) (PRED_TRUE(kno_request_bytes(in,nbytes)))

static int async_mode = 1;
static int auto_reload = 0;
static int server_shutdown = 0;
static int stealsockets = 0;

static int debug_maxelts = 32, debug_maxchars = 80;

static void shutdown_onsignal(int sig,siginfo_t *info,void *data);

/* Various exceptions */
static u8_condition BadPortSpec=_("Bad port spec");
static u8_condition BadRequest=_("Bad client request");
static u8_condition NoServers=_("NoServers");
static u8_condition Startup=_("Startup");
static u8_condition ServerAbort=_("Knodaemon/ABORT");
static u8_condition ServerConfig=_("Knodaemon/CONFIG");
static u8_condition ServerStartup=_("Knodaemon/STARTUP");
static u8_condition ServerShutdown=_("Knodaemon/SHUTDOWN");

static const sigset_t *server_sigmask;

static u8_condition Incoming=_("Incoming"), Outgoing=_("Outgoing");

static u8_string pid_file = NULL, nid_file = NULL, cmd_file = NULL, inject_file = NULL;

/* This is the global lisp environment for all servers.
   It is modified to include various modules, including dbserv. */
static kno_lexenv working_env = NULL, server_env = NULL;

/* This is the server struct used to establish the server. */
static struct U8_SERVER dtype_server;
static int server_flags=
  U8_SERVER_LOG_LISTEN|U8_SERVER_LOG_CONNECT|U8_SERVER_ASYNC;

/* When the server started, used by UPTIME */
static struct U8_XTIME boot_time;

/*  Total number of queued requests, served threads, etc. */
static int max_queue = 128, init_clients = 32, n_threads = 4, server_initialized = 0;
/* This is the backlog of connection requests not transactions.
   It is passed as the argument to listen() */
static int max_backlog = -1;
/* This is the maximum number of concurrent connections allowed.
   Note that this currently is handled by libu8. */
static int max_conn = 0;
/* This is how long to wait for clients to finish when shutting down the
   server.  Note that the server stops listening for new connections right
   away, so we can start another server.  */
static int shutdown_grace = 30000000; /* 30 seconds */
/* Controlling trace activity: logeval prints expressions, logtrans reports
   transactions (request/response pairs). */
static int logeval = 0, logerrs = 0, logtrans = 0, logbacktrace = 0;
static int backtrace_width = KNO_BACKTRACE_WIDTH;

static int no_storage_api = 0;

static u8_mutex init_server_lock;

static void init_server(void);
static void write_state_files(void);
static int run_server(u8_string server_spec);
static int init_server_env(u8_string server_spec,kno_lexenv core_env);

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

/* Checking for (good) injections */

static int check_for_injection()
{
  if (working_env == NULL) return 0;
  else if (inject_file == NULL) return 0;
  else if (u8_file_existsp(inject_file)) {
    u8_string temp_file = u8_string_append(inject_file,".loading",NULL);
    int rv = u8_movefile(inject_file,temp_file);
    if (rv<0) {
      u8_log(LOG_WARN,"Knodaemon/InjectionIgnored",
             "Can't stage injection file %s to %s",
             inject_file,temp_file);
      kno_clear_errors(1);
      u8_free(temp_file);
      return 0;}
    else {
      u8_string content = u8_filestring(temp_file,NULL);
      if (content == NULL)  {
        u8_log(LOG_WARN,"Knodaemon/InjectionCantRead",
               "Can't read %s",temp_file);
        kno_clear_errors(1);
        u8_free(temp_file);
        return -1;}
      else {
        lispval result;
        u8_log(LOG_WARN,"Knodaemon/InjectLoad",
               "From %s\n\"%s\"",temp_file,content);
        result = kno_load_source(temp_file,working_env,NULL);
        if (KNO_ABORTP(result)) {
          u8_exception ex = u8_current_exception;
          if (!(ex)) {
            u8_log(LOG_CRIT,"Knodaemon/InjectError",
                   "Unknown error processing injection from %s: \"%s\"",
                   inject_file,content);}
          else if ((ex->u8x_context!=NULL)&&
                   (ex->u8x_details!=NULL))
            u8_log(LOG_CRIT,"Knodaemon/InjectionError",
                   "Error %s (%s) processing injection %s: %s\n\"%s\"",
                   ex->u8x_cond,ex->u8x_context,inject_file,
                   ex->u8x_details,content);
          else if (ex->u8x_context!=NULL)
            u8_log(LOG_CRIT,"Knodaemon/InjectionError",
                   "Error %s (%s) processing injection %s\n\"%s\"",
                   ex->u8x_cond,ex->u8x_context,inject_file,content);
          else u8_log(LOG_CRIT,"Knodaemon/InjectionError",
                      "Error %s processing injection %s\n\"%s\"",
                      ex->u8x_cond,inject_file,content);
          kno_clear_errors(1);
          return -1;}
        else {
          u8_log(LOG_WARN,"Knodaemon/InjectionDone",
                 "Finished from %s",inject_file);}
        rv = u8_removefile(temp_file);
        if (rv<0) {
          u8_log(LOG_CRIT,"Knodaemon/InjectionCleanup",
                 "Error removing %s",temp_file);
          kno_clear_errors(1);}
        kno_decref(result);
        u8_free(content);
        u8_free(temp_file);
        return 1;}}}
  else return 0;
}

static int server_loopfn(struct U8_SERVER *server)
{
  check_for_injection();
  return 1;
}

/* Log files */

u8_condition LogFileError=_("Log file error");

static u8_string log_filename = NULL;
static int log_fd = -1;
static int set_logfile(u8_string logfile,int exitonfail)
{
  int logsync = ((getenv("LOGSYNC") == NULL)?(0):(O_SYNC));
  if (!(logfile)) {
    u8_log(LOG_WARN,ServerConfig,"Can't specify NULL log file");
    if (exitonfail) exit(1);
    u8_seterr(LogFileError,"set_logfile",u8_strdup("Can't set NULL log file"));
    return -1;}
  int new_fd = open(logfile,O_RDWR|O_APPEND|O_CREAT|logsync,0644);
  if (new_fd<0) {
    u8_log(LOG_WARN,ServerConfig,"Couldn't open log file %s",logfile);
    if (exitonfail) exit(1);
    u8_seterr(LogFileError,"set_logfile",u8_strdup(logfile));
    return -1;}
  else if ((log_filename)&&((strcmp(logfile,log_filename))!=0))
    u8_log(LOG_WARN,ServerConfig,"Changing log file to %s from %s",
           logfile,log_filename);
  else u8_log(LOG_WARN,ServerConfig,"Using log file %s",logfile);
  dup2(new_fd,1);
  dup2(new_fd,2);
  if (log_fd>=0) close(log_fd);
  log_fd = new_fd;
  if (log_filename) u8_free(log_filename);
  log_filename = u8_strdup(logfile);
  return 1;
}

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

static int n_ports = 0;

static int config_serve_port(lispval var,lispval val,void U8_MAYBE_UNUSED *data)
{
  if (server_initialized==0) init_server();
  if (n_ports<0)
    return -1;
  else if (KNO_UINTP(val)) {
    int retval = u8_add_server(&dtype_server,NULL,FIX2INT(val));
    if (retval>0)
      n_ports = n_ports+retval;
    else if (retval<0)
      return KNO_ERR(retval,BadPortSpec,"config_serve_port",NULL,val);
    else {
      u8_log(LOG_WARN,"NoServers","No servers were added for port #%q",val);
      return 0;}
    return retval;}
  else if (STRINGP(val)) {
    int rv = 0;
    u8_string port = CSTRING(val);
    if ((strchr(port,'/')) && (u8_file_existsp(port))) {
      if (! (u8_socketp(port)) ) {
        u8_log(LOG_CRIT,"Knolet/start",
               "File %s exists and is not a socket",port);
        rv = -1;}
      else if (stealsockets) {
        rv = u8_removefile(port);
        if (rv<0)
          u8_log(LOG_CRIT,"Knolet/start",
                 "Couldn't remove existing socket file %s",port);}
      else {
        u8_log(LOG_WARN,"Knolet/start",
               "Socket file %s already exists",port);
        rv=-1;}}
    else rv=0;
    if (rv < 0) {
      u8_seterr("InvalidFileSocket","config_server_port",u8_strdup(port));
      return rv;}
    rv = u8_add_server(&dtype_server,CSTRING(val),0);
    if (rv>0)
      n_ports = n_ports+rv;
    else if (rv<0)
      return KNO_ERR(rv,BadPortSpec,"config_serve_port",NULL,val);
    else {
      u8_log(LOG_WARN,"NoServers","No servers were added for port %q",val);
      return 0;}
    return rv;}
  else return KNO_ERR(-1,BadPortSpec,"config_serve_port",NULL,val);
}

static lispval config_get_ports(lispval var,void U8_MAYBE_UNUSED *data)
{
  lispval results = KNO_EMPTY;
  int i = 0, lim = dtype_server.n_servers;
  while (i<lim) {
    lispval id = knostring(dtype_server.server_info[i].idstring);
    CHOICE_ADD(results,id); i++;}
  return results;
}

/* Configuring dtype_server fields */

static lispval config_get_dtype_server_flag(lispval var,void *data)
{
  kno_ptrbits bigmask = (kno_ptrbits)data;
  unsigned int mask = (unsigned int)(bigmask&0xFFFFFFFF), flags;
  u8_lock_mutex(&init_server_lock);
  if (server_initialized) flags = dtype_server.flags;
  else flags = server_flags;
  u8_unlock_mutex(&init_server_lock);
  if ((flags)&(mask))
    return KNO_TRUE;
  else return KNO_FALSE;
}

static int config_set_dtype_server_flag(lispval var,lispval val,void *data)
{
  kno_ptrbits bigmask = (kno_ptrbits)data;
  unsigned int mask = (bigmask&0xFFFFFFFF), *flagsp, flags;
  u8_lock_mutex(&init_server_lock);
  if (server_initialized) {
    flags = dtype_server.flags; flagsp = &(dtype_server.flags);}
  else {
    flags = server_flags; flagsp = &(server_flags);}
  if (FALSEP(val))
    *flagsp = flags&(~(mask));
  else if ((STRINGP(val))&&(STRLEN(val)==0))
    *flagsp = flags&(~(mask));
  else if (STRINGP(val)) {
    u8_string s = CSTRING(val);
    int bool = kno_boolstring(CSTRING(val),-1);
    if (bool<0) {
      int guess = (((s[0]=='y')||(s[0]=='Y'))?(1):
                   ((s[0]=='N')||(s[0]=='n'))?(0):
                   (-1));
      if (guess<0) {
        u8_log(LOG_WARN,ServerConfig,"Unknown boolean setting %s for %q",s,var);
        u8_unlock_mutex(&init_server_lock);
        return kno_reterr(kno_TypeError,"setserverflag","boolean value",val);}
      else u8_log(LOG_WARN,ServerConfig,
                  "Unfamiliar boolean setting %s for %q, assuming %s",
                  s,var,((guess)?("true"):("false")));
      if (!(guess<0)) bool = guess;}
    if (bool) *flagsp = flags|mask;
    else *flagsp = flags&(~(mask));}
  else *flagsp = flags|mask;
  u8_unlock_mutex(&init_server_lock);
  return 1;
}

/* Configuring whether clients have access to a full Scheme interpreter
   or only explicitly exported functions.  Note that even if they have
   a full scheme interpreter, they still only have access to "safe" functions,
   and can't access the filesystem, network, etc. */

static int fullscheme = 0;

static int config_set_fullscheme(lispval var,lispval val,void U8_MAYBE_UNUSED *data)
{
  int oldval = fullscheme;
  if (KNO_TRUEP(val))  fullscheme = 1; else fullscheme = 0;
  return (oldval!=fullscheme);
}

static lispval config_get_fullscheme(lispval var,void U8_MAYBE_UNUSED *data)
{
  if (fullscheme) return KNO_TRUE; else return KNO_FALSE;
}

/* Cleaning up state files */

/* The server currently writes two state files: a pid file indicating
   the server's process id, and an nid file indicating the ports on which
   the server is listening. */

static void cleanup_state_files()
{
  if (state_files_written) {
    u8_string exit_filename = kno_runbase_filename(".exit");
    FILE *exitfile = u8_fopen(exit_filename,"w");
    if (pid_file) {
      if (u8_file_existsp(pid_file)) u8_removefile(pid_file);
      u8_free(pid_file); pid_file = NULL;}
    if (cmd_file) {
      if (u8_file_existsp(cmd_file)) u8_removefile(cmd_file);
      u8_free(cmd_file); cmd_file = NULL;}
    if (nid_file) {
      if (u8_file_existsp(nid_file)) u8_removefile(nid_file);
      u8_free(nid_file); nid_file = NULL;}
    if (inject_file) {
      if (u8_file_existsp(inject_file)) u8_removefile(inject_file);
      u8_free(inject_file);
      inject_file = NULL;}
    if (exitfile) {
      struct U8_XTIME xt; struct U8_OUTPUT out;
      char timebuf[64]; double elapsed = u8_elapsed_time();
      u8_now(&xt); U8_INIT_FIXED_OUTPUT(&out,sizeof(timebuf),timebuf);
      u8_xtime_to_iso8601(&out,&xt);
      fprintf(exitfile,"%d@%s(%f)\n",getpid(),timebuf,elapsed);
      fclose(exitfile);}}
}

/* Core functions */

/* This represents a live client connection and its environment. */
typedef struct KNO_CLIENT {
  U8_CLIENT_FIELDS;
  struct KNO_STREAM clientstream;
  time_t lastlive; double elapsed;
  kno_lexenv env;} KNO_CLIENT;
typedef struct KNO_CLIENT *kno_client;

/* This creates the client structure when called by the server loop. */
static u8_client simply_accept(u8_server srv,u8_socket sock,
                               struct sockaddr *addr,size_t len)
{
  kno_client client = (kno_client)
    u8_client_init(NULL,sizeof(KNO_CLIENT),addr,len,sock,srv);
  kno_init_stream(&(client->clientstream),
                  client->idstring,sock,KNO_STREAM_SOCKET,
                  KNO_NETWORK_BUFSIZE);
  /* To help debugging, move the client->idstring (libu8)
     into the stream's id (knostorage). */
  client->env = kno_make_env(kno_make_hashtable(NULL,16),server_env);
  client->elapsed = 0; client->lastlive = ((time_t)(-1));
  u8_set_nodelay(sock,1);
  return (u8_client) client;
}

/* This serves a request on a client. If the libu8 server code
   is functioning properly, it will only be called on connections
   with data on them, so there shouldn't be too much waiting. */
static int dtypeserver(u8_client ucl)
{
  lispval expr;
  kno_client client = (kno_client)ucl;
  kno_stream stream = &(client->clientstream);
  kno_inbuf inbuf = kno_readbuf(stream);
  int async = ((async_mode)&&((client->server->flags)&U8_SERVER_ASYNC));

  /* Set the signal mask for the current thread.  By default, this
     only accepts synchronoyus signals. */
  /* Note that this is called on every loop, but we're presuming it's
     really fast. */
  pthread_sigmask(SIG_SETMASK,server_sigmask,NULL);

  if (auto_reload) kno_update_file_modules(0);
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
    int tracethis = ((logtrans) &&
                     ((client->n_trans==1) ||
                      (((client->n_trans)%logtrans)==0)));
    int trans_id = client->n_trans, sock = client->socket;
    double xstart = (u8_elapsed_time()), elapsed = -1.0;
    if (knod_loglevel >= LOG_DEBUG)
      u8_log(-LOG_DEBUG,Incoming,"%s[%d/%d]: > %q",
             client->idstring,sock,trans_id,expr);
    else if (knod_loglevel >= LOG_INFO)
      u8_log(-LOG_INFO,Incoming,
             "%s[%d/%d]: Received request for execution",
             client->idstring,sock,trans_id);
    value = kno_eval(expr,client->env);
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
        size_t head_off = outbuf->bufwrite-outbuf->buffer;
        unsigned char *buf = outbuf->buffer;
        kno_write_byte(outbuf,dt_block);
        kno_write_4bytes(outbuf,nbytes);
        memcpy(headbuf,buf+head_off,5);
        memmove(buf+start_off+5,buf+start_off,nbytes);
        memcpy(buf+start_off,headbuf,5);}}
    else kno_write_dtype(outbuf,value);
    if (async) {
      struct KNO_RAWBUF *rawbuf = (struct KNO_RAWBUF *)inbuf;
      size_t n_bytes = rawbuf->bufpoint-rawbuf->buffer;
      u8_client_write(ucl,rawbuf->buffer,n_bytes,0);
      rawbuf->bufpoint = rawbuf->buffer;
      return 1;}
    else {
      kno_write_dtype(outbuf,value);
      kno_flush_stream(stream);}
    time(&(client->lastlive));
    if (tracethis)
      u8_log(LOG_INFO,Outgoing,"%s[%d/%d]: Response sent after %fs",
             client->idstring,sock,trans_id,u8_elapsed_time()-xstart);
    kno_decref(expr); kno_decref(value);
    kno_swapcheck();
    return 0;}
}

static int close_knoclient(u8_client ucl)
{
  kno_client client = (kno_client)ucl;
  kno_close_stream(&(client->clientstream),0);
  kno_decref((lispval)((kno_client)ucl)->env);
  ucl->socket = -1;
  return 1;
}

/* Module configuration */

/* A list of exposed modules */
static lispval module_list = NIL;
/* This is the exposed environment. */
static kno_lexenv exposed_lexenv = NULL;
/* This is the shutdown procedure to be called when the
   server shutdowns. */
static lispval shutdown_procs = EMPTY;
static int normal_exit = 0;

static lispval config_get_modules(lispval var,void *data)
{
  return kno_incref(module_list);
}
static int config_use_module(lispval var,lispval val,void *data)
{
  int failed = 0;
  lispval module = kno_find_module(val,1);
  if (HASHTABLEP(module))
    exposed_lexenv =
      kno_make_env(kno_incref(module),exposed_lexenv);
  else if (KNO_LEXENVP(module)) {
    KNO_LEXENV *env = KNO_CONSPTR(kno_lexenv,module);
    if (HASHTABLEP(env->env_exports))
      exposed_lexenv =
        kno_make_env(kno_incref(env->env_exports),exposed_lexenv);}
  else {
    u8_log(LOG_WARN,"CorruptedModuleTable","Got %q for %q",
           module,var);
    failed = 1;}
  if (!(failed))
    module_list = kno_conspair(kno_incref(val),module_list);
  return 1;
}

/* Handling signals, etc. */

static u8_condition shutdown_reason = "fate";

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

static void shutdown_server(u8_string why)
{
  shutdown_reason = why;
  u8_server_shutdown(&dtype_server,shutdown_grace);
}

static lispval knod_shutdown_prim(lispval why)
{
  if (shutdown_reason)
    return KNO_FALSE;
  if (KNO_SYMBOLP(why))
    shutdown_server(KNO_SYMBOL_NAME(why));
  else if (KNO_STRINGP(why))
    shutdown_server(KNO_CSTRING(why));
  else shutdown_server("Knodaemon/SHUTDOWN");
  return KNO_TRUE;
}

static void shutdown_dtypeserver_onexit()
{
  shutdown_server("ONEXIT");
}

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

static lispval get_server_status()
{
  lispval result = kno_init_slotmap(NULL,0,NULL);
  struct U8_SERVER_STATS stats, livestats, curstats;

  kno_store(result,kno_intern("nthreads"),KNO_INT(dtype_server.n_threads));
  kno_store(result,kno_intern("nqueued"),KNO_INT(dtype_server.n_queued));
  kno_store(result,kno_intern("nbusy"),KNO_INT(dtype_server.n_busy));
  kno_store(result,kno_intern("nclients"),KNO_INT(dtype_server.n_clients));
  kno_store(result,kno_intern("totaltrans"),KNO_INT(dtype_server.n_trans));
  kno_store(result,kno_intern("totalconn"),KNO_INT(dtype_server.n_accepted));

  u8_server_statistics(&dtype_server,&stats);
  u8_server_livestats(&dtype_server,&livestats);
  u8_server_curstats(&dtype_server,&curstats);

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

static lispval asyncok()
{
  if ((async_mode)&&(dtype_server.flags&U8_SERVER_ASYNC))
    return KNO_TRUE;
  else return KNO_FALSE;
}

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

/* State dir */

static u8_string state_dir = NULL;

/* The main() event */

static void init_server()
{
  u8_lock_mutex(&init_server_lock);
  if (server_initialized) return;
  server_initialized = 1;
  u8_init_server
    (&dtype_server,
     simply_accept, /* acceptfn */
     dtypeserver, /* handlefn */
     NULL, /* donefn */
     close_knoclient, /* closefn */
     U8_SERVER_INIT_CLIENTS,init_clients,
     U8_SERVER_NTHREADS,n_threads,
     U8_SERVER_BACKLOG,max_backlog,
     U8_SERVER_MAX_QUEUE,max_queue,
     U8_SERVER_MAX_CLIENTS,max_conn,
     U8_SERVER_FLAGS,server_flags,
     U8_SERVER_END_INIT);
  dtype_server.xserverfn = server_loopfn;
  u8_unlock_mutex(&init_server_lock);
}

KNO_EXPORT int kno_init_dbserv(void);
static void init_configs(void);
static kno_lexenv init_core_env(void);
static int run_server(u8_string source_file);

static void exit_knodaemon()
{
  if (!(kno_be_vewy_quiet))
    kno_log_status("Exit(Knodaemon)");
}

int main(int argc,char **argv)
{
  int i = 1;
  /* Mask of args which we handle */
  unsigned char arg_mask[argc];  memset(arg_mask,0,argc);
  u8_string logfile = NULL;

  u8_log_show_date=1;
  u8_log_show_procinfo=1;
  u8_log_show_threadinfo=1;
  u8_log_show_elapsed=1;

  int u8_version = u8_initialize(), kno_version;
  u8_string server_spec = NULL, source_file = NULL, server_port = NULL;
  /* This is the base of the environment used to be passed to the server.
     It is augmented by the dbserv module, all of the modules declared by
     MODULE = configurations, and either the exports or the definitions of
     the server control file from the command line. */
  kno_lexenv core_env;

  kno_main_errno_ptr = &errno;

  KNO_INIT_STACK();

  server_sigmask = kno_default_sigmask;
  sigactions_init();

  /* Close and reopen STDIN */
  close(0);  if (open("/dev/null",O_RDONLY) == -1) {
    u8_log(LOG_CRIT,ServerAbort,"Unable to reopen stdin for daemon");
    exit(1);}
  /* We handle stderr and stdout below */

  if (u8_version<0) {
    u8_log(LOG_CRIT,ServerAbort,"Can't initialize LIBU8");
    exit(1);}
  /* Record the startup time for UPTIME */
  else u8_now(&boot_time);

  set_exename(argv);

  /* Find the server spec */
  while (i<argc) {
    if (isconfig(argv[i]))
      u8_log(LOG_NOTICE,"KnodaemonConfig","    %s",argv[i++]);
    else if (server_spec) i++;
    else {
      arg_mask[i] = 'X';
      server_spec = argv[i++];}} /* while (i<argc) */
  i = 1;

  if (!(server_spec)) {
    fprintf(stderr,
            "Usage: knod [conf = val]* (port|control_file) [conf = val]*\n");
    return 1;}
  else if (u8_file_existsp(server_spec))
    source_file = server_spec;
  else server_port = server_spec;

  if (getenv("STDLOG")) {
    u8_log(LOG_WARN,Startup,"Obeying STDLOG and using stdout/stderr for logging");}
  else if (getenv("LOGFILE")) {
    char *envfile = getenv("LOGFILE");
    if ((envfile)&&(envfile[0])&&(strcmp(envfile,"-")))
      logfile = u8_fromlibc(envfile);}
  else if ((getenv("LOGDIR"))&&(server_spec)) {
    u8_string base = u8_basename(server_spec,"*");
    u8_string logname = u8_mkstring("%s.log",base);
    logfile = u8_mkpath(getenv("LOGDIR"),logname);
    u8_free(base); u8_free(logname);}
  else if ((!(foreground))&&(server_spec)) {
    u8_string base = u8_basename(server_spec,"*");
    u8_string logname = u8_mkstring("%s.log",base);
    logfile = u8_mkpath(KNO_SERVLET_LOG_DIR,logname);
    u8_free(base); u8_free(logname);}
  else u8_log(LOG_WARN,Startup,"No logfile, using stdout");

  /* Close and reopen STDIN */
  close(0);  if (open("/dev/null",O_RDONLY) == -1) {
    u8_log(LOG_CRIT,ServerAbort,"Unable to reopen stdin for daemon");
    exit(1);}

  /* We do this using the Unix environment (rather than configuration
     variables) for two reasons.  First, we want to redirect errors
     from the processing of the configuration variables themselves
     (where lots of errors could happen); second, we want to be able
     to set this in the environment we wrap around calls (which is how
     mod_knocgi does it). */
  if (logfile) {
    int logsync = ((getenv("LOGSYNC") == NULL)?(0):(O_SYNC));
    int log_fd = open(logfile,O_RDWR|O_APPEND|O_CREAT|logsync,0644);
    if (log_fd<0) {
      u8_log(LOG_CRIT,ServerAbort,"Couldn't open log file %s",logfile);
      exit(1);}
    dup2(log_fd,1);
    dup2(log_fd,2);}

  char header[] =
    ";;====||====||====||====||====||====||====||===="
    "||====||====||====||====||====||====||====||===="
    "||====||====||====||====||====||====||====||====\n";
  ssize_t written = write(1,header,strlen(header));
  if (written < 0) {
    int err = errno; errno=0;
    u8_log(LOG_WARN,"WriteFailed",
           "Write to output failed errno=%d:%s",err,u8_strerror(err));}

  kno_setapp(server_spec,state_dir);
  kno_boot_message();

  {
    if (argc>2) {
      struct U8_OUTPUT out; unsigned char buf[2048]; int i = 1;
      U8_INIT_OUTPUT_BUF(&out,2048,buf);
      while (i<argc) {
        unsigned char *arg = argv[i++];
        if (arg == server_spec) u8_puts(&out," @");
        else {u8_putc(&out,' '); u8_puts(&out,arg);}}
      u8_log(LOG_WARN,Startup,"Starting beingmeta Knodaemon %s with:\n  %s",
             server_spec,out.u8_outbuf);
      u8_close((U8_STREAM *)&out);}
    else u8_log(LOG_WARN,Startup,"Starting beingmeta Knodaemon %s",server_spec);
    u8_log(LOG_WARN,Startup,
           "Copyright (C) beingmeta 2004-2019, all rights reserved");}

  kno_version = kno_init_scheme();

  if (kno_version<0) {
    fprintf(stderr,"Can't initialize Kno libraries\n");
    return -1;}

  atexit(exit_knodaemon);

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

  /* Now we initialize the libu8 logging configuration */
  u8_log_show_date = 1;
  u8_log_show_elapsed = 1;
  u8_log_show_procinfo = 1;
  u8_log_show_threadinfo = 1;
  u8_use_syslog(1);

#if ((!(HAVE_CONSTRUCTOR_ATTRIBUTES)) || (KNO_TESTCONFIG))
  kno_init_schemeio();
  kno_init_texttools();
#else
  KNO_INIT_SCHEME_BUILTINS();
#endif

  /* Initialize config settings */
  init_configs();

  if (getenv("STDLOG")) {}
  else if (log_filename) {}
  else if ((!(foreground))&&(server_spec)) {
    u8_string base = u8_basename(server_spec,"*");
    u8_string logname = u8_mkstring("%s.log",base);
    u8_string logfile = u8_mkpath(KNO_DAEMON_LOG_DIR,logname);
    set_logfile(logfile,1);
    u8_free(base); u8_free(logname);}
  else {
    u8_log(LOG_WARN,Startup,"No logfile, using stdout");}


  /* Get the core environment */
  core_env = init_core_env();

  /* Create the exposed environment.  This may be further modified by
     MODULE configs. */
  if (no_storage_api)
    exposed_lexenv = core_env;
  else exposed_lexenv=
         kno_make_env(kno_incref(kno_dbserv_module),core_env);

  /* Now process all the configuration arguments */
  kno_handle_argv(argc,argv,arg_mask,NULL);

  KNO_NEW_STACK(((struct KNO_STACK *)NULL),"knodaemon",NULL,VOID);
  _stack->stack_label=u8_strdup(u8_appid());
  U8_SETBITS(_stack->stack_flags,KNO_STACK_FREE_LABEL);

  /* Store server initialization information in the configuration
     environment. */
  if (source_file) {
    lispval interpreter = kno_wrapstring(u8_fromlibc(argv[0]));
    lispval src = kno_wrapstring(u8_realpath(source_file,NULL));
    kno_set_config("INTERPRETER",interpreter);
    kno_set_config("SOURCE",src);
    kno_decref(interpreter); kno_decref(src);}
  if (server_port) {
    lispval sval = knostring(server_port);
    kno_set_config("PORT",sval);
    kno_decref(sval);}

  pid_file = kno_runbase_filename(".pid");
  nid_file = kno_runbase_filename(".nid");
  cmd_file = kno_runbase_filename(".cmd");
  inject_file = kno_runbase_filename(".inj");

  write_cmd_file(cmd_file,"DaemonInvocation",argc,argv);

  init_server_env(server_spec,core_env);

  return run_server(server_spec);

}

static void init_configs()
{
  kno_register_config
    ("STEALSOCKETS",
     _("Remove existing socket files with extreme prejudice"),
     kno_boolconfig_get,kno_boolconfig_set,&stealsockets);
  kno_register_config
    ("KNOD:LOGLEVEL",_("Loglevel for knodaemon itself"),
     kno_intconfig_get,kno_loglevelconfig_set,&knod_loglevel);
  kno_register_config
    ("FOREGROUND",_("Whether to run in the foreground"),
     kno_boolconfig_get,kno_boolconfig_set,&foreground);
  kno_register_config
    ("RESTART",_("Whether to enable auto-restart"),
     kno_boolconfig_get,kno_boolconfig_set,&daemonize);
  kno_register_config
    ("PIDWAIT",_("Whether to wait for the servlet PID file"),
     kno_boolconfig_get,kno_boolconfig_set,&pidwait);
  kno_register_config
    ("BACKLOG",
     _("Number of pending connection requests allowed"),
     kno_intconfig_get,kno_intconfig_set,&max_backlog);
  kno_register_config
    ("MAXQUEUE",_("Max number of requests to keep queued"),
     kno_intconfig_get,kno_intconfig_set,&max_queue);
  kno_register_config
    ("INITCLIENTS",
     _("Number of clients to prepare for/grow by"),
     kno_intconfig_get,kno_intconfig_set,&init_clients);
  kno_register_config
    ("SERVERTHREADS",_("Number of threads in the thread pool"),
     kno_intconfig_get,kno_intconfig_set,&n_threads);
  kno_register_config
    ("MODULE",_("modules to provide in the server environment"),
     config_get_modules,config_use_module,NULL);
  kno_register_config
    ("FULLSCHEME",_("whether to provide full scheme interpreter"),
     config_get_fullscheme,config_set_fullscheme,NULL);
  kno_register_config
    ("LOGEVAL",_("Whether to log each request and response"),
     kno_boolconfig_get,kno_boolconfig_set,&logeval);
  kno_register_config
    ("LOGTRANS",_("Whether to log each transaction"),
     kno_intconfig_get,kno_boolconfig_set,&logtrans);
  kno_register_config
    ("LOGERRS",
     _("Whether to log errors returned by the server to clients"),
     kno_boolconfig_get,kno_boolconfig_set,&logerrs);
  kno_register_config
    ("LOGBACKTRACE",
     _("Whether to include a detailed backtrace when logging errors"),
     kno_boolconfig_get,kno_boolconfig_set,&logbacktrace);
  kno_register_config
    ("BACKTRACEWIDTH",_("Line width for logged backtraces"),
     kno_intconfig_get,kno_intconfig_set,&backtrace_width);
  kno_register_config
    ("U8LOGCONNECT",
     _("Whether to have libu8 log each connection"),
     config_get_dtype_server_flag,config_set_dtype_server_flag,
     (void *)(U8_SERVER_LOG_CONNECT));
  kno_register_config
    ("U8LOGTRANSACT",
     _("Whether to have libu8 log each transaction"),
     config_get_dtype_server_flag,config_set_dtype_server_flag,
     (void *)(U8_SERVER_LOG_TRANSACT));
#ifdef U8_SERVER_LOG_TRANSFER
  kno_register_config
    ("U8LOGTRANSFER",
     _("Whether to have libu8 log all data transfers"),
     config_get_dtype_server_flag,config_set_dtype_server_flag,
     (void *)(U8_SERVER_LOG_TRANSFER));
#endif
  kno_register_config
    ("U8ASYNC",
     _("Whether to support thread-asynchronous transactions"),
     config_get_dtype_server_flag,config_set_dtype_server_flag,
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
    ("STATEDIR",_("Where to write server pid/nid files"),
     kno_sconfig_get,kno_sconfig_set,&state_dir);
  kno_register_config
    ("ASYNCMODE",_("Whether to run in asynchronous mode"),
     kno_boolconfig_get,kno_boolconfig_set,&async_mode);
  kno_register_config
    ("GRACEFULDEATH",
     _("How long (μs) to wait for tasks during shutdown"),
     kno_intconfig_get,kno_intconfig_set,&shutdown_grace);
  kno_register_config
    ("AUTORELOAD",
     _("Whether to automatically reload changed files"),
     kno_boolconfig_get,kno_boolconfig_set,&auto_reload);
  kno_register_config
    ("NOSTORAGEAPI",
     _("Whether to disable exported Kno DB API"),
     kno_boolconfig_get,kno_boolconfig_set,&no_storage_api);
}

static void link_local_cprims()
{
}

static kno_lexenv init_core_env()
{
  /* This is a safe environment (e.g. a sandbox without file/io etc). */
  kno_lexenv core_env = kno_working_lexenv();
  lispval core_module = (lispval) core_env;
  kno_init_dbserv();
  kno_register_module("dbserv",kno_incref(kno_dbserv_module),0);
  kno_finish_module(kno_dbserv_module);

  /* We add some special functions */
  kno_def_evalfn(core_module,"bound?","",boundp_evalfn);

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
  link_local_cprims();

  return core_env;
}

static void write_state_files()
{
  FILE *f = u8_fopen(pid_file,"w"); if (f) {
    fprintf(f,"%d\n",getpid());
    fclose(f);}
  else {
    u8_log(LOG_WARN,u8_strerror(errno),
           "Couldn't write PID %d to '%s'",
           getpid(),pid_file);
    errno = 0;}
  /* Write the NID file */
  f = u8_fopen(nid_file,"w"); if (f) {
    if (dtype_server.n_servers) {
      int i = 0; while (i<dtype_server.n_servers) {
        fprintf(f,"%s\n",dtype_server.server_info[i].idstring);
        i++;}}
    fclose(f);}
  else {
    u8_log(LOG_WARN,u8_strerror(errno),
           "Couldn't write NID info to '%s'",
           pid_file);
    if (dtype_server.n_servers) {
      int i = 0; while (i<dtype_server.n_servers) {
        u8_log(LOG_NOTICE,Startup,"%s\n",
               dtype_server.server_info[i].idstring);
        i++;}}
    else u8_log(LOG_NOTICE,Startup,"temp.socket\n");
    errno = 0;}
  state_files_written = u8_microtime();
  atexit(cleanup_state_files);
}

static int init_server_env(u8_string server_spec,kno_lexenv core_env)
{
#ifdef SIGHUP
  sigaction(SIGHUP,&sigaction_shutdown,NULL);
#endif
#ifdef SIGTERM
  sigaction(SIGTERM,&sigaction_shutdown,NULL);
#endif
#ifdef SIGEXIT
  sigaction(SIGEXT,&sigaction_shutdown,NULL);
#endif
  if (u8_file_existsp(server_spec)) {
    /* The source file is loaded into a full (non sandbox environment).
       Its exports are then exposed through the server. */
    u8_string source_file = u8_abspath(server_spec,NULL);
    kno_lexenv env = working_env = kno_working_lexenv();
    lispval result = kno_load_source(source_file,env,NULL);
    if (KNO_TROUBLEP(result)) {
      u8_exception e = u8_erreify();
      U8_OUTPUT out; U8_INIT_STATIC_OUTPUT(out,512);
      kno_print_exception(&out,e);
      kno_print_backtrace(&out,e,80);
      fputs(out.u8_outbuf,stderr);
      u8_free(out.u8_outbuf);
      u8_free(source_file);
      kno_decref(result);
      kno_decref((lispval)env);
      return -1;}
    else {
      lispval startup_proc = kno_symeval(kno_intern("startup"),env);
      lispval shutdown_proc = kno_symeval(kno_intern("shutdown"),env);
      if (KNO_APPLICABLEP(shutdown_proc)) {
        KNO_ADD_TO_CHOICE(shutdown_procs,shutdown_proc);}
      kno_decref(result); result = VOID;
      /* If the init file did any exporting, expose those exports to
         clients.  Otherwise, expose all the definitions in the init
         file.  Note that the clients won't be able to get at the
         unsafe "empowered" environment but that the procedures
         defined are closed in that environment. */
      if (HASHTABLEP(env->env_exports))
        server_env = kno_make_env(kno_incref(env->env_exports),
                                  exposed_lexenv);
      else server_env = kno_make_env(kno_incref(env->env_bindings),
                                     exposed_lexenv);
      if (fullscheme==0) {
        /* Cripple the core environment if requested */
        kno_decref((lispval)(core_env->env_parent));
        core_env->env_parent = NULL;}
      if (KNO_VOIDP(startup_proc)) {}
      else {
        DO_CHOICES(p,startup_proc) {
          lispval result = kno_apply(p,0,NULL);
          if (KNO_ABORTP(result)) {
            u8_exception ex = u8_erreify(), root = ex;
            int old_maxelts = kno_unparse_maxelts;
            int old_maxchars = kno_unparse_maxchars;
            U8_OUTPUT out; U8_INIT_STATIC_OUTPUT(out,512);
            while (root->u8x_prev) root = root->u8x_prev;
            kno_unparse_maxchars = debug_maxchars;
            kno_unparse_maxelts = debug_maxelts;
            kno_print_exception(&out,root);
            kno_print_backtrace(&out,ex,80);
            kno_unparse_maxelts = old_maxelts;
            kno_unparse_maxchars = old_maxchars;
            fputs(out.u8_outbuf,stderr);
            u8_free(out.u8_outbuf);
            u8_free_exception(ex,1);
            exit(kno_interr(result));}
          else kno_decref(result);}}}}
  else server_env = exposed_lexenv;
  kno_defn((lispval)working_env,
           kno_make_cprim1("knod/shutdown!",knod_shutdown_prim,MIN_ARGS(0),
                           "Shuts down the running daemon"));
  return 1;
}

static int run_server(u8_string server_spec)
{
  init_server();
  /* Prepare for the end */
  atexit(shutdown_dtypeserver_onexit);
#ifdef SIGTERM
  sigaction(SIGTERM,&sigaction_shutdown,NULL);
#endif
#ifdef SIGQUIT
  sigaction(SIGQUIT,&sigaction_shutdown,NULL);
#endif
  kno_register_config("PORT",_("port or port@host to listen on"),
                      config_get_ports,config_serve_port,NULL);
  if (n_ports<=0) {
    u8_log(LOG_WARN,NoServers,"No servers configured, exiting...");
    exit(-1);
    return -1;}
  write_state_files();
  u8_message("beingmeta Kno, (C) beingmeta 2004-2019, all rights reserved");
  u8_log(LOG_NOTICE,ServerStartup,
         "Kno (%s) Knodaemon %s running, %d/%d pools/indexes, %d ports",
         KNO_REVISION,server_spec,kno_n_pools,
         kno_n_primary_indexes+kno_n_secondary_indexes,n_ports);
  u8_log(LOG_NOTICE,ServerStartup,"Serving on %d sockets",n_ports);
  u8_server_loop(&dtype_server);
  normal_exit = 1;
  u8_log(LOG_CRIT,ServerShutdown,
         "Shutting down server for %s",
         shutdown_reason);
  run_shutdown_procs();
  u8_log(LOG_NOTICE,ServerShutdown,"Exited server loop");
  u8_threadexit();
  kno_doexit(KNO_FALSE);
  return 0;
}

