/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2016 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/numbers.h"
#include "framerd/support.h"
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

#if HAVE_SYS_WAIT_H
#include <sys/wait.h>
#endif

#include "main.h"

FD_EXPORT int fd_update_file_modules(int force);

#include "main.c"

static int daemonize=0, foreground=0, pidwait=1;

static long long state_files_written=0;

#define nobytes(in,nbytes) (FD_EXPECT_FALSE(!(fd_needs_bytes(in,nbytes))))
#define havebytes(in,nbytes) (FD_EXPECT_TRUE(fd_needs_bytes(in,nbytes)))

static int async_mode=1;
static int auto_reload=0;

static int debug_maxelts=32, debug_maxchars=80;

/* Various exceptions */
static fd_exception BadPortSpec=_("Bad port spec");
static fd_exception BadRequest=_("Bad client request");
static u8_condition NoServers=_("NoServers");
static u8_condition Startup=_("Startup");
static u8_condition ServerFork=_("FDServer/FORK");
static u8_condition ServerAbort=_("FDServer/ABORT");
static u8_condition ServerSignal=_("FDServer/SIGNAL");
static u8_condition ServerRestart=_("FDServer/RESTART");
static u8_condition ServerConfig=_("FDServer/CONFIG");
static u8_condition ServerLoop=_("FDServer/LOOP");
static u8_condition ServerStartup=_("FDServer/STARTUP");
static u8_condition ServerShutdown=_("FDServer/SHUTDOWN");

static const sigset_t *server_sigmask;

static u8_condition Incoming=_("Incoming"), Outgoing=_("Outgoing");

static u8_string pid_file=NULL, nid_file=NULL, cmd_file=NULL, inject_file=NULL;

/* This is the global lisp environment for all servers.
   It is modified to include various modules, including dbserv. */
static fd_lispenv working_env=NULL, server_env=NULL;

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

static int no_fddb=0;

static time_t last_launch=(time_t)-1;
static int fastfail_threshold=60, fastfail_wait=60;

#if FD_THREADS_ENABLED
static u8_mutex init_server_lock;
#endif

static void init_server(void);

/* Managing your dependent (for restarting servers) */

static int sustaining=0;
static pid_t dependent=-1;
static void kill_dependent_onexit()
{
  u8_string ppid_file=fd_runbase_filename(".ppid");
  pid_t dep=dependent; dependent=-1;
  sustaining=0;
  if (dep>0) kill(dep,SIGTERM);
  if ((ppid_file)&&(u8_file_existsp(ppid_file))) {
    u8_removefile(ppid_file);
    u8_free(ppid_file);
    ppid_file=NULL;}
  if ((inject_file)&&(u8_file_existsp(inject_file))) {
    u8_removefile(inject_file);
    u8_free(inject_file);
    inject_file=NULL;}
}

static void kill_dependent_onsignal(int sig,siginfo_t *info,void *stuff)
{
  u8_string ppid_file=fd_runbase_filename(".ppid");
  pid_t dep=dependent; dependent=-1;
  sustaining=0;
  if (dep>0)
    u8_log(LOG_WARN,ServerSignal,
           "FDServer controller %d got signal %d, passing to %d",
           getpid(),sig,dep);
  if (dep>0) kill(dep,sig);
  if ((ppid_file)&&(u8_file_existsp(ppid_file))) {
    u8_removefile(ppid_file);
    u8_free(ppid_file);
    ppid_file=NULL;}
  if ((inject_file)&&(u8_file_existsp(inject_file))) {
    u8_removefile(inject_file);
    u8_free(inject_file);
    inject_file=NULL;}
}

static void shutdown_server_onsignal(int,siginfo_t *,void *);

static struct sigaction sigaction_ignore;
static struct sigaction sigaction_abraham;
static struct sigaction sigaction_shutdown;
 
static void sigactions_init()
{
  memset(&sigaction_ignore,0,sizeof(sigaction_ignore));
  sigaction_ignore.sa_handler=SIG_IGN;
  
  memset(&sigaction_abraham,0,sizeof(sigaction_ignore));
  sigaction_abraham.sa_sigaction=kill_dependent_onsignal;
  sigaction_abraham.sa_flags=SA_SIGINFO;

  memset(&sigaction_shutdown,0,sizeof(sigaction_ignore));
  sigaction_shutdown.sa_sigaction=kill_dependent_onsignal;
  sigaction_shutdown.sa_flags=SA_SIGINFO;

}

/* Checking for (good) injections */

static int check_for_injection()
{
  if (working_env==NULL) return 0;
  else if (inject_file==NULL) return 0;
  else if (u8_file_existsp(inject_file)) {
    u8_string temp_file=u8_string_append(inject_file,".loading",NULL);
    int rv=u8_movefile(inject_file,temp_file);
    if (rv<0) {
      u8_log(LOGWARN,"FDServlet/InjectionIgnored",
             "Can't stage injection file %s to %s",
             inject_file,temp_file);
      fd_clear_errors(1);
      u8_free(temp_file);
      return 0;}
    else {
      u8_string content=u8_filestring(temp_file,NULL);
      if (content==NULL)  {
        u8_log(LOGWARN,"FDServlet/InjectionCantRead",
               "Can't read %s",temp_file);
        fd_clear_errors(1);
        u8_free(temp_file);
        return -1;}
      else {
        fdtype result;
        u8_log(LOGWARN,"FDServlet/InjectLoad",
               "From %s\n\"%s\"",temp_file,content);
        result=fd_load_source(temp_file,working_env,NULL);
        if (FD_ABORTP(result)) {
          u8_exception ex=u8_current_exception;
          if (!(ex)) {
            u8_log(LOGCRIT,"FDServlet/InjectError",
                   "Unknown error processing injection from %s: \"%s\"",
                   inject_file,content);}
          else if ((ex->u8x_context!=NULL)&&
                   (ex->u8x_details!=NULL))
            u8_log(LOGCRIT,"FDServlet/InjectionError",
                   "Error %s (%s) processing injection %s: %s\n\"%s\"",
                   ex->u8x_cond,ex->u8x_context,inject_file,
                   ex->u8x_details,content);
          else if (ex->u8x_context!=NULL) 
            u8_log(LOGCRIT,"FDServlet/InjectionError",
                   "Error %s (%s) processing injection %s\n\"%s\"",
                   ex->u8x_cond,ex->u8x_context,inject_file,content);
          else u8_log(LOGCRIT,"FDServlet/InjectionError",
                      "Error %s processing injection %s\n\"%s\"",
                      ex->u8x_cond,inject_file,content);
          fd_clear_errors(1);
          return -1;}
        else {
          u8_log(LOGWARN,"FDServlet/InjectionDone",
                 "Finished from %s",inject_file);}
        rv=u8_removefile(temp_file);
        if (rv<0) {
          u8_log(LOGCRIT,"FDServlet/InjectionCleanup",
                 "Error removing %s",temp_file);
          fd_clear_errors(1);}
        fd_decref(result);
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

static u8_string log_filename=NULL;
static int log_fd=-1;
static int set_logfile(u8_string logfile,int exitonfail)
{
  int logsync=((getenv("LOGSYNC")==NULL)?(0):(O_SYNC));
  if (!(logfile)) {
    u8_log(LOG_WARN,ServerConfig,"Can't specify NULL log file");
    if (exitonfail) exit(1);
    u8_seterr(LogFileError,"set_logfile",u8_strdup("Can't set NULL log file"));
    return -1;}
  int new_fd=open(logfile,O_RDWR|O_APPEND|O_CREAT|logsync,0644);
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
  log_fd=new_fd;
  if (log_filename) u8_free(log_filename);
  log_filename=u8_strdup(logfile);
  return 1;
}

/*
static fdtype config_get_logfile(fdtype var,void *state)
{
  if (log_filename)
    return fdstring(log_filename);
  else return FD_FALSE;
}

static int config_set_logfile(fdtype var,fdtype val,void *state)
{
  if (!(FD_STRINGP(val)))  {
    fd_seterr(fd_TypeError,"config_set_logfile,",u8_strdup("filename"),val);
    return -1;}
  else if (set_logfile(FD_STRDATA(val),0)<0) return -1;
  else return 1;
}
*/

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
    if (retval<0) {
      fd_seterr(BadPortSpec,"config_serve_port",NULL,val);
      return -1;}
    else n_ports=n_ports+retval;
    return retval;}
  else if (FD_STRINGP(val)) {
    int retval=u8_add_server(&dtype_server,FD_STRDATA(val),0);
    if (retval<0) {
      fd_seterr(BadPortSpec,"config_serve_port",NULL,val);
      return -1;}
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
    fdtype id=fdstring(dtype_server.server_info[i].idstring);
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
        u8_log(LOG_WARN,ServerConfig,"Unknown boolean setting %s for %q",s,var);
        fd_unlock_mutex(&init_server_lock);
        return fd_reterr(fd_TypeError,"setserverflag","boolean value",val);}
      else u8_log(LOG_WARN,ServerConfig,
                  "Unfamiliar boolean setting %s for %q, assuming %s",
                  s,var,((guess)?("true"):("false")));
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

#define need_escape(s) \
  ((strchr(s,'"'))||(strchr(s,'\\'))|| \
   (strchr(s,' '))||(strchr(s,'\t'))|| \
   (strchr(s,'\n'))||(strchr(s,'\r')))

static void write_cmd_file(int argc,char **argv)
{
  const char *abspath=u8_abspath(cmd_file,NULL);
  int i=0, fd=open(abspath,O_CREAT|O_RDWR|O_TRUNC,
                   S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH);
  u8_byte buf[512]; struct U8_OUTPUT out;
  U8_INIT_OUTPUT_BUF(&out,512,buf);
  while (i<argc) {
    char *arg=argv[i];
    u8_string argstring=u8_fromlibc(arg);
    if (i>0) u8_putc(&out,' '); i++;
    if (need_escape(argstring)) {
      u8_string scan=argstring; 
      int c=u8_sgetc(&scan); u8_putc(&out,'"');
      while (c>=0) {
        if (c=='\\') {
          u8_putc(&out,'\\'); c=u8_sgetc(&scan);}
        else if ((c==' ')||(c=='\n')||(c=='\t')||(c=='\r')||(c=='"')) {
          u8_putc(&out,'\\');}
        if (c>=0) u8_putc(&out,c);
        c=u8_sgetc(&scan);}
      u8_putc(&out,'"');}
    else u8_puts(&out,argstring);
    if (argstring!=((u8_string)arg)) u8_free(argstring);}
  u8_log(LOG_INFO,"DaemonInvocation","%s",out.u8_outbuf);
  if (fd>=0) write(fd,out.u8_outbuf,out.u8_outptr-out.u8_outbuf);
  u8_free(abspath); u8_close_output(&out); close(fd);
}

static void cleanup_state_files()
{
  if (state_files_written) {
    u8_string exit_filename=fd_runbase_filename(".exit");
    FILE *exitfile=u8_fopen(exit_filename,"w");
    if (pid_file) {
      if (u8_file_existsp(pid_file)) u8_removefile(pid_file);
      u8_free(pid_file); pid_file=NULL;}
    if (cmd_file) {
      if (u8_file_existsp(cmd_file)) u8_removefile(cmd_file);
      u8_free(cmd_file); cmd_file=NULL;}
    if (nid_file) {
      if (u8_file_existsp(nid_file)) u8_removefile(nid_file);
      u8_free(nid_file); nid_file=NULL;}
    if (inject_file) {
      if (u8_file_existsp(inject_file)) u8_removefile(inject_file);
      u8_free(inject_file);
      inject_file=NULL;}
    if (exitfile) {
      struct U8_XTIME xt; struct U8_OUTPUT out;
      char timebuf[64]; double elapsed=u8_elapsed_time();
      u8_now(&xt); U8_INIT_FIXED_OUTPUT(&out,sizeof(timebuf),timebuf);
      u8_xtime_to_iso8601(&out,&xt);
      fprintf(exitfile,"%d@%s(%f)\n",getpid(),timebuf,elapsed);
      fclose(exitfile);}}
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
    else client->stream.id=u8_strdup("fdserver/dtypestream");}
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
  fdtype expr;
  fd_client client=(fd_client)ucl;
  fd_dtype_stream stream=&(client->stream);
  int async=((async_mode)&&((client->server->flags)&U8_SERVER_ASYNC));

  /* Set the signal mask for the current thread.  By default, this
     only accepts synchronoyus signals. */
  /* Note that this is called on every loop, but we're presuming it's
     really fast. */
  pthread_sigmask(SIG_SETMASK,server_sigmask,NULL);

  if (auto_reload) fd_update_file_modules(0);
  if ((client->reading>0)&&(u8_client_finished(ucl))) {
    expr=fd_dtsread_dtype(stream);}
  else if ((client->writing>0)&&(u8_client_finished(ucl))) {
    /* Reset the stream */
    stream->ptr=stream->start;
    /* Update the stream if we were doing asynchronous I/O */
    if ((client->buf==stream->start)&&(client->len))
      stream->end=stream->ptr+client->len;
    /* And report that we're finished */
    return 0;}
  else if ((client->reading>0)||(client->writing>0))
    /* These should never happen, but let's be complete */
    return 1;
  else if (async) {
    /* See if we can use asynchronous reading */
    fd_dts_start_read(stream);
    if (nobytes((fd_byte_input)stream,1)) expr=FD_EOD;
    else if ((*(stream->ptr))==dt_block) {
      int MAYBE_UNUSED dtcode=fd_dtsread_byte(stream);
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
           "%s[%d]: Received bad request %q",
           client->idstring,client->n_trans,expr);
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
          struct U8_OUTPUT out; U8_INIT_STATIC_OUTPUT(out,1024);
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
  else if (FD_ENVIRONMENTP(module)) {
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
  else if (FD_ENVIRONMENTP(module)) {
    FD_ENVIRONMENT *env=
      FD_GET_CONS(module,fd_environment_type,FD_ENVIRONMENT *);
    if (FD_HASHTABLEP(env->exports))
      exposed_environment=
        fd_make_env(fd_incref(env->exports),exposed_environment);}
  module_list=fd_conspair(fd_incref(val),module_list);
  return 1;
}

/* Handling signals, etc. */

static void shutdown_dtypeserver_onsignal(int sig)
{
  if (sig==SIGHUP) {
    fd_update_file_modules(1);
    return;}
  u8_log(LOG_CRIT,ServerShutdown,"Shutting down server on signal %d",sig);
  u8_server_shutdown(&dtype_server,shutdown_grace);
  if (FD_APPLICABLEP(shutdown_proc)) {
    fdtype sigval=FD_INT(sig), value;
    u8_log(LOG_WARNING,ServerShutdown,"Calling shutdown procedure %q",
           shutdown_proc);
    value=fd_apply(shutdown_proc,1,&sigval);
    fd_decref(value);}
  cleanup_state_files();
  fd_doexit(FD_INT(sig));
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
  return fd_init_flonum(NULL,u8_xtime_diff(&now,&boot_time));
}

static fdtype get_server_status()
{
  fdtype result=fd_init_slotmap(NULL,0,NULL);
  struct U8_SERVER_STATS stats, livestats, curstats;

  fd_store(result,fd_intern("NTHREADS"),FD_INT(dtype_server.n_threads));
  fd_store(result,fd_intern("NQUEUED"),FD_INT(dtype_server.n_queued));
  fd_store(result,fd_intern("NBUSY"),FD_INT(dtype_server.n_busy));
  fd_store(result,fd_intern("NCLIENTS"),FD_INT(dtype_server.n_clients));
  fd_store(result,fd_intern("TOTALTRANS"),FD_INT(dtype_server.n_trans));
  fd_store(result,fd_intern("TOTALCONN"),FD_INT(dtype_server.n_accepted));

  u8_server_statistics(&dtype_server,&stats);
  u8_server_livestats(&dtype_server,&livestats);
  u8_server_curstats(&dtype_server,&curstats);

  fd_store(result,fd_intern("NACTIVE"),FD_INT(stats.n_active));
  fd_store(result,fd_intern("NREADING"),FD_INT(stats.n_reading));
  fd_store(result,fd_intern("NWRITING"),FD_INT(stats.n_writing));
  fd_store(result,fd_intern("NXBUSY"),FD_INT(stats.n_busy));

  if (stats.tcount>0) {
    fd_store(result,fd_intern("TRANSAVG"),
             fd_make_flonum(((double)stats.tsum)/
                            (((double)stats.tcount))));
    fd_store(result,fd_intern("TRANSMAX"),FD_INT(stats.tmax));
    fd_store(result,fd_intern("TRANSCOUNT"),FD_INT(stats.tcount));}

  if (stats.qcount>0) {
    fd_store(result,fd_intern("QUEUEAVG"),
             fd_make_flonum(((double)stats.qsum)/
                            (((double)stats.qcount))));
    fd_store(result,fd_intern("QUEUEMAX"),FD_INT(stats.qmax));
    fd_store(result,fd_intern("QUEUECOUNT"),FD_INT(stats.qcount));}

  if (stats.rcount>0) {
    fd_store(result,fd_intern("READAVG"),
             fd_make_flonum(((double)stats.rsum)/
                            (((double)stats.rcount))));
    fd_store(result,fd_intern("READMAX"),FD_INT(stats.rmax));
    fd_store(result,fd_intern("READCOUNT"),FD_INT(stats.rcount));}

  if (stats.wcount>0) {
    fd_store(result,fd_intern("WRITEAVG"),
             fd_make_flonum(((double)stats.wsum)/
                            (((double)stats.wcount))));
    fd_store(result,fd_intern("WRITEMAX"),FD_INT(stats.wmax));
    fd_store(result,fd_intern("WRITECOUNT"),FD_INT(stats.wcount));}

  if (stats.xcount>0) {
    fd_store(result,fd_intern("EXECAVG"),
             fd_make_flonum(((double)stats.xsum)/
                            (((double)stats.xcount))));
    fd_store(result,fd_intern("EXECMAX"),FD_INT(stats.xmax));
    fd_store(result,fd_intern("EXECCOUNT"),FD_INT(stats.xcount));}

  if (livestats.tcount>0) {
    fd_store(result,fd_intern("LIVE/TRANSAVG"),
             fd_make_flonum(((double)livestats.tsum)/
                            (((double)livestats.tcount))));
    fd_store(result,fd_intern("LIVE/TRANSMAX"),FD_INT(livestats.tmax));
    fd_store(result,fd_intern("LIVE/TRANSCOUNT"),
             FD_INT(livestats.tcount));}

  if (livestats.qcount>0) {
    fd_store(result,fd_intern("LIVE/QUEUEAVG"),
             fd_make_flonum(((double)livestats.qsum)/
                            (((double)livestats.qcount))));
    fd_store(result,fd_intern("LIVE/QUEUEMAX"),FD_INT(livestats.qmax));
    fd_store(result,fd_intern("LIVE/QUEUECOUNT"),
             FD_INT(livestats.qcount));}

  if (livestats.rcount>0) {
    fd_store(result,fd_intern("LIVE/READAVG"),
             fd_make_flonum(((double)livestats.rsum)/
                            (((double)livestats.rcount))));
    fd_store(result,fd_intern("LIVE/READMAX"),FD_INT(livestats.rmax));
    fd_store(result,fd_intern("LIVE/READCOUNT"),
             FD_INT(livestats.rcount));}

  if (livestats.wcount>0) {
    fd_store(result,fd_intern("LIVE/WRITEAVG"),
             fd_make_flonum(((double)livestats.wsum)/
                            (((double)livestats.wcount))));
    fd_store(result,fd_intern("LIVE/WRITEMAX"),FD_INT(livestats.wmax));
    fd_store(result,fd_intern("LIVE/WRITECOUNT"),
             FD_INT(livestats.wcount));}

  if (livestats.xcount>0) {
    fd_store(result,fd_intern("LIVE/EXECAVG"),
             fd_make_flonum(((double)livestats.xsum)/
                            (((double)livestats.xcount))));
    fd_store(result,fd_intern("LIVE/EXECMAX"),FD_INT(livestats.xmax));
    fd_store(result,fd_intern("LIVE/EXECCOUNT"),
             FD_INT(livestats.xcount));}

    if (curstats.tcount>0) {
    fd_store(result,fd_intern("CUR/TRANSAVG"),
             fd_make_flonum(((double)curstats.tsum)/
                            (((double)curstats.tcount))));
    fd_store(result,fd_intern("CUR/TRANSMAX"),FD_INT(curstats.tmax));
    fd_store(result,fd_intern("CUR/TRANSCOUNT"),
             FD_INT(curstats.tcount));}

  if (curstats.qcount>0) {
    fd_store(result,fd_intern("CUR/QUEUEAVG"),
             fd_make_flonum(((double)curstats.qsum)/
                            (((double)curstats.qcount))));
    fd_store(result,fd_intern("CUR/QUEUEMAX"),FD_INT(curstats.qmax));
    fd_store(result,fd_intern("CUR/QUEUECOUNT"),
             FD_INT(curstats.qcount));}

  if (curstats.rcount>0) {
    fd_store(result,fd_intern("CUR/READAVG"),
             fd_make_flonum(((double)curstats.rsum)/
                            (((double)curstats.rcount))));
    fd_store(result,fd_intern("CUR/READMAX"),FD_INT(curstats.rmax));
    fd_store(result,fd_intern("CUR/READCOUNT"),
             FD_INT(curstats.rcount));}

  if (curstats.wcount>0) {
    fd_store(result,fd_intern("CUR/WRITEAVG"),
             fd_make_flonum(((double)curstats.wsum)/
                            (((double)curstats.wcount))));
    fd_store(result,fd_intern("CUR/WRITEMAX"),FD_INT(curstats.wmax));
    fd_store(result,fd_intern("CUR/WRITECOUNT"),
             FD_INT(curstats.wcount));}

  if (curstats.xcount>0) {
    fd_store(result,fd_intern("CUR/EXECAVG"),
             fd_make_flonum(((double)curstats.xsum)/
                            (((double)curstats.xcount))));
    fd_store(result,fd_intern("CUR/EXECMAX"),FD_INT(curstats.xmax));
    fd_store(result,fd_intern("CUR/EXECCOUNT"),
             FD_INT(curstats.xcount));}

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
  dtype_server.xserverfn=server_loopfn;
  fd_unlock_mutex(&init_server_lock);
}

FD_EXPORT int fd_init_fddbserv(void);
static void init_configs(void);
static fd_lispenv init_core_env(void);
static int launch_server(u8_string source_file,fd_lispenv env);
static int fork_server(u8_string source_file,fd_lispenv env);
static int run_server(u8_string source_file);

int main(int argc,char **argv)
{
  int u8_version=u8_initialize();
  int fd_version;
  int i=1; u8_string server_spec=NULL, source_file=NULL, server_port=NULL;
  /* This is the base of the environment used to be passed to the server.
     It is augmented by the fdbserv module, all of the modules declared by
     MODULE= configurations, and either the exports or the definitions of
     the server control file from the command line.
     It starts out built on the default safe environment, but loses that if
     fullscheme is zero after configuration and file loading.  fullscheme can be
     set by the FULLSCHEME configuration parameter. */
  fd_lispenv core_env;

  server_sigmask=fd_default_sigmask;
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

  /* Find the source file (the non-config arg)
     Also initialize the log file if needed.  */
  while (i<argc)
    if (strchr(argv[i],'=')) i++;
    else if (server_spec) i++;
    else server_spec=argv[i++];
  i=1;

  if (!(server_spec)) {
    fprintf(stderr,
            "Usage: fdserver [conf=val]* (port|control_file) [conf=val]*\n");
    return 1;}
  else if (u8_file_existsp(server_spec))
    source_file=server_spec;
  else server_port=server_spec;

  fddb_loglevel=LOG_INFO;

  if (getenv("STDLOG")) {
    u8_log(LOG_WARN,Startup,
           "Obeying STDLOG and using stdout/stderr for logging");}
  else if ((!(log_filename))&&(getenv("LOGFILE")))
    set_logfile(getenv("LOGFILE"),1);
  if ((!(log_filename))&&(getenv("LOGDIR"))) {
    u8_string base=u8_basename(server_spec,"*");
    u8_string logname=u8_mkstring("%s.log",base);
    u8_string logfile=u8_mkpath(getenv("LOGDIR"),logname);
    set_logfile(logfile,1);
    u8_free(base); u8_free(logname);}

  {
    if (argc>2) {
      struct U8_OUTPUT out; unsigned char buf[2048]; int i=1;
      U8_INIT_OUTPUT_BUF(&out,2048,buf);
      while (i<argc) {
        unsigned char *arg=argv[i++];
        if (arg==server_spec) u8_puts(&out," @");
        else {u8_putc(&out,' '); u8_puts(&out,arg);}}
      u8_log(LOG_WARN,Startup,"Starting beingmeta fdserver %s with:\n  %s",
             server_spec,out.u8_outbuf);
      u8_close((U8_STREAM *)&out);}
    else u8_log(LOG_WARN,Startup,"Starting beingmeta fdserver %s",server_spec);
    u8_log(LOG_WARN,Startup,
           "Copyright (C) beingmeta 2004-2016, all rights reserved");}

  fd_version=fd_init_fdscheme();

  if (fd_version<0) {
    fprintf(stderr,"Can't initialize FramerD libraries\n");
    return -1;}

  atexit(fd_status_message);

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
  u8_log_show_date=1;
  u8_log_show_elapsed=1;
  u8_log_show_procinfo=1;
  u8_log_show_threadinfo=1;
  u8_use_syslog(1);

#if ((!(HAVE_CONSTRUCTOR_ATTRIBUTES)) || (FD_TESTCONFIG))
  fd_init_schemeio();
  fd_init_texttools();
#else
  FD_INIT_SCHEME_BUILTINS();
#endif

  /* Initialize config settings */
  init_configs();

  if (getenv("STDLOG")) {}
  else if (log_filename) {}
  else if ((!(foreground))&&(server_spec)) {
    u8_string base=u8_basename(server_spec,"*");
    u8_string logname=u8_mkstring("%s.log",base);
    u8_string logfile=u8_mkpath(FD_DAEMON_LOG_DIR,logname);
    set_logfile(logfile,1);
    u8_free(base); u8_free(logname);}
  else {
    u8_log(LOG_WARN,Startup,"No logfile, using stdout");}


  /* Get the core environment */
  core_env=init_core_env();

  /* Create the exposed environment.  This may be further modified by
     MODULE configs. */
  if (no_fddb)
    exposed_environment=core_env;
  else exposed_environment=
         fd_make_env(fd_incref(fd_fdbserv_module),core_env);

  /* Now process all the configuration arguments */
  while (i<argc)
    if (strchr(argv[i],'='))
      fd_config_assignment(argv[i++]);
    else i++;

  /* Store server initialization information in the configuration
     environment. */
  fd_setapp(server_spec,state_dir);
  if (source_file) {
    fdtype interpreter=fd_lispstring(u8_fromlibc(argv[0]));
    fdtype src=fd_lispstring(u8_realpath(source_file,NULL));
    fd_config_set("INTERPRETER",interpreter);
    fd_config_set("SOURCE",src);
    fd_decref(interpreter); fd_decref(src);}
  if (server_port) {
    fdtype sval=fdstring(server_port);
    fd_config_set("PORT",sval);
    fd_decref(sval);}

  fd_boot_message();
  u8_now(&boot_time);

  pid_file=fd_runbase_filename(".pid");
  nid_file=fd_runbase_filename(".nid");
  cmd_file=fd_runbase_filename(".cmd");
  inject_file=fd_runbase_filename(".inj");

  write_cmd_file(argc,argv);

  if ((daemonize>0)||(!(foreground)))
    return fork_server(server_spec,core_env);
  else return launch_server(server_spec,core_env);
}

static void init_configs()
{
  fd_register_config("FOREGROUND",_("Whether to run in the foreground"),
                     fd_boolconfig_get,fd_boolconfig_set,&foreground);
  fd_register_config("RESTART",_("Whether to enable auto-restart"),
                     fd_boolconfig_get,fd_boolconfig_set,&daemonize);
  fd_register_config("PIDWAIT",_("Whether to wait for the servlet PID file"),
                     fd_boolconfig_get,fd_boolconfig_set,&pidwait);
  fd_register_config("FASTFAIL",_("Threshold for daemon fastfails"),
                     fd_intconfig_get,fd_intconfig_set,&fastfail_threshold);
  fd_register_config("FASTFAIL_WAIT",
                     _("How long (secs) to wait after a fastfail"),
                     fd_intconfig_get,fd_intconfig_set,&fastfail_wait);
  fd_register_config("BACKLOG",
                     _("Number of pending connection requests allowed"),
                     fd_intconfig_get,fd_intconfig_set,&max_backlog);
  fd_register_config("MAXQUEUE",_("Max number of requests to keep queued"),
                     fd_intconfig_get,fd_intconfig_set,&max_queue);
  fd_register_config("INITCLIENTS",
                     _("Number of clients to prepare for/grow by"),
                     fd_intconfig_get,fd_intconfig_set,&init_clients);
  fd_register_config("REQTHREADS",_("Number of threads in the thread pool"),
                     fd_intconfig_get,fd_intconfig_set,&n_threads);
  /* This version is deprecated. */
  fd_register_config("NTHREADS",_("Number of threads in the thread pool"),
                     fd_intconfig_get,fd_intconfig_set,&n_threads);
  fd_register_config("MODULE",_("modules to provide in the server environment"),
                     config_get_modules,config_use_module,NULL);
  fd_register_config("FULLSCHEME",_("whether to provide full scheme interpreter"),
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
#ifdef U8_SERVER_LOG_TRANSFER
  fd_register_config("U8LOGTRANSFER",
                     _("Whether to have libu8 log all data transfers for fine-grained debugging"),
                     config_get_dtype_server_flag,config_set_dtype_server_flag,
                     (void *)(U8_SERVER_LOG_TRANSFER));
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
  fd_register_config("AUTORELOAD",
                     _("Whether to automatically reload changed files"),
                     fd_boolconfig_get,fd_boolconfig_set,&auto_reload);
  fd_register_config("NOFDDB",
                     _("Whether to disable exported FramerD DB API"),
                     fd_boolconfig_get,fd_boolconfig_set,&no_fddb);
}

static fd_lispenv init_core_env()
{
  /* This is a safe environment (e.g. a sandbox without file/io etc). */
  fd_lispenv core_env=fd_safe_working_environment();
  fd_init_fddbserv();
  fd_register_module("FDBSERV",fd_incref(fd_fdbserv_module),FD_MODULE_SAFE);
  fd_finish_module(fd_fdbserv_module);
  fd_persist_module(fd_fdbserv_module);

  /* We add some special functions */
  fd_defspecial((fdtype)core_env,"BOUND?",boundp_handler);
  fd_idefn((fdtype)core_env,fd_make_cprim0("BOOT-TIME",get_boot_time,0));
  fd_idefn((fdtype)core_env,fd_make_cprim0("UPTIME",get_uptime,0));
  fd_idefn((fdtype)core_env,fd_make_cprim0("ASYNCOK?",asyncok,0));
  fd_idefn((fdtype)core_env,
           fd_make_cprim0("SERVER-STATUS",get_server_status,0));

  return core_env;
}

static int sustain_server(pid_t grandchild,
                          u8_string server_spec,
                          fd_lispenv env);

static int fork_server(u8_string server_spec,fd_lispenv env)
{
  pid_t child, grandchild; double start=u8_elapsed_time();
  if ((foreground)&&(daemonize>0)) {
    /* This is the scenario where we stay in the foreground but
       restart automatically.  */
    if ((child=fork())) {
      if (child<0) {
        u8_log(LOG_CRIT,ServerFork,"Fork failed for %s",server_spec);
        exit(1);}
      else {
        u8_log(LOG_NOTICE,ServerFork,"Running server %s has PID %d",
               server_spec,child);
        return sustain_server(child,server_spec,env);}}
    else return launch_server(server_spec,env);}
  else if ((child=fork()))  {
    /* The grandparent waits until the parent exits and then
       waits until the .pid file has been written. */
    int count=60; double done; int status=0;
    if (child<0) {
      u8_log(LOG_CRIT,ServerFork,"Fork failed for %s\n",server_spec);
      exit(1);}
    else u8_log(LOG_WARN,ServerFork,"Initial fork spawned pid %d from %d",
                child,getpid());
#if HAVE_WAITPID
    if (waitpid(child,&status,0)<0) {
      u8_log(LOG_CRIT,ServerFork,"Fork wait failed");
      exit(1);}
    if (!(WIFEXITED(status))) {
      u8_log(LOG_CRIT,ServerFork,"First fork failed to finish");
      exit(1);}
    else if (WEXITSTATUS(status)!=0) {
      u8_log(LOG_CRIT,ServerFork,"First fork return non-zero status");
      exit(1);}
#endif
    /* If the parent has exited, we wait around for the pid_file to be created
       by our grandchild. */
    if (!(pidwait))
      u8_log(LOG_WARN,Startup,"Not waiting for PID file %s",pid_file);
    else while ((count>0)&&(!(u8_file_existsp(pid_file)))) {
      if ((count%10)==0)
        u8_log(LOG_WARN,Startup,"Waiting for PID file %s",pid_file);
      count--; sleep(1);}
    done=u8_elapsed_time();
    if (u8_file_existsp(pid_file))
      u8_log(LOG_NOTICE,Startup,"Server %s launched in %02fs",
             server_spec,done-start);
    else if (!(pidwait))
      u8_log(LOG_WARN,Startup,
             "Server %s hasn't launched after %02fs",
             server_spec,done-start);
    else u8_log(LOG_CRIT,ServerAbort,
                "Server %s hasn't launched after %02fs",
                server_spec,done-start);
    exit(0);}
  else {
    /* If we get here, we're the parent, and we start by trying to
       become session leader */
    if (setsid()==-1) {
      u8_log(LOG_CRIT,ServerAbort,
             "Process %d failed to become session leader for %s (%s)",
             getpid(),server_spec,strerror(errno));
      errno=0;
      exit(1);}
    else u8_log(LOG_INFO,ServerFork,
                "Process %d become session leader for %s",getpid(),server_spec);
    /* Now we fork again.  In the normal case, this fork (the grandchild) is
       the actual server.  If we're auto-restarting, this fork is the one which
       does the restarting. */
    if ((grandchild=fork())) {
      if (grandchild<0) {
        u8_log(LOG_CRIT,ServerAbort,"Second fork failed for %s",server_spec);
        exit(1);}
      else if (daemonize>0)
        u8_log(LOG_NOTICE,ServerFork,"Restart monitor for %s has PID %d",
               server_spec,grandchild);
      else u8_log(LOG_NOTICE,ServerFork,"Running server %s has PID %d",
                  server_spec,grandchild);
      /* This is the parent, which always exits */
      exit(0);}
    else if (daemonize>0) {
      pid_t worker;
      if ((worker=fork())) {
        if (worker<0)
          u8_log(LOG_CRIT,ServerAbort,"Worker fork failed for %s",server_spec);
        else {
          u8_log(LOG_NOTICE,ServerFork,"Running server %s has PID %d",
                 server_spec,worker);
          return sustain_server(worker,server_spec,env);}}
      else return launch_server(server_spec,env);}
    else return launch_server(server_spec,env);}
  exit(0);
}

static int sustain_server(pid_t grandchild,
                          u8_string server_spec,fd_lispenv env)
{
  u8_string ppid_filename=fd_runbase_filename(".ppid");
  FILE *f=fopen(ppid_filename,"w");
  int status=-1, sleepfor=daemonize;
  tweak_exename("fdserv",2,'x');
  sustaining=1;
  if (f) {
    fprintf(f,"%ld\n",(long)getpid());
    fclose(f);
    u8_free(ppid_filename);}
  else {
    u8_log(LOG_WARN,"CantWritePPID","Couldn't write ppid file %s",
           ppid_filename);
    u8_free(ppid_filename);}
  last_launch=time(NULL);
  /* Don't try to catch an error here */
  if (sleepfor>60) sleepfor=60;
  errno=0;
  /* Update the global variable with our current dependent grandchild */
  dependent=grandchild;
  /* Setup atexit and signal handlers to kill our dependent when we're
     gone. */
  u8_log(LOG_WARN,ServerLoop,"Monitoring %s pid=%d",server_spec,grandchild);
  atexit(kill_dependent_onexit);
#ifdef SIGHUP
  sigaction(SIGHUP,&sigaction_ignore,NULL);
#endif
#ifdef SIGTERM
  sigaction(SIGTERM,&sigaction_abraham,NULL);
#endif
#ifdef SIGQUIT
  sigaction(SIGQUIT,&sigaction_abraham,NULL);
#endif
  while ((sustaining)&&(waitpid(grandchild,&status,0))) {
    time_t now=time(NULL);
    if (WIFSIGNALED(status))
      u8_log(LOG_WARN,ServerRestart,
             "Server %s(%d) terminated on signal %d",
             server_spec,grandchild,WTERMSIG(status));
    else if (WIFEXITED(status))
      u8_log(LOG_NOTICE,ServerRestart,
             "Server %s(%d) terminated normally with status %d",
             server_spec,grandchild,status);
    else continue;
    if (dependent<0) {
      u8_log(LOG_WARN,ServerAbort,
             "Terminating restart process for %s",server_spec);
      exit(0);}
    if ((now-last_launch)<fastfail_threshold) {
      u8_log(LOG_CRIT,ServerLoop,
             "FDServer %s fast-failed after %d seconds, pausing %d seconds",
             server_spec,now-last_launch,fastfail_wait);
      sleep(fastfail_wait);}
    else if (sleepfor>0) sleep(sleepfor);
    else {}
    last_launch=time(NULL);
    if ((grandchild=fork())) {
      u8_log(LOG_NOTICE,ServerRestart,
             "Server %s restarted with pid %d",
             server_spec,grandchild);
      dependent=grandchild;
      continue;}
    else return launch_server(server_spec,env);}
  exit(0);
  return 0;
}

static void write_state_files(void);

static int launch_server(u8_string server_spec,fd_lispenv core_env)
{
#ifdef SIGHUP
  sigaction(SIGHUP,&sigaction_shutdown,NULL);
#endif
  tweak_exename("fdxerv",2,'s');
  if (u8_file_existsp(server_spec)) {
    /* The source file is loaded into a full (non sandbox environment).
       It's exports are then exposed through the server. */
    u8_string source_file=u8_abspath(server_spec,NULL);
    fd_lispenv env=working_env=fd_working_environment();
    fdtype result=fd_load_source(source_file,env,NULL);
    if (FD_TROUBLEP(result)) {
      u8_exception e=u8_erreify();
      U8_OUTPUT out; U8_INIT_STATIC_OUTPUT(out,512);
      fd_print_exception(&out,e);
      fd_print_backtrace(&out,e,80);
      fputs(out.u8_outbuf,stderr);
      u8_free(out.u8_outbuf);
      u8_free(source_file);
      fd_decref(result);
      fd_decref((fdtype)env);
      return -1;}
    else {
      fdtype startup_proc=fd_symeval(fd_intern("STARTUP"),env);
      shutdown_proc=fd_symeval(fd_intern("SHUTDOWN"),env);
      fd_decref(result); result=FD_VOID;
      /* If the init file did any exporting, expose those exports to clients.
         Otherwise, expose all the definitions in the init file.  Note that the clients
         won't be able to get at the unsafe "empowered" environment but that the
         procedures defined are closed in that environment. */
      if (FD_HASHTABLEP(env->exports))
        server_env=fd_make_env(fd_incref(env->exports),exposed_environment);
      else server_env=fd_make_env(fd_incref(env->bindings),exposed_environment);
      if (fullscheme==0) {
        /* Cripple the core environment if requested */
        fd_decref((fdtype)(core_env->parent));
        core_env->parent=NULL;}
      if (FD_VOIDP(startup_proc)) {}
      else {
        FD_DO_CHOICES(p,startup_proc) {
          fdtype result=fd_apply(p,0,NULL);
          if (FD_ABORTP(result)) {
            u8_exception ex=u8_erreify(), root=ex;
            int old_maxelts=fd_unparse_maxelts, old_maxchars=fd_unparse_maxchars;
            U8_OUTPUT out; U8_INIT_STATIC_OUTPUT(out,512);
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
          else fd_decref(result);}}}}
  else server_env=exposed_environment;

  return run_server(server_spec);
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
  fd_register_config("PORT",_("port or port@host to listen on"),
                     config_get_ports,config_serve_port,NULL);
  if (n_ports<=0) {
    u8_log(LOG_WARN,NoServers,"No servers configured, exiting...");
    exit(-1);
    return -1;}
  write_state_files();
  u8_message("beingmeta FramerD, (C) beingmeta 2004-2016, all rights reserved");
  u8_log(LOG_NOTICE,ServerStartup,
         "FramerD (%s) fdserver %s running, %d/%d pools/indices, %d ports",
         FRAMERD_REVISION,server_spec,fd_n_pools,
         fd_n_primary_indices+fd_n_secondary_indices,n_ports);
  u8_log(LOG_NOTICE,ServerStartup,"Serving on %d sockets",n_ports);
  u8_server_loop(&dtype_server); normal_exit=1;
  u8_log(LOG_NOTICE,ServerShutdown,"Exited server loop");
  exit(0);
}

static void write_state_files()
{
  FILE *f=u8_fopen(pid_file,"w"); if (f) {
    fprintf(f,"%d\n",getpid());
    fclose(f);}
  else {
    u8_log(LOG_WARN,u8_strerror(errno),
           "Couldn't write PID %d to '%s'",
           getpid(),pid_file);
    errno=0;}
  /* Write the NID file */
  f=u8_fopen(nid_file,"w"); if (f) {
    if (dtype_server.n_servers) {
      int i=0; while (i<dtype_server.n_servers) {
        fprintf(f,"%s\n",dtype_server.server_info[i].idstring);
        i++;}}
    fclose(f);}
  else {
    u8_log(LOG_WARN,u8_strerror(errno),
           "Couldn't write NID info to '%s'",
           pid_file);
    if (dtype_server.n_servers) {
      int i=0; while (i<dtype_server.n_servers) {
        u8_log(LOG_NOTICE,Startup,"%s\n",
               dtype_server.server_info[i].idstring);
        i++;}}
    else u8_log(LOG_NOTICE,Startup,"temp.socket\n");
    errno=0;}
  state_files_written=u8_microtime();
  atexit(cleanup_state_files);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
