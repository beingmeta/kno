/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2013 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#include "framerd/dtype.h"
#include "framerd/tables.h"
#include "framerd/numbers.h"
#include "framerd/eval.h"
#include "framerd/fddb.h"
#include "framerd/fdweb.h"
#include "framerd/ports.h"
#include "framerd/fileprims.h"

#include <libu8/libu8.h>
#include <libu8/libu8io.h>
#include <libu8/u8timefns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8netfns.h>
#include <libu8/u8srvfns.h>
#include <libu8/u8rusage.h>
#include <libu8/u8stdio.h>

#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>
#include <signal.h>
#include <math.h>

#include "fdversion.h"

/* This is the size of file to return all at once. */
#define FD_FILEBUF_MAX (256*256)

static u8_condition Startup=_("FDSERV Startup");

FD_EXPORT void fd_init_fdweb(void);
FD_EXPORT void fd_init_texttools(void);
FD_EXPORT void fd_init_tagger(void);
FD_EXPORT int fd_init_fddbserv(void);

#include "webcommon.h"

#define nobytes(in,nbytes) (FD_EXPECT_FALSE(!(fd_needs_bytes(in,nbytes))))
#define havebytes(in,nbytes) (FD_EXPECT_TRUE(fd_needs_bytes(in,nbytes)))

#define HTML_UTF8_CTYPE_HEADER "Content-type: text/html; charset=utf-8\r\n\r\n"

/* This lets the u8_server loop do I/O buffering to keep threads from
   waiting on I/O. */
static int async_mode=1;

/* Logging declarations */
static FILE *statlog=NULL;
static double overtime=0;
static int stealsockets=0;

/* Tracking ports */
static u8_string server_id=NULL;
static u8_string *ports=NULL;
static int n_ports=0, max_ports=0;
static u8_mutex server_port_lock;

/* When the server started, used by UPTIME */
static struct U8_XTIME boot_time;

/* This is how old a socket file needs to be to be deleted as 'leftover' */
#define FD_LEFTOVER_AGE 30
static int ignore_leftovers=0;

typedef struct FD_WEBCONN {
  U8_CLIENT_FIELDS;
  fdtype cgidata;
  struct FD_DTYPE_STREAM in;
  struct U8_OUTPUT out;} FD_WEBCONN;
typedef struct FD_WEBCONN *fd_webconn;

static fd_lispenv server_env;
struct U8_SERVER fdwebserver;
static int server_running=0;

/* This environment is the parent of all script/page environments spun off
   from this server. */
static int init_clients=64, servlet_threads=8, max_queue=256, max_conn=0;
/* This is the backlog of connection requests not transactions.
   It is passed as the argument to listen() */
static int max_backlog=-1;

/* This is how long (μs) to wait for clients to finish when shutting down the
   server.  Note that the server stops listening for new connections right
   away, so we can start another server.  */
static int shutdown_grace=30000000; /* 30 seconds */

/* STATLOG config */

/* The statlog is a plain text (UTF-8) file which contains server
   connection/thread information.  */

static u8_string statlogfile;
static long long last_status=0;
static long status_interval=-1;

static int statlog_set(fdtype var,fdtype val,void *data)
{
  if (FD_STRINGP(val)) {
    u8_string filename=FD_STRDATA(val);
    fd_lock_mutex(&log_lock);
    if (statlog) {
      fclose(statlog); statlog=NULL;
      u8_free(statlogfile); statlogfile=NULL;}
    statlogfile=u8_abspath(filename,NULL);
    statlog=u8_fopen_locked(statlogfile,"a");
    if (statlog) {
      u8_string tmp;
      tmp=u8_mkstring("# Log open %*lt for %s\n",u8_sessionid());
      fputs(tmp,statlog);
      fd_unlock_mutex(&log_lock);
      u8_free(tmp);
      return 1;}
    else {
      u8_log(LOG_WARN,"no file","Couldn't open %s",statlogfile);
      fd_unlock_mutex(&log_lock);
      u8_free(statlogfile); statlogfile=NULL;
      return 0;}}
  else if (FD_FALSEP(val)) {
    fd_lock_mutex(&log_lock);
    if (statlog) {fclose(statlog); statlog=NULL;}
    fd_unlock_mutex(&log_lock);
    return 0;}
  else return fd_reterr
	 (fd_TypeError,"config_set_statlog",u8_strdup(_("string")),val);
}

static fdtype statlog_get(fdtype var,void *data)
{
  if (statlog)
    return fdtype_string(statlogfile);
  else return FD_FALSE;
}

static int statinterval_set(fdtype var,fdtype val,void *data)
{
  if (FD_FIXNUMP(val)) {
    int intval=FD_FIX2INT(val);
    if (intval>=0)  status_interval=intval*1000;
    else {
      return fd_reterr
	(fd_TypeError,"config_set_statinterval",
	 u8_strdup(_("fixnum")),val);}}
  else return fd_reterr
	 (fd_TypeError,"config_set_statinterval",
	  u8_strdup(_("fixnum")),val);
  return 1;
}

static fdtype statinterval_get(fdtype var,void *data)
{
  if (status_interval<0) return FD_FALSE;
  else return FD_FIX2INT(status_interval);
}

#define STATUS_LINE1 "[%*t][%f] %d/%d/%d busy/waiting/clients/threads\n"
#define STATUS_LINE2 "[%*t][%f] avg(wait)=%f(%d); avg(run)=%f(%d)\n"
#define STATUS_LINE3 "[%*t][%f] waiting (n=%lld) min=%lld max=%lld avg=%f\n"
#define STATUS_LINE4 "[%*t][%f] running (n=%lld) min=%lld max=%lld avg=%f\n"
#define STATUS_LINEXN "[%*t][%f] %s mean=%0.2fus max=%lldus sd=%0.2f (n=%d)\n"
#define STATUS_LINEX "%s mean=%0.2fus max=%lldus sd=%0.2f (n=%d)"

#define stdev(v,v2,n) \
  ((double)(sqrt((((double)v2)/((double)n))-			\
		 ((((double)v)/((double)n))*(((double)v)/((double)n))))))
#define getmean(v,n) (((double)v)/((double)n))

static void output_stats(struct U8_SERVER_STATS *stats,FILE *logto)
{
  double elapsed=u8_elapsed_time();
  if (stats->tcount>0) {
    u8_fprintf(logto,STATUS_LINEXN,elapsed,"busy",
	       getmean(stats->tsum,stats->tcount),
	       stats->tmax,
	       stdev(stats->tsum,stats->tsum2,stats->tcount),
	       stats->tcount);
    u8_log(LOG_INFO,"fdserv",STATUS_LINEX,"busy",
	   getmean(stats->tsum,stats->tcount),
	   stats->tmax,
	   stdev(stats->tsum,stats->tsum2,stats->tcount),
	   stats->tcount);}
  
  if (stats->qcount>0) {
    u8_fprintf(logto,STATUS_LINEXN,elapsed,"queued",
	       getmean(stats->qsum,stats->qcount),stats->qmax,
	       stdev(stats->qsum,stats->qsum2,stats->qcount),
	       stats->qcount);
    u8_log(LOG_INFO,"fdserv",STATUS_LINEX,"queued",
	   getmean(stats->qsum,stats->qcount),stats->qmax,
	   stdev(stats->qsum,stats->qsum2,stats->qcount),
	   stats->qcount);}

  if (stats->rcount>0) {
    u8_fprintf(logto,STATUS_LINEXN,elapsed,"reading",
	       getmean(stats->rsum,stats->rcount),stats->rmax,
	       stdev(stats->rsum,stats->rsum2,stats->rcount),
	       stats->rcount);
    u8_log(LOG_INFO,"fdserv",STATUS_LINEX,"reading",
	   getmean(stats->rsum,stats->rcount),stats->rmax,
	   stdev(stats->rsum,stats->rsum2,stats->rcount),
	   stats->rcount);}

  if (stats->wcount>0) {
    u8_fprintf(logto,STATUS_LINEXN,elapsed,"writing",
	       getmean(stats->wsum,stats->wcount),stats->wmax,
	       stdev(stats->wsum,stats->wsum2,stats->wcount),
	       stats->wcount);
    u8_log(LOG_INFO,"fdserv",STATUS_LINEX,"writing",
	   getmean(stats->wsum,stats->wcount),stats->wmax,
	   stdev(stats->wsum,stats->wsum2,stats->wcount),
	   stats->wcount);}

  if (stats->xcount>0) {
    u8_fprintf(logto,STATUS_LINEXN,elapsed,"running",
	       getmean(stats->xsum,stats->xcount),stats->xmax,
	       stdev(stats->xsum,stats->xsum2,stats->xcount),
	       stats->xcount);
    u8_log(LOG_INFO,"fdserv",STATUS_LINEX,"running",
	   getmean(stats->xsum,stats->xcount),stats->xmax,
	   stdev(stats->xsum,stats->xsum2,stats->xcount),
	   stats->xcount);}
}

static void report_status()
{
  FILE *logto=statlog;
  struct U8_SERVER_STATS stats;
  double elapsed=u8_elapsed_time();
  if (!(logto)) logto=stderr;
  u8_fprintf(logto,STATUS_LINE1,elapsed,
	     fdwebserver.n_busy,fdwebserver.n_queued,
	     fdwebserver.n_clients,fdwebserver.n_threads);
  u8_log(LOG_INFO,"fdserv",STATUS_LINE1,elapsed,
	 fdwebserver.n_busy,fdwebserver.n_queued,
	 fdwebserver.n_clients,fdwebserver.n_threads);

  u8_fprintf(logto,"Current statistics\n");
  u8_server_curstats(&fdwebserver,&stats);
  output_stats(&stats,logto);

  u8_fprintf(logto,"Live statistics\n");
  u8_server_livestats(&fdwebserver,&stats);
  output_stats(&stats,logto);

  u8_fprintf(logto,"Aggregate statistics\n");
  u8_log(LOG_INFO,"fdserv","Aggregate statistics");
  
  u8_server_statistics(&fdwebserver,&stats);
  output_stats(&stats,logto);

  if (statlog) fflush(statlog);
}

static fdtype servlet_status()
{
  fdtype result=fd_init_slotmap(NULL,0,NULL);
  struct U8_SERVER_STATS stats, livestats, curstats;

  fd_store(result,fd_intern("NTHREADS"),FD_INT2DTYPE(fdwebserver.n_threads));
  fd_store(result,fd_intern("NQUEUED"),FD_INT2DTYPE(fdwebserver.n_queued));
  fd_store(result,fd_intern("NBUSY"),FD_INT2DTYPE(fdwebserver.n_busy));
  fd_store(result,fd_intern("NCLIENTS"),FD_INT2DTYPE(fdwebserver.n_clients));
  fd_store(result,fd_intern("TOTALTRANS"),FD_INT2DTYPE(fdwebserver.n_trans));
  fd_store(result,fd_intern("TOTALCONN"),FD_INT2DTYPE(fdwebserver.n_accepted));

  u8_server_statistics(&fdwebserver,&stats);
  u8_server_livestats(&fdwebserver,&livestats);
  u8_server_curstats(&fdwebserver,&curstats);

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

/* Writing the PID file */

static int check_pid_file(char *sockname)
{
  int fd; u8_string dir=NULL; char buf[128]; 
  int len=strlen(sockname);
  char *dot=strchr(sockname,'.');
  if (dot) *dot='\0';
  if (sockname[0]!='/') dir=u8_getcwd();
  pidfile=u8_string_append(dir,((dir)?"/":""),sockname,".pid",NULL);
  if (dot) *dot='.'; if (dir) u8_free(dir);
  fd=open(pidfile,O_WRONLY|O_CREAT|O_EXCL,644);
  if (fd<0) {
    struct stat fileinfo;
    int rv=stat(pidfile,&fileinfo);
    if (rv<0) {
      u8_log(LOG_CRIT,"Can't write file",
	     "Couldn't write file","Couldn't write PID file %s",pidfile);
      return 0;}
    else if ((!(ignore_leftovers))&&
	     (((time(NULL))-(fileinfo.st_mtime))<FD_LEFTOVER_AGE)) {
      u8_log(LOG_CRIT,"Race Condition",
	     "Current pidfile (%s) too young to replace",
	     pidfile);
      return 0;}
    else {
      remove(pidfile);
      fd=open(pidfile,O_WRONLY|O_CREAT|O_EXCL,644);
      if (fd<0) {
	u8_log(LOG_CRIT,"Couldn't write file",
	       "Couldn't write PID file %s",pidfile);
	return 0;}}}
  sprintf(buf,"%d",getpid());
  u8_writeall(fd,buf,strlen(buf));
  close(fd);
  return 1;
}

/* Document generation */

#define write_string(sock,string) u8_writeall(sock,string,strlen(string))

static int output_content(fd_webconn ucl,fdtype content)
{
  if (FD_STRINGP(content)) {
    u8_writeall(ucl->socket,FD_STRDATA(content),FD_STRLEN(content));
    return FD_STRLEN(content);}
  else if (FD_PACKETP(content)) {
    u8_writeall(ucl->socket,FD_PACKET_DATA(content),FD_PACKET_LENGTH(content));
    return FD_PACKET_LENGTH(content);}
  else return 0;
}

/* Configuring u8server fields */

static fdtype config_get_u8server_flag(fdtype var,void *data)
{
  fd_ptrbits bigmask=(fd_ptrbits)data;
  unsigned int mask=(unsigned int)(bigmask&0xFFFFFFFF);
  unsigned int flags=fdwebserver.flags;
  if ((flags)&(mask)) return FD_TRUE; else return FD_FALSE;
}

static int config_set_u8server_flag(fdtype var,fdtype val,void *data)
{
  fd_ptrbits bigmask=(fd_ptrbits)data;
  unsigned int mask=(bigmask&0xFFFFFFFF);
  unsigned int flags=fdwebserver.flags;
  unsigned int *flagsp=&(fdwebserver.flags);
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
	return fd_reterr(fd_TypeError,"setserverflag","boolean value",val);}
      else u8_log(LOG_WARN,"SERVERFLAG",
		  "Unfamiliar boolean setting %s, assuming %s",
		  s,((guess)?("true"):("false")));
      if (!(guess<0)) bool=guess;}
    if (bool) *flagsp=flags|mask;
    else *flagsp=flags&(~(mask));}
  else *flagsp=flags|mask;
  return 1;
}

/* Running the server */

static u8_client simply_accept(u8_server srv,u8_socket sock,
			       struct sockaddr *addr,size_t len)
{
  /* We could do access control here. */
  fd_webconn consed=(fd_webconn)
    u8_client_init(NULL,sizeof(FD_WEBCONN),addr,len,sock,srv);
  fd_init_dtype_stream(&(consed->in),sock,4096);
  U8_INIT_OUTPUT(&(consed->out),8192);
  u8_set_nodelay(sock,1);
  consed->cgidata=FD_VOID;
  return (u8_client) consed;
}

static int max_error_depth=128;

static int webservefn(u8_client ucl)
{
  fdtype proc=FD_VOID, result=FD_VOID, onerror=FD_VOID;
  fdtype cgidata=FD_VOID, path=FD_VOID, precheck;
  fdtype content=FD_VOID, retfile=FD_VOID;
  fd_lispenv base_env=NULL;
  fd_webconn client=(fd_webconn)ucl;
  int write_headers=1, close_html=0, resuming=0;
  double start_time, setup_time, parse_time, exec_time, write_time;
  struct FD_THREAD_CACHE *threadcache=NULL;
  struct rusage start_usage, end_usage;
  double start_load[]={-1,-1,-1}, end_load[]={-1,-1,-1};
  int forcelog=0, retval=0;
  size_t http_len=0, head_len=0, content_len=0;
  fd_dtype_stream stream=&(client->in);
  u8_output outstream=&(client->out);
  if ((status_interval>=0)&&(u8_microtime()>last_status+status_interval))
    report_status();
  int async=((async_mode)&&((client->server->flags)&U8_SERVER_ASYNC));
  int return_code=0, buffered=0;
  /* Reset the streams */
  outstream->u8_outptr=outstream->u8_outbuf;
  stream->ptr=stream->end=stream->start;
  /* Handle async reading (where the server buffers incoming and outgoing data) */
  if ((client->reading>0)&&(u8_client_finished(ucl))) { 
    /* We got the whole payload, set up the stream
       for reading it without waiting.  */
    stream->end=stream->start+client->len;}
  else if (client->reading>0)
    /* We shouldn't get here, but just in case.... */
    return 1; 
  else if ((client->writing>0)&&(u8_client_finished(ucl)))
    /* All done */
    return 0;
  else if (client->writing>0) 
    /* We shouldn't get here, but just in case.... */
    return 1;
  else {
    /* We read a little to see if we can just queue up what we
       need. */
    fd_dts_start_read(stream);
    if ((async)&&
	(havebytes((fd_byte_input)stream,1))&&
	((*(stream->ptr))==dt_block)) {
	  /* If we can be asynchronous, let's try */
	  int dtcode=fd_dtsread_byte(stream);
	  int nbytes=fd_dtsread_4bytes(stream);
	  if (fd_has_bytes(stream,nbytes)) {
	    /* We can execute without waiting */}
	  else {
	    int need_size=5+nbytes;
	    /* Allocate enough space for what we need to read */
	    if (stream->bufsiz<need_size) {
	      fd_grow_byte_input((fd_byte_input)stream,need_size);
	      stream->bufsiz=need_size;}
	    /* Set up the client for async input */
	    if (u8_client_read(ucl,stream->start,5+nbytes,
			       (stream->end-stream->start))) { 
	      /* We got the whole payload, set up the stream
		 for reading it without waiting.  */
	      stream->end=stream->start+client->len;}
	    else return 1;}}
    else {}}
  /* Do this ASAP to avoid session leakage */
  fd_reset_threadvars();
  /* Clear outstanding errors from the last session */
  fd_clear_errors(1);
  /* Begin with the new request */
  start_time=u8_elapsed_time();
  getloadavg(start_load,3);
  u8_getrusage(RUSAGE_SELF,&start_usage);
  /* Start doing your stuff */
  if (fd_update_file_modules(0)<0) {
    u8_condition c; u8_context cxt; u8_string details=NULL;
    fdtype irritant;
    setup_time=parse_time=u8_elapsed_time();
    if (fd_poperr(&c,&cxt,&details,&irritant)) 
      proc=fd_err(c,cxt,details,irritant);
    if (details) u8_free(details); fd_decref(irritant);
    setup_time=u8_elapsed_time();
    cgidata=fd_dtsread_dtype(stream);}
  else if (update_preloads()<0) {
    u8_condition c; u8_context cxt; u8_string details=NULL;
    fdtype irritant;
    setup_time=parse_time=u8_elapsed_time();
    if (fd_poperr(&c,&cxt,&details,&irritant)) 
      proc=fd_err(c,cxt,details,irritant);
    if (details) u8_free(details); fd_decref(irritant);
    setup_time=u8_elapsed_time();
    cgidata=fd_dtsread_dtype(stream);}
  else {
    /* This is where we usually end up, when all the updates
       and preloads go without a hitch. */
    setup_time=u8_elapsed_time();
    /* Now we extract arguments and figure out what we're going to
       run to respond to the request. */
    cgidata=fd_dtsread_dtype(stream);
    if (cgidata==FD_EOD) {
      if (traceweb>0)
	u8_log(LOG_NOTICE,"FDSERV/webservefn","Client %s (sock=%d) closing",
	       client->idstring,client->socket);
      u8_client_close(ucl);
      return -1;}
    else if (!(FD_TABLEP(cgidata))) {
      u8_log(LOG_CRIT,"FDSERV/webservefn",
	     "Bad fdserv request on client %s (sock=%d), closing",
	     client->idstring,client->socket);
      u8_client_close(ucl);
      return -1;}
    else {}
    if (docroot) webcommon_adjust_docroot(cgidata,docroot);
    path=fd_get(cgidata,script_filename,FD_VOID);
    if (traceweb>0) {
      fdtype referer=fd_get(cgidata,referer_symbol,FD_VOID);
      fdtype remote=fd_get(cgidata,remote_info,FD_VOID);
      fdtype uri=fd_get(cgidata,uri_symbol,FD_VOID);
      if ((FD_STRINGP(uri)) &&  (FD_STRINGP(referer)) && (FD_STRINGP(remote)))
	u8_log(LOG_NOTICE,
	       "REQUEST","Handling request for %s from %s by %s, load=%f/%f/%f",
	       FD_STRDATA(uri),FD_STRDATA(referer),FD_STRDATA(remote),
	       start_load[0],start_load[1],start_load[2]);
      else if ((FD_STRINGP(uri)) &&  (FD_STRINGP(remote)))
	u8_log(LOG_NOTICE,
	       "REQUEST","Handling request for %s by %s, load=%f/%f/%f",
	       FD_STRDATA(uri),FD_STRDATA(remote),
	       start_load[0],start_load[1],start_load[2]);
      else if ((FD_STRINGP(uri)) &&  (FD_STRINGP(referer)))
	u8_log(LOG_NOTICE,
	       "REQUEST","Handling request for %s from %s, load=%f/%f/%f",
	       FD_STRDATA(uri),FD_STRDATA(referer),
	       start_load[0],start_load[1],start_load[2]);
      else if (FD_STRINGP(uri))
	u8_log(LOG_NOTICE,"REQUEST","Handling request for %s",FD_STRDATA(uri));
      fd_decref(referer);
      fd_decref(uri);}
    /* This is what we'll execute, be it a procedure or FDXML */
    proc=getcontent(path);}
  /* This is where we parse all the CGI variables, etc */
  fd_parse_cgidata(cgidata);
  parse_time=u8_elapsed_time();
  if ((reqlog) || (urllog) || (trace_cgidata))
    dolog(cgidata,FD_NULL,NULL,-1,parse_time-start_time);
  u8_set_default_output(outstream);
  fd_use_reqinfo(cgidata);
  fd_thread_set(browseinfo_symbol,FD_EMPTY_CHOICE);
  if (!(FD_ABORTP(proc))) 
    precheck=run_preflight();
  if (FD_ABORTP(proc)) result=fd_incref(proc);
  else if (!((FD_FALSEP(precheck))||
	     (FD_VOIDP(precheck))||
	     (FD_EMPTY_CHOICEP(precheck))))
    result=precheck;
  else if (FD_PRIM_TYPEP(proc,fd_sproc_type)) {
    struct FD_SPROC *sp=FD_GET_CONS(proc,fd_sproc_type,fd_sproc);
    if (traceweb>1)
      u8_log(LOG_NOTICE,"START","Handling %q with Scheme procedure %q",
	     path,proc);
    base_env=sp->env;
    threadcache=checkthreadcache(sp->env);
    result=fd_cgiexec(proc,cgidata);}
  else if ((FD_PAIRP(proc))&&
	   (FD_PRIM_TYPEP((FD_CAR(proc)),fd_sproc_type))) {
    struct FD_SPROC *sp=FD_GET_CONS(FD_CAR(proc),fd_sproc_type,fd_sproc);
    if (traceweb>1)
      u8_log(LOG_NOTICE,"START","Handling %q with Scheme procedure %q",
	     path,proc);
    threadcache=checkthreadcache(sp->env);
    base_env=sp->env;
    result=fd_cgiexec(FD_CAR(proc),cgidata);}
  else if (FD_PAIRP(proc)) {
    fdtype lenv=FD_CDR(proc), setup_proc=FD_VOID;
    fd_lispenv base=
      ((FD_PTR_TYPEP(lenv,fd_environment_type)) ?
       (FD_GET_CONS(FD_CDR(proc),fd_environment_type,fd_environment)) :
       (NULL));
    fd_lispenv runenv=fd_make_env(fd_incref(cgidata),base);
    base_env=base;
    if (base) fd_load_latest(NULL,base,NULL);
    threadcache=checkthreadcache(base);
    if (traceweb>1)
      u8_log(LOG_NOTICE,"START","Handling %q with template",path);
    setup_proc=fd_symeval(setup_symbol,base);
    /* Run setup procs */
    if (FD_VOIDP(setup_proc)) {}
    else if (FD_CHOICEP(setup_proc)) {
      FD_DO_CHOICES(proc,setup_proc)
	if (FD_APPLICABLEP(proc)) {
	  fdtype v=fd_apply(proc,0,NULL);
	  fd_decref(v);}}
    else if (FD_APPLICABLEP(setup_proc)) {
      fdtype v=fd_apply(setup_proc,0,NULL);
      fd_decref(v);}
    fd_decref(setup_proc);
    /* We assume that the XML contains headers, so we won't add them. */
    write_headers=0;
    fd_output_xml_preface(&(client->out),cgidata);
    if (FD_PAIRP(FD_CAR(proc))) {
      FD_DOLIST(expr,FD_CAR(proc)) {
	fd_decref(result);
	result=fd_xmleval(&(client->out),expr,runenv);
	if (FD_ABORTP(result)) break;}}
    else result=fd_xmleval(&(client->out),FD_CAR(proc),runenv);
    fd_decref((fdtype)runenv);}
  exec_time=u8_elapsed_time();
  /* We're now done with all the core computation. */
  if (!(FD_TROUBLEP(result))) {
    /* See if the content or retfile will get us into trouble. */
    content=fd_get(cgidata,content_slotid,FD_VOID);
    retfile=((FD_VOIDP(content))?
	     (fd_get(cgidata,retfile_slotid,FD_VOID)):
	     (FD_VOID));
    if ((!(FD_VOIDP(content)))&&
	(!((FD_STRINGP(content))||(FD_PACKETP(content))))) {
      fd_decref(result);
      result=fd_err(fd_TypeError,"fdserv_content","string or packet",
		    content);}
    if ((!(FD_VOIDP(retfile)))&&
	((!(FD_STRINGP(retfile)))||
	 (!(u8_file_existsp(FD_STRDATA(retfile)))))) {
      fd_decref(result);
      result=fd_err(u8_CantOpenFile,"fdserv_retfile","existing filename",
		    retfile);}}
  if (!(FD_TROUBLEP(result))) u8_set_default_output(NULL);
  if (FD_TROUBLEP(result)) {
    u8_exception ex=u8_current_exception, exscan=ex;
    fdtype errorpage=((base_env)?
		      (fd_symeval(errorpage_symbol,base_env)):
		      (FD_VOID));
    int depth=0;
    if (((FD_VOIDP(errorpage))||(errorpage==FD_UNBOUND))&&
	(!(FD_VOIDP(errpage)))) {
      fd_incref(errpage); errorpage=errpage;}
    while ((exscan)&&(depth<max_error_depth)) {
      u8_condition excond=exscan->u8x_cond;
      u8_context excxt=((exscan->u8x_context) ? (exscan->u8x_context) :
			((u8_context)"somewhere"));
      u8_context exdetails=((exscan->u8x_details) ? (exscan->u8x_details) :
			    ((u8_string)"no more details"));
      fdtype irritant=fd_exception_xdata(exscan);
      if (FD_STRINGP(path))
	u8_log(LOG_ERR,excond,"Unexpected error \"%m \" for %s:@%s (%s)",
	       excond,FD_STRDATA(path),excxt,exdetails);
      else u8_log(LOG_ERR,excond,"Unexpected error \"%m \" %s:@%s (%s)",
	       excond,excxt,exdetails);
      exscan=exscan->u8x_prev; depth++;}
    if (FD_APPLICABLEP(errorpage)) {
      if (outstream->u8_outptr>outstream->u8_outbuf) {
	fdtype output=fd_make_string
	  (NULL,outstream->u8_outptr-outstream->u8_outbuf,
	   outstream->u8_outbuf);
	/* Save the output to date on the request */
	fd_store(cgidata,output_symbol,output);
	fd_decref(output);}
      outstream->u8_outptr=outstream->u8_outbuf;
      /* Apply the error page object */
      result=fd_apply(errorpage,0,NULL);
      if (FD_ABORTP(result)) {
	ex=u8_current_exception; exscan=ex; depth=0;
	while ((exscan)&&(depth<max_error_depth)) {
	  u8_condition excond=exscan->u8x_cond;
	  u8_context excxt=((exscan->u8x_context) ? (exscan->u8x_context) :
			    ((u8_context)"somewhere"));
	  u8_context exdetails=((exscan->u8x_details) ? (exscan->u8x_details) :
				((u8_string)"no more details"));
	  fdtype irritant=fd_exception_xdata(exscan);
	  if (FD_STRINGP(path))
	    u8_log(LOG_ERR,excond,
		   "Unexpected recursive error \"%m \" for %s:@%s (%s)",
		   excond,FD_STRDATA(path),excxt,exdetails);
	  else u8_log(LOG_ERR,excond,
		      "Unexpected recursive error \"%m \" %s:@%s (%s)",
		      excond,excxt,exdetails);
	  exscan=exscan->u8x_prev; depth++;}
	fd_decref(errorpage); errorpage=FD_VOID;}}
    if (!(FD_TROUBLEP(result))) {}
    else if ((FD_STRINGP(errorpage))&&
	(strstr(FD_STRDATA(errorpage),"\n")!=NULL)) {
      /* Assume that the error page is a string of HTML */
      ex=u8_erreify();
      http_len=http_len+strlen(HTML_UTF8_CTYPE_HEADER);
      write_string(client->socket,HTML_UTF8_CTYPE_HEADER);
      write_string(client->socket,FD_STRDATA(errpage));}
    else if (FD_STRINGP(errorpage)) {
      /* This should check for redirect URLs, but for now it
	 just dumps the error page as plain text.  */
      ex=u8_erreify();
      http_len=http_len+
	strlen("Content-type: text/plain; charset=utf-8\r\n\r\n");
      write_string(client->socket,
		   "Content-type: text/plain; charset=utf-8\r\n\r\n");
      write_string(client->socket,FD_STRDATA(errpage));}
    else if ((webdebug)||
	     ((weballowdebug)&&(fd_req_test(webdebug_symbol,FD_VOID)))) {
      http_len=http_len+
	strlen("Content-type: text/html; charset=utf-8\r\n\r\n");
      write_string(client->socket,
		   "Content-type: text/html; charset=utf-8\r\n\r\n");
      fd_xhtmldebugpage(&(client->out),ex);}
    else {
      http_len=http_len+
	strlen("Content-type: text/html; charset=utf-8\r\n\r\n");
      write_string(client->socket,
		   "Content-type: text/html; charset=utf-8\r\n\r\n");
      fd_xhtmlerrorpage(&(client->out),ex);}
    u8_free_exception(ex,1);
    if ((reqlog) || (urllog) || (trace_cgidata))
      dolog(cgidata,result,outstream->u8_outbuf,
	    outstream->u8_outptr-outstream->u8_outbuf,
	    u8_elapsed_time()-start_time);
    content_len=content_len+(outstream->u8_outptr-outstream->u8_outbuf);
    /* We do a hanging write in this, hopefully not common case */
    u8_writeall(client->socket,outstream->u8_outbuf,
		outstream->u8_outptr-outstream->u8_outbuf);
    return_code=-1;
    fd_decref(errorpage);
    /* And close the client for good measure */
    u8_client_close(ucl);}
  else {
    U8_OUTPUT httphead, htmlhead; int tracep;
    fdtype traceval=fd_get(cgidata,tracep_slotid,FD_VOID);
    if (FD_VOIDP(traceval)) tracep=0; else tracep=1;
    U8_INIT_OUTPUT(&httphead,1024); U8_INIT_OUTPUT(&htmlhead,1024);
    fd_output_http_headers(&httphead,cgidata);
    if ((FD_VOIDP(content))&&(FD_VOIDP(retfile))) {
      char clen_header[128]; u8_byte *bundle;
      size_t bundle_len=0;
      if (write_headers) {
	close_html=fd_output_xhtml_preface(&htmlhead,cgidata);
	head_len=(htmlhead.u8_outptr-htmlhead.u8_outbuf);
	if (close_html) u8_puts(outstream,"\n</body>\n</html>\n");}
      content_len=head_len+(outstream->u8_outptr-outstream->u8_outbuf);
      sprintf(clen_header,"Content-length: %lu\r\n\r\n",
	      (unsigned long)content_len);
      u8_puts(&httphead,clen_header);
      content_len=outstream->u8_outptr-outstream->u8_outbuf;
      http_len=httphead.u8_outptr-httphead.u8_outbuf;
      head_len=htmlhead.u8_outptr-htmlhead.u8_outbuf;
      bundle_len=http_len+head_len+content_len;
      if (!(async)) {
	u8_writeall(client->socket,httphead.u8_outbuf,http_len);
	u8_writeall(client->socket,htmlhead.u8_outbuf,head_len);
	u8_writeall(client->socket,outstream->u8_outbuf,content_len);
	return_code=0;}
      else {
	u8_byte *start;
	u8_grow_stream(outstream,head_len+http_len+1);
	start=outstream->u8_outbuf;
	memmove(start+head_len+http_len,start,content_len);
	strncpy(start,httphead.u8_outbuf,http_len);
	strncpy(start+http_len,htmlhead.u8_outbuf,head_len);
	outstream->u8_outptr=start+http_len+head_len+content_len;
	u8_client_write(ucl,start,bundle_len,0);
	buffered=1;
	return_code=1;}}
    else if (FD_STRINGP(retfile)) {
      /* This needs more error checking, signalling, etc */
      u8_string filename=FD_STRDATA(retfile);
      struct stat fileinfo; FILE *f;
      if ((stat(filename,&fileinfo)==0)&&(f=u8_fopen(filename,"rb")))  {
	int bytes_read=0;
	unsigned char *filebuf=NULL; off_t total_len=-1;
	u8_printf(&httphead,"Content-length: %ld\r\n\r\n",
		  (long int)(fileinfo.st_size));
	http_len=httphead.u8_outptr-httphead.u8_outbuf;
	total_len=http_len+fileinfo.st_size;
	if ((async)&&(total_len<FD_FILEBUF_MAX))
	  filebuf=u8_malloc(total_len);
	if (filebuf) {
	  /* This is the case where we hand off a buffer to fdserv
	     to write for us. */
	  unsigned char *write=filebuf+http_len;
	  off_t to_read=fileinfo.st_size, so_far=0;
	  memcpy(write,httphead.u8_outbuf,http_len);
	  while ((to_read>0)&&
		 ((bytes_read=fread(write,sizeof(uchar),to_read,f))>0)) {
	    to_read=to_read-bytes_read;}
	  client->buf=filebuf; client->off=0;
	  client->len=client->buflen=total_len;
	  client->writing=u8_microtime(); client->reading=-1;
	  /* Let the server loop free the buffer when done */
	  client->ownsbuf=1; buffered=1; return_code=1;
	  fclose(f);}
	else {
	  char buf[32768];
	  /* This is the case where we hang while we write. */
	  u8_writeall(client->socket,httphead.u8_outbuf,http_len);
	  while ((bytes_read=fread(buf,sizeof(uchar),32768,f))>0) {
	    content_len=content_len+bytes_read;
	    retval=u8_writeall(client->socket,buf,bytes_read);
	    if (retval<0) break;}
	  return_code=0;}}
      else {/* Error here */}}
    else if (FD_STRINGP(content)) {
      int bundle_len; unsigned char *outbuf;
      content_len=FD_STRLEN(content);
      u8_printf(&httphead,"Content-length: %ld\r\n\r\n",content_len);
      http_len=httphead.u8_outptr-httphead.u8_outbuf;
      bundle_len=http_len+content_len;
      if (async) outbuf=u8_malloc(bundle_len+1);
      if (outbuf) {
	memcpy(outbuf,httphead.u8_outbuf,http_len);
	memcpy(outbuf+http_len,FD_STRDATA(content),content_len);
	outbuf[bundle_len]='\0';
	client->buf=outbuf; client->len=client->buflen=bundle_len;
	client->writing=u8_microtime(); client->reading=-1;
	/* Let the server loop free the buffer when done */
	client->ownsbuf=1; buffered=1; return_code=1;}
      else  {
	u8_writeall(client->socket,httphead.u8_outbuf,
		    httphead.u8_outptr-httphead.u8_outbuf);
	u8_writeall(client->socket,FD_STRDATA(content),FD_STRLEN(content));}}
    else if (FD_PACKETP(content)) {
      int bundle_len; unsigned char *outbuf;
      content_len=FD_PACKET_LENGTH(content);
      u8_printf(&httphead,"Content-length: %ld\r\n\r\n",content_len);
      http_len=httphead.u8_outptr-httphead.u8_outbuf;
      bundle_len=http_len+content_len;
      if (async) outbuf=u8_malloc(bundle_len);
      if (outbuf) {
	memcpy(outbuf,httphead.u8_outbuf,http_len);
	memcpy(outbuf+http_len,FD_PACKET_DATA(content),content_len);
	client->buf=outbuf; client->len=client->buflen=bundle_len;
	client->writing=u8_microtime(); client->reading=-1;
	/* Let the server loop free the buffer when done */
	client->ownsbuf=1; buffered=1; return_code=1;}
      else {
	u8_writeall(client->socket,httphead.u8_outbuf,
		    httphead.u8_outptr-httphead.u8_outbuf);
	u8_writeall(client->socket,FD_PACKET_DATA(content),
		    FD_PACKET_LENGTH(content));}}
    else {
      /* Where the servlet has specified some particular content */
      content_len=content_len+output_content(client,content);}
    /* Reset the stream */
    outstream->u8_outptr=outstream->u8_outbuf;
    /* If we're not still in the transaction, call u8_client_done() */
    if (!(return_code)) {u8_client_done(ucl);}
    if (traceweb>2)
      u8_log(LOG_NOTICE,"HTTPHEAD","HTTPHEAD=%s",httphead.u8_outbuf);
    u8_free(httphead.u8_outbuf); u8_free(htmlhead.u8_outbuf);
    fd_decref(content); fd_decref(traceval);
    if (retval<0)
      u8_log(LOG_ERROR,"BADRET","Bad retval from writing data");
    if ((reqlog) || (urllog) || (trace_cgidata))
      dolog(cgidata,result,client->out.u8_outbuf,
	    outstream->u8_outptr-outstream->u8_outbuf,
	    u8_elapsed_time()-start_time);}
  if (fd_test(cgidata,cleanup_slotid,FD_VOID)) {
    fdtype cleanup=fd_get(cgidata,cleanup_slotid,FD_EMPTY_CHOICE);
    FD_DO_CHOICES(cl,cleanup) {
      fdtype retval=fd_apply(cleanup,0,NULL);
      fd_decref(retval);}}
  forcelog=fd_req_test(forcelog_symbol,FD_VOID);
  run_postflight();
  if (threadcache) fd_pop_threadcache(threadcache);
  fd_use_reqinfo(FD_EMPTY_CHOICE);
  fd_thread_set(browseinfo_symbol,FD_VOID);
  fd_clear_errors(1);
  write_time=u8_elapsed_time();
  getloadavg(end_load,3);
  u8_getrusage(RUSAGE_SELF,&end_usage);
  if ((forcelog)||(traceweb>0)||
      ((overtime>0)&&((write_time-start_time)>overtime))) {
    fdtype query=fd_get(cgidata,query_symbol,FD_VOID);
    if (FD_VOIDP(query))
      u8_log(LOG_NOTICE,"DONE",
	     "%s %d=%d+%d+%d bytes for %q in %f=setup:%f+req:%f+run:%f+write:%f secs, stime=%.2fms, utime=%.2fms, load=%f/%f/%f",
	     ((buffered)?("Buffered"):("Sent")),
	     http_len+head_len+content_len,http_len,head_len,content_len,
	     path,
	     write_time-start_time,
	     setup_time-start_time,
	     parse_time-setup_time,
	     exec_time-parse_time,
	     write_time-exec_time,
	     (u8_dbldifftime(end_usage.ru_utime,start_usage.ru_utime))/1000.0,
	     (u8_dbldifftime(end_usage.ru_stime,start_usage.ru_stime))/1000.0,
	     end_load[0],end_load[1],end_load[2]);
    else u8_log(LOG_NOTICE,"DONE",
		"%s %d=%d+%d+%d bytes %q q=%q in %f=setup:%f+req:%f+run:%f+write:%f secs, stime=%.2fms, utime=%.2fms, load=%f/%f/%f",
		((buffered)?("Buffered"):("Sent")),
		http_len+head_len+content_len,http_len,head_len,content_len,
		path,query,
		write_time-start_time,
		setup_time-start_time,
		parse_time-setup_time,
		exec_time-parse_time,
		write_time-exec_time,
		(u8_dbldifftime(end_usage.ru_utime,start_usage.ru_utime))/1000.0,
		(u8_dbldifftime(end_usage.ru_stime,start_usage.ru_stime))/1000.0,
		end_load[0],end_load[1],end_load[2]);
    if ((forcelog)||((overtime>0)&&((write_time-start_time)>overtime))) {
      u8_string cond=(((overtime>0)&&((write_time-start_time)>overtime))?
		      ("OVERTIME"):("FORCELOG"));
      u8_string before=u8_rusage_string(&start_usage);
      u8_string after=u8_rusage_string(&end_usage);
      u8_log(LOG_NOTICE,cond,"before: %s",before);
      u8_log(LOG_NOTICE,cond," after: %s",after);
      u8_free(before); u8_free(after);}
    /* If we're calling traceweb, keep the log files up to date also. */
    fd_lock_mutex(&log_lock);
    if (urllog) fflush(urllog);
    if (reqlog) fd_dtsflush(reqlog);
    fd_unlock_mutex(&log_lock);
    fd_decref(query);}
  else {}
  fd_decref(proc); fd_decref(result); fd_decref(path);
  fd_swapcheck();
  /* Task is done */
  if (return_code<=0) {}
  return return_code;
}

static int close_webclient(u8_client ucl)
{
  fd_webconn client=(fd_webconn)ucl;
  fd_decref(client->cgidata); client->cgidata=FD_VOID;
  fd_dtsclose(&(client->in),2);
  u8_close((u8_stream)&(client->out));
  return 1;
}

static int reuse_webclient(u8_client ucl)
{
  fd_webconn client=(fd_webconn)ucl;
  fd_decref(client->cgidata); client->cgidata=FD_VOID;
  return 1;
}

static void shutdown_server(u8_condition reason)
{
  int i=n_ports-1;
  u8_lock_mutex(&server_port_lock); i=n_ports-1;
  if (reason) 
    u8_log(LOG_WARN,reason,
	   "Shutting down, removing socket files and pidfile %s",
	   pidfile);
  u8_server_shutdown(&fdwebserver,shutdown_grace);
  webcommon_shutdown();
  while (i>=0) {
    u8_string spec=ports[i];
    if (!(spec)) {}
    else if (strchr(spec,'/')) {
      if (remove(spec)<0) 
	u8_log(LOG_WARN,"FDSERV/shutdown",
	       "Couldn't remove portfile %s",spec);
      u8_free(spec); ports[i]=NULL;}
    else {u8_free(spec); ports[i]=NULL;}
    i--;}
  u8_free(ports);
  ports=NULL; n_ports=0; max_ports=0;
  u8_unlock_mutex(&server_port_lock);
  if (pidfile) {
    int retval=u8_removefile(pidfile);
    if (retval<0) 
      u8_log(LOG_WARN,"FDSERV/shutdown","Couldn't remove pid file %s",pidfile);
    u8_free(pidfile);}
  pidfile=NULL;
  fd_recycle_hashtable(&pagemap);
}

static fdtype servlet_status_string()
{
  u8_string status=u8_server_status(&fdwebserver,NULL,0);
  return fd_lispstring(status);
}

/* Making sure you can write the socket file */

#define SOCKDIR_PERMISSIONS \
  (S_IRUSR|S_IWUSR|S_IXUSR|S_IRGRP|S_IWGRP|S_IXGRP|S_IROTH|S_IXOTH)

static int check_socket_path(char *sockarg)
{
  u8_string sockname=u8_fromlibc(sockarg);
  u8_string sockfile=u8_abspath(sockname,NULL);
  u8_string sockdir=u8_dirname(sockfile);
  int retval=u8_mkdirs(sockdir,SOCKDIR_PERMISSIONS);
  if (retval<0) {
    if (sockname!=((u8_string)sockarg)) u8_free(sockname);
    u8_free(sockfile);
    u8_free(sockdir);
    return retval;}
  else if ((u8_file_existsp(sockname)) ?
	   (u8_file_writablep(sockname)) :
	   (u8_file_writablep(sockdir))) {
    if (sockname!=((u8_string)sockarg)) u8_free(sockname);
    u8_free(sockfile);
    u8_free(sockdir);
    return retval;}
  else {
    u8_free(sockfile);
    u8_free(sockdir);
    u8_seterr(fd_CantWrite,"check_socket_path",sockname);
    return -1;}
}

/* Listeners */

static int add_server(u8_string spec)
{
  int file_socket=((strchr(spec,'/'))!=NULL);
  int len=strlen(spec), retval;
  if (spec[0]==':') spec=spec+1;
  else if (spec[len-1]=='@') spec[len-1]='\0';
  else {}
  retval=u8_add_server(&fdwebserver,spec,((file_socket)?(-1):(0)));
  if (retval<0) return retval;
  else if (file_socket) chmod(spec,0777);
  return 0;
}

static int addfdservport(fdtype var,fdtype val,void *data)
{
  u8_string new_port=NULL;
  u8_lock_mutex(&server_port_lock);
  if (FD_STRINGP(val)) {
    u8_string spec=FD_STRDATA(val);
    if (strchr(spec,'/')) {
      if (check_socket_path(spec)>0) {
	new_port=u8_abspath(spec,NULL);}
      else {
	u8_seterr("Can't write socket file","setportconfig",
		  u8_abspath(spec,NULL));
	u8_unlock_mutex(&server_port_lock);
	return -1;}}
    else if ((strchr(spec,'@'))||(strchr(spec,':'))) 
      new_port=u8_strdup(spec);
    else if (check_socket_path(spec)>0) {
      new_port=u8_abspath(spec,NULL);}
    else {
      u8_unlock_mutex(&server_port_lock);
      u8_seterr("Can't write socket file","setportconfig",
		u8_abspath(spec,NULL));
      return -1;}}
  else if (FD_FIXNUMP(val))
    new_port=u8_mkstring("%d",FD_FIX2INT(val));
  else {
    fd_incref(val);
    fd_seterr(fd_TypeError,"setportconfig",NULL,val);
    return -1;}
  if (!(server_id)) server_id=new_port;
  if (n_ports>=max_ports) {
    int new_max=((max_ports)?(max_ports+8):(8));
    if (ports)
      ports=u8_realloc(ports,sizeof(u8_string)*new_max);
    else ports=u8_malloc(sizeof(u8_string)*new_max);}
  ports[n_ports++]=new_port;
  if (server_running) {
    add_server(new_port);
    u8_unlock_mutex(&server_port_lock);
    return 1;}
  u8_unlock_mutex(&server_port_lock);
  return 0;
}

static fdtype getfdservports(fdtype var,void *data)
{
  fdtype result=FD_EMPTY_CHOICE;
  int i=0, lim=n_ports;
  u8_lock_mutex(&server_port_lock); lim=n_ports;
  while (i<lim) {
    fdtype string=fdtype_string(ports[i++]);
    FD_ADD_TO_CHOICE(result,string);}
  u8_unlock_mutex(&server_port_lock);
  return result;
}

static int start_servers()
{
  int i=0, lim=n_ports;
  u8_lock_mutex(&server_port_lock); lim=n_ports;
  while (i<lim) {
    u8_string port=ports[i++];
    if ((strchr(port,'/'))&&(u8_file_existsp(port))) {
      if (stealsockets) {
	int retval=u8_removefile(port);
	if (retval<0)
	  u8_log(LOG_WARN,"FDSERV/start","Couldn't remove socket file %s",
		 port);}
      else {
	u8_log(LOG_WARN,"FDSERV/start","Socket file %s already exists",
	       port);}}}
  i=0; while (i<lim) {
    int retval=add_server(ports[i]);
    if (retval<0) {
      u8_log(LOG_CRIT,"FDSERV/START","Couldn't start server %s",ports[i]);
      u8_clear_errors(1);}
    i++;}
  server_running=1;
  u8_unlock_mutex(&server_port_lock);
  return i;
}

/* The main() event */

FD_EXPORT void fd_init_dbfile(void); 

int main(int argc,char **argv)
{
  int u8_version=u8_initialize();
  int fd_version; /* Wait to set this until we have a log file */
  int i=1, file_socket=0;
  u8_string socket_spec=NULL;

  if (u8_version<0) {
    u8_log(LOG_ERROR,"STARTUP","Can't initialize LIBU8");
    exit(1);}

  /* Set this here, before processing any configs */
  fddb_loglevel=LOG_INFO;
  
  /* Find the socket spec (the non-config arg) */
  while (i<argc)
    if (strchr(argv[i],'=')) i++;
    else if (socket_spec) i++;
    else socket_spec=argv[i++];
  i=1;

  u8_init_mutex(&server_port_lock);

  if (socket_spec) {
    ports=u8_malloc(sizeof(u8_string)*8);
    max_ports=8; n_ports=1;
    server_id=ports[0]=u8_strdup(socket_spec);}

  fd_register_config("PORT",_("Ports for listening for connections"),
		     getfdservports,addfdservport,NULL);
  fd_register_config("ASYNCMODE",_("Whether to run in asynchronous mode"),
		     fd_boolconfig_get,fd_boolconfig_set,&async_mode);
  
  if (!(getenv("LOGFILE"))) 
    u8_log(LOG_WARN,Startup,"No logfile, using stdout");
  else u8_log(LOG_WARN,Startup,"LOGFILE='%s'",getenv("LOGFILE"));
  
  /* We do this using the Unix environment (rather than configuration
     variables) for twor reasons.  First, we want to redirect errors
     from the processing of the configuration variables themselves
     (where lots of errors could happen); second, we want to be able
     to set this in the environment we wrap around calls (which is how
     mod_fdserv does it). */
  if (getenv("LOGFILE")) {
    char *logfile=u8_strdup(getenv("LOGFILE"));
    int log_fd=open(logfile,O_RDWR|O_APPEND|O_CREAT|O_SYNC,0644);
    if (log_fd<0) {
      u8_log(LOG_WARN,Startup,"Couldn't open log file %s",logfile);
      exit(1);}
    dup2(log_fd,1);
    dup2(log_fd,2);}

  fd_version=fd_init_fdscheme();
  
  if (fd_version<0) {
    u8_log(LOG_WARN,Startup,"Couldn't initialize FramerD");
    exit(EXIT_FAILURE);}

  /* We register this module so that we can have pages that use the functions,
     for instance with an HTTP PROXY that can be used as a dtype server */

  /* Record the startup time for UPTIME and other functions */
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

  /* Now we initialize the u8 configuration variables */
  u8_log_show_date=1;
  u8_log_show_procinfo=1;
  u8_use_syslog(1);

  /* And now we initialize FramerD */
#if ((!(HAVE_CONSTRUCTOR_ATTRIBUTES)) || (FD_TESTCONFIG))
  fd_init_fdscheme();
  fd_init_schemeio();
  fd_init_texttools();
  fd_init_tagger();
  /* May result in innocuous redundant calls */
  FD_INIT_SCHEME_BUILTINS();
  fd_init_fddbserv();
#else
  FD_INIT_SCHEME_BUILTINS();
  fd_init_fddbserv();
#endif

  /* This is the module where the data-access API lives */
  fd_register_module("FDBSERV",fd_incref(fd_fdbserv_module),FD_MODULE_SAFE);
  fd_finish_module(fd_fdbserv_module);
  fd_persist_module(fd_fdbserv_module);

  fd_init_fdweb();
  fd_init_dbfile(); 

  init_webcommon_data();
  init_webcommon_symbols();
  
  /* This is the root of all client service environments */
  if (server_env==NULL) server_env=fd_working_environment();
  fd_idefn((fdtype)server_env,fd_make_cprim0("BOOT-TIME",get_boot_time,0));
  fd_idefn((fdtype)server_env,fd_make_cprim0("UPTIME",get_uptime,0));
  fd_idefn((fdtype)server_env,
	   fd_make_cprim0("SERVLET-STATUS->STRING",servlet_status_string,0));
  fd_idefn((fdtype)server_env,
	   fd_make_cprim0("SERVLET-STATUS",servlet_status,0));

  init_webcommon_configs();
  fd_register_config("OVERTIME",_("Trace web transactions over N seconds"),
		     fd_dblconfig_get,fd_dblconfig_set,&overtime);
  fd_register_config("BACKLOG",
		     _("Number of pending connection requests allowed"),
		     fd_intconfig_get,fd_intconfig_set,&max_backlog);
  fd_register_config("MAXQUEUE",_("Max number of requests to keep queued"),
		     fd_intconfig_get,fd_intconfig_set,&max_queue);
  fd_register_config("MAXCONN",
		     _("Max number of concurrent connections to allow (NYI)"),
		     fd_intconfig_get,fd_intconfig_set,&max_conn);
  fd_register_config("INITCONN",
		     _("Number of clients to prepare for/grow by"),
		     fd_intconfig_get,fd_intconfig_set,&init_clients);
  fd_register_config("NTHREADS",_("Number of threads in the thread pool"),
		     fd_intconfig_get,fd_intconfig_set,&servlet_threads);
  fd_register_config("STATLOG",_("File for recording status reports"),
		     statlog_get,statlog_set,NULL);
  fd_register_config
    ("STATINTERVAL",_("Milliseconds (roughly) between status reports"),
     statinterval_get,statinterval_set,NULL);
  fd_register_config("GRACEFULDEATH",
		     _("How long (μs) to wait for tasks during shutdown"),
		     fd_intconfig_get,fd_intconfig_set,&shutdown_grace);

  fd_register_config("STEALSOCKETS",
		     _("Remove existing socket files with extreme prejudice"),
		     fd_boolconfig_get,fd_boolconfig_set,&stealsockets);

  fd_register_config("IGNORELEFTOVERS",
		     _("Whether to check for existing PID files"),
		     fd_boolconfig_get,fd_boolconfig_set,&ignore_leftovers);

#if FD_THREADS_ENABLED
  /* We keep a lock on the log, which could become a bottleneck if there are I/O problems.
     An alternative would be to log to a data structure and have a separate thread writing
     to the log.  Of course, if we have problems writing to the log, we probably have all sorts
     of other problems too! */
  fd_init_mutex(&log_lock);
#endif

  u8_log(LOG_NOTICE,"LAUNCH","fdserv %s",socket_spec);

  /* Process the config statements */
  while (i<argc)
    if (strchr(argv[i],'=')) {
      u8_log(LOG_NOTICE,"CONFIG","   %s",argv[i]);
      fd_config_assignment(argv[i++]);}
    else i++;

  if (!(server_id)) {
    u8_uuid tmp=u8_getuuid(NULL);
    server_id=u8_uuidstring(tmp,NULL);}

  if (!(check_pid_file(server_id)))
    exit(EXIT_FAILURE);

  u8_log(LOG_DEBUG,Startup,"Updating preloads");
  /* Initial handling of preloads */
  if (update_preloads()<0) {
    /* Error here, rather than repeatedly */
    fd_clear_errors(1);
    exit(EXIT_FAILURE);}
  
  memset(&fdwebserver,0,sizeof(fdwebserver));
  
  u8_init_server
    (&fdwebserver,
     simply_accept, /* acceptfn */
     webservefn, /* handlefn */
     reuse_webclient, /* donefn */
     close_webclient, /* closefn */
     U8_SERVER_INIT_CLIENTS,init_clients,
     U8_SERVER_NTHREADS,servlet_threads,
     U8_SERVER_BACKLOG,max_backlog,
     U8_SERVER_MAX_QUEUE,max_queue,
     U8_SERVER_MAX_CLIENTS,max_conn,
     U8_SERVER_END_INIT); 

  fd_register_config("U8LOGLISTEN",
		     _("Whether to have libu8 log each monitored address"),
		     config_get_u8server_flag,config_set_u8server_flag,
		     (void *)(U8_SERVER_LOG_LISTEN));
  fd_register_config("U8LOGCONNECT",
		     _("Whether to have libu8 log each connection"),
		     config_get_u8server_flag,config_set_u8server_flag,
		     (void *)(U8_SERVER_LOG_CONNECT));
  fd_register_config("U8LOGTRANSACT",
		     _("Whether to have libu8 log each transaction"),
		     config_get_u8server_flag,config_set_u8server_flag,
		     (void *)(U8_SERVER_LOG_TRANSACT));
#ifdef U8_SERVER_LOG_TRANSFERS
  fd_register_config
    ("U8LOGTRANSFER",
     _("Whether to have libu8 log data transmission/receiption"),
     config_get_u8server_flag,config_set_u8server_flag,
     (void *)(U8_SERVER_LOG_TRANSFERS));
#endif
#ifdef U8_SERVER_ASYNC
  fd_register_config("U8ASYNC",
		     _("Whether to support thread-asynchronous transactions"),
		     config_get_u8server_flag,config_set_u8server_flag,
		     (void *)(U8_SERVER_ASYNC));
  if (async_mode) fdwebserver.flags=fdwebserver.flags|U8_SERVER_ASYNC;
#endif
  
  /* Now that we're running, shutdowns occur normally. */
  init_webcommon_finalize();

  /* We check this now, to kludge around some race conditions */
  if ((file_socket)&&(u8_file_existsp(socket_spec))) {
    if (((time(NULL))-(u8_file_mtime(socket_spec)))<FD_LEFTOVER_AGE) {
      u8_log(LOG_CRIT,"FDSERV/SOCKETRACE",
	     "Aborting due to recent socket file %s",socket_spec);
      return -1;}
    else {
      u8_log(LOG_WARN,"FDSERV/SOCKETZAP",
	     "Removing leftover socket file %s",socket_spec);
      remove(socket_spec);}}

  update_preloads();

  if (start_servers()<=0) {
    u8_log(LOG_CRIT,"FDSERV/STARTUP","Startup failed");
    exit(1);}

  u8_log(LOG_INFO,NULL,
	 "FramerD (%s) fdserv servlet running, %d/%d pools/indices",
	 FRAMERD_REV,fd_n_pools,
	 fd_n_primary_indices+fd_n_secondary_indices);
  u8_message("beingmeta FramerD, (C) beingmeta 2004-2013, all rights reserved");
  u8_server_loop(&fdwebserver);

  shutdown_server("exit");

  return 0;
}

