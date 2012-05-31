/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2012 beingmeta, inc.
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

#include "revision.h"

static u8_condition Startup=_("FDSERV Startup");

FD_EXPORT void fd_init_fdweb(void);
FD_EXPORT void fd_init_texttools(void);
FD_EXPORT void fd_init_tagger(void);
FD_EXPORT int fd_init_fddbserv(void);

#include "webcommon.h"

/* Logging declarations */
static FILE *statlog=NULL;
static double overtime=0;
static int loglisten=1, logconnect=0, logtransact=0;

/* Tracking ports */
static u8_string server_id=NULL;
static u8_string *ports=NULL;
static int n_ports=0, max_ports=0;
static u8_mutex server_port_lock;

/* When the server started, used by UPTIME */
static struct U8_XTIME boot_time;

#define FD_REQERRS 1 /* records only transactions which return errors */
#define FD_ALLREQS 2 /* records all requests */
#define FD_ALLRESP 3 /* records all requests and the response set back */

/* This is how old a socket file needs to be to be deleted as 'leftover' */
#define FD_LEFTOVER_AGE 30

typedef struct FD_WEBCONN {
  U8_CLIENT_FIELDS;
  struct FD_DTYPE_STREAM in;
  struct U8_OUTPUT out;} FD_WEBCONN;
typedef struct FD_WEBCONN *fd_webconn;

static fd_lispenv server_env;
struct U8_SERVER fdwebserver;
static int server_running=0;

/* This environment is the parent of all script/page environments spun off
   from this server. */
static int servlet_ntasks=64, servlet_threads=8;
/* This is the backlog of connection requests not transactions.
   It is passed as the argument to listen() */
static int max_backlog=-1;

/* STATLOG config */

/* The urllog is a plain text (UTF-8) file which contains all of the
   URLs passed to the servlet.  This can be used for generating new load tests
   or for general debugging. */

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

static void report_status()
{
  u8_lock_mutex(&(fdwebserver.lock));
  long long now=u8_microtime();
  long long wcount=0, wmax=0, wmin=0, wsum=0, wsqsum=0;
  long long rcount=0, rmax=0, rmin=0, rsum=0, rsqsum=0;
  int i=0, lim=fdwebserver.socket_lim;
  struct U8_CLIENT **socketmap=fdwebserver.socketmap;
  last_status=now;
  while (i<lim) {
    u8_client cl=socketmap[i++];
    if (!(cl)) continue;
    if (cl->queued>=0) {
      long long queuestart=cl->queued;
      long long interval=now-queuestart;
      wcount++; wsum=wsum+interval; wsqsum=wsqsum+(interval*interval);
      if (interval>wmax) wmax=interval;}
    if (cl->started>=0) {
      long long runstart=cl->started;
      long long interval=now-runstart;
      rcount++; rsum=rsum+interval; rsqsum=rsqsum+(interval*interval);
      if (interval>rmax) rmax=interval;}}
  if (statlog) {
    u8_fprintf(statlog,
	       "[%*t][%f] %d/%d/%d busy clients; avg(wait)=%f(%d); avg(run)=%f(%d)\n",
	       u8_elapsed_time(),
	       fdwebserver.n_busy,fdwebserver.n_tasks,fdwebserver.n_clients,
	       ((double)fdwebserver.waitsum)/(((double)fdwebserver.waitcount)),fdwebserver.waitcount,
	       ((double)fdwebserver.runsum)/(((double)fdwebserver.runcount)),fdwebserver.runcount);
    fflush(statlog);}
  else u8_log(LOG_INFO,"fdserv",
	       "[%*t][%f] %d/%d/%d busy/waiting/clients; avg(wait)=%f; avg(run)=%f",
	      u8_elapsed_time(),
	      fdwebserver.n_busy,fdwebserver.n_tasks,fdwebserver.n_clients,
	      ((double)fdwebserver.waitsum)/(((double)fdwebserver.waitcount)),
	      ((fdwebserver.runcount)?
	       (((double)fdwebserver.runsum)/(((double)fdwebserver.runcount))):
	       (-1)));
  if (statlog)
    u8_fprintf
      (statlog,"[%*t][%f] wait (n=%lld) min=%lld max=%lld avg=%f\n",
       u8_elapsed_time(),wcount,wmin,wmax,((double)wsum)/wcount);
  else u8_log(LOG_INFO,"fdserv","[%*t][%f] wait (n=%lld) min=%lld max=%lld avg=%f",
	      u8_elapsed_time(),wcount,wmin,wmax,((double)wsum)/wcount);
  if (rcount) {
    if (statlog)
      u8_fprintf
	(statlog,
	 "[%*t][%f] run (n=%lld) min=%lld max=%lld avg=%f\n",
	 u8_elapsed_time(),rcount,rmin,rmax,((double)rsum)/rcount);
    else u8_log(LOG_INFO,"fdserv",
		"[%*t][%f] run (n=%lld) min=%lld max=%lld avg=%f",
		u8_elapsed_time(),rcount,rmin,rmax,((double)rsum)/rcount);}
  u8_unlock_mutex(&(fdwebserver.lock));
  if (statlog) fflush(statlog);
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
    else if (((time(NULL))-(fileinfo.st_mtime))<FD_LEFTOVER_AGE) {
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

/* Running the server */

static u8_client simply_accept(int sock,struct sockaddr *addr,int len)
{
  /* We could do access control here. */
  fd_webconn consed=u8_alloc(FD_WEBCONN);
  memset(consed,0,sizeof(FD_WEBCONN));
  consed->socket=sock; consed->flags=0;
  fd_init_dtype_stream(&(consed->in),sock,4096);
  U8_INIT_OUTPUT(&(consed->out),8192);
  u8_set_nodelay(sock,1);
  return (u8_client) consed;
}

static int webservefn(u8_client ucl)
{
  fdtype proc=FD_VOID, result=FD_VOID, cgidata=FD_VOID, path=FD_VOID;
  fd_webconn client=(fd_webconn)ucl; int write_headers=1;
  double start_time, setup_time, parse_time, exec_time, write_time;
  struct FD_THREAD_CACHE *threadcache=NULL;
  struct rusage start_usage, end_usage;
  double start_load[]={-1,-1,-1}, end_load[]={-1,-1,-1};
  int forcelog=0, retval=0;
  size_t headerlen=0, contentlen=0;
  if ((status_interval>=0)&&(u8_microtime()>last_status+status_interval))
    report_status();
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
    if (details) u8_free(details); fd_decref(irritant);}
  else if (update_preloads()<0) {
    u8_condition c; u8_context cxt; u8_string details=NULL;
    fdtype irritant;
    setup_time=parse_time=u8_elapsed_time();
    if (fd_poperr(&c,&cxt,&details,&irritant)) 
      proc=fd_err(c,cxt,details,irritant);
    if (details) u8_free(details); fd_decref(irritant);}
  else {
    setup_time=u8_elapsed_time();
    cgidata=fd_dtsread_dtype(&(client->in));
    if (docroot) webcommon_adjust_docroot(cgidata,docroot);
    path=fd_get(cgidata,script_filename,FD_VOID);
    if (traceweb>0) {
      fdtype referer=fd_get(cgidata,referer_symbol,FD_VOID);
      fdtype remote=fd_get(cgidata,remote_info,FD_VOID);
      fdtype uri=fd_get(cgidata,uri_symbol,FD_VOID);
      if ((FD_STRINGP(uri)) &&  (FD_STRINGP(referer)) && (FD_STRINGP(remote)))
	u8_log(LOG_NOTICE,"REQUEST","Handling request for %s from %s by %s, load=%f/%f/%f",
	       FD_STRDATA(uri),FD_STRDATA(referer),FD_STRDATA(remote),
	       start_load[0],start_load[1],start_load[2]);
      else if ((FD_STRINGP(uri)) &&  (FD_STRINGP(remote)))
	u8_log(LOG_NOTICE,"REQUEST","Handling request for %s by %s, load=%f/%f/%f",
	       FD_STRDATA(uri),FD_STRDATA(remote),
	       start_load[0],start_load[1],start_load[2]);
      else if ((FD_STRINGP(uri)) &&  (FD_STRINGP(referer)))
	u8_log(LOG_NOTICE,"REQUEST","Handling request for %s from %s, load=%f/%f/%f",
	       FD_STRDATA(uri),FD_STRDATA(referer),
	       start_load[0],start_load[1],start_load[2]);
      else if (FD_STRINGP(uri))
	u8_log(LOG_NOTICE,"REQUEST","Handling request for %s",FD_STRDATA(uri));
      fd_decref(referer);
      fd_decref(uri);}
    proc=getcontent(path);
    fd_parse_cgidata(cgidata);
    parse_time=u8_elapsed_time();
    if ((reqlog) || (urllog) || (trace_cgidata))
      dolog(cgidata,FD_NULL,NULL,-1,parse_time-start_time);}
  fd_set_default_output(&(client->out));
  fd_use_reqinfo(cgidata);
  fd_thread_set(browseinfo_symbol,FD_EMPTY_CHOICE);
  if (FD_ABORTP(proc)) result=fd_incref(proc);
  else if (FD_PRIM_TYPEP(proc,fd_sproc_type)) {
    struct FD_SPROC *sp=FD_GET_CONS(proc,fd_sproc_type,fd_sproc);
    if (traceweb>1)
      u8_log(LOG_NOTICE,"START","Handling %q with Scheme procedure %q",
	     path,proc);
    threadcache=checkthreadcache(sp->env);
    result=fd_cgiexec(proc,cgidata);}
  else if ((FD_PAIRP(proc)) && (FD_PRIM_TYPEP((FD_CAR(proc)),fd_sproc_type))) {
    struct FD_SPROC *sp=FD_GET_CONS(FD_CAR(proc),fd_sproc_type,fd_sproc);
    if (traceweb>1)
      u8_log(LOG_NOTICE,"START","Handling %q with Scheme procedure %q",
	     path,proc);
    threadcache=checkthreadcache(sp->env);
    result=fd_cgiexec(FD_CAR(proc),cgidata);}
  else if (FD_PAIRP(proc)) {
    fdtype lenv=FD_CDR(proc), setup_proc=FD_VOID;
    fd_lispenv base=((FD_PTR_TYPEP(lenv,fd_environment_type)) ?
		     (FD_GET_CONS(FD_CDR(proc),fd_environment_type,fd_environment)) :
		     (NULL));
    fd_lispenv runenv=fd_make_env(fd_incref(cgidata),base);
    if (base) fd_load_latest(NULL,base,NULL);
    threadcache=checkthreadcache(base);
    if (traceweb>1)
      u8_log(LOG_NOTICE,"START","Handling %q with template",path);
    setup_proc=fd_symeval(setup_symbol,base);
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
  fd_set_default_output(NULL);
  if (FD_TROUBLEP(result)) {
    u8_exception ex=u8_erreify();
    u8_condition excond=ex->u8x_cond;
    u8_context excxt=((ex->u8x_context) ?
		      (ex->u8x_context) : ((u8_context)"somewhere"));
    u8_context exdetails=((ex->u8x_details)
			  ? (ex->u8x_details) : ((u8_string)"no more details"));
    fdtype irritant=fd_exception_xdata(ex);
    if (FD_VOIDP(irritant))
      u8_log(LOG_INFO,excond,"Unexpected error \"%m \"for %s:@%s (%s)",
	     excond,FD_STRDATA(path),excxt,exdetails);
    else u8_log(LOG_INFO,excond,"Unexpected error \"%m\" for %s:%s (%s) %q",
		excond,FD_STRDATA(path),excxt,exdetails,irritant);
    headerlen=headerlen+
      strlen("Content-type: text/html; charset='utf-8'\r\n\r\n");
    write_string(client->socket,
		 "Content-type: text/html; charset='utf-8'\r\n\r\n");
    fd_xhtmlerrorpage(&(client->out),ex);
    u8_free_exception(ex,1);
    if ((reqlog) || (urllog) || (trace_cgidata))
      dolog(cgidata,result,client->out.u8_outbuf,
	    client->out.u8_outptr-client->out.u8_outbuf,
	    u8_elapsed_time()-start_time);
    contentlen=contentlen+(client->out.u8_outptr-client->out.u8_outbuf);
    u8_writeall(client->socket,client->out.u8_outbuf,
		client->out.u8_outptr-client->out.u8_outbuf);
    u8_client_close(ucl);}
  else {
    U8_OUTPUT tmp; int tracep;
    fdtype content=fd_get(cgidata,content_slotid,FD_VOID);
    fdtype traceval=fd_get(cgidata,tracep_slotid,FD_VOID);
    fdtype retfile=((FD_VOIDP(content))?
		    (fd_get(cgidata,retfile_slotid,FD_VOID)):
		    (FD_VOID));
    if (FD_VOIDP(traceval)) tracep=0; else tracep=1;
    U8_INIT_OUTPUT(&tmp,1024);
    fd_output_http_headers(&tmp,cgidata);
    if ((cgitrace)&&(tracep)) fprintf(stderr,"%s\n",tmp.u8_outbuf);
    headerlen=headerlen+(tmp.u8_outptr-tmp.u8_outbuf);
    u8_writeall(client->socket,tmp.u8_outbuf,tmp.u8_outptr-tmp.u8_outbuf);
    tmp.u8_outptr=tmp.u8_outbuf;
    if ((FD_VOIDP(content))&&(FD_VOIDP(retfile))) {
      /* Normal case, when the output is just sent to the client */
      if (write_headers) {
	write_headers=fd_output_xhtml_preface(&tmp,cgidata);
	contentlen=contentlen+(tmp.u8_outptr-tmp.u8_outbuf);
	u8_writeall(client->socket,tmp.u8_outbuf,tmp.u8_outptr-tmp.u8_outbuf);}
      contentlen=contentlen+(client->out.u8_outptr-client->out.u8_outbuf);
      retval=u8_writeall(client->socket,client->out.u8_outbuf,
			 client->out.u8_outptr-client->out.u8_outbuf);
      if (write_headers) {
	contentlen=contentlen+strlen("</body>\n</html>\n");
	write_string(client->socket,"</body>\n</html>\n");}}
    else if (FD_STRINGP(retfile)) {
      /* This needs more error checking, signallin */
      u8_string filename=FD_STRDATA(retfile);
      FILE *f=u8_fopen(filename,"rb");
      if (f) {
	int bytes_read=0; unsigned char buf[32768];
	while ((bytes_read=fread(buf,sizeof(unsigned char),32768,f))>0) {
	  contentlen=contentlen+bytes_read;
	  retval=u8_writeall(client->socket,buf,bytes_read);
	  if (retval<0) break;}
	fclose(f);}}
      else {
	/* Where the servlet has specified some particular content */
	contentlen=contentlen+output_content(client,content);}
    /* Reset the stream */
    client->out.u8_outptr=client->out.u8_outbuf;
    u8_client_close(ucl); /* u8_client_done(ucl); */
    u8_free(tmp.u8_outbuf);
    fd_decref(content); fd_decref(traceval);
    if (retval<0)
      u8_log(LOG_ERROR,"BADRET","Bad retval from writing data");
    if ((reqlog) || (urllog) || (trace_cgidata))
      dolog(cgidata,result,client->out.u8_outbuf,
	    client->out.u8_outptr-client->out.u8_outbuf,
	    u8_elapsed_time()-start_time);}
  if (fd_test(cgidata,cleanup_slotid,FD_VOID)) {
    fdtype cleanup=fd_get(cgidata,cleanup_slotid,FD_EMPTY_CHOICE);
    FD_DO_CHOICES(cl,cleanup) {
      fdtype retval=fd_apply(cleanup,0,NULL);
      fd_decref(retval);}}
  forcelog=fd_req_test(forcelog_symbol,FD_VOID);
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
	     "Sent %d=%d+%d bytes for %q in %f=setup:%f+req:%f+run:%f+write:%f secs, stime=%.2fms, utime=%.2fms, load=%f/%f/%f",
	     headerlen+contentlen,headerlen,contentlen,path,
	     write_time-start_time,
	     setup_time-start_time,
	     parse_time-setup_time,
	     exec_time-parse_time,
	     write_time-exec_time,
	     (u8_dbldifftime(end_usage.ru_utime,start_usage.ru_utime))/1000.0,
	     (u8_dbldifftime(end_usage.ru_stime,start_usage.ru_stime))/1000.0,
	     end_load[0],end_load[1],end_load[2]);
    else u8_log(LOG_NOTICE,"DONE",
		"Sent %d=%d+%d bytes %q q=%q in %f=setup:%f+req:%f+run:%f+write:%f secs, stime=%.2fms, utime=%.2fms, load=%f/%f/%f",
		headerlen+contentlen,headerlen,contentlen,path,query,
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
		      "OVERTIME":"FORCELOG");
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
  /* u8_client_close(ucl); */
  fd_swapcheck();
  /* Task is done */
  return 0;
}

static int close_webclient(u8_client ucl)
{
  fd_webconn client=(fd_webconn)ucl;
  fd_dtsclose(&(client->in),2);
  u8_close((u8_stream)&(client->out));
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
  u8_server_shutdown(&fdwebserver);
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
    u8_removefile(pidfile);
    u8_free(pidfile);}
  pidfile=NULL;
  fd_recycle_hashtable(&pagemap);
}

static fdtype get_servlet_status()
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
  if (server_running) add_server(new_port);
  u8_unlock_mutex(&server_port_lock);
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
  
  if (!(getenv("LOGFILE"))) 
    u8_log(LOG_WARN,Startup,"No logfile, using stdio");
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
	   fd_make_cprim0("SERVLET-STATUS",get_servlet_status,0));

  init_webcommon_configs();
  fd_register_config("OVERTIME",_("Trace web transactions over N seconds"),
		     fd_dblconfig_get,fd_dblconfig_set,&traceweb);
  fd_register_config("BACKLOG",
		     _("Number of pending connection requests allowed"),
		     fd_intconfig_get,fd_intconfig_set,&max_backlog);
  fd_register_config("MAXQUEUE",_("Max number of requests to keep queued"),
		     fd_intconfig_get,fd_intconfig_set,&servlet_ntasks);
  fd_register_config("NTHREADS",_("Number of threads in the thread pool"),
		     fd_intconfig_get,fd_intconfig_set,&servlet_threads);
  fd_register_config("STATLOG",_("File for recording status reports"),
		     statlog_get,statlog_set,NULL);
  fd_register_config
    ("STATINTERVAL",_("Milliseconds (roughly) between status reports"),
     statinterval_get,statinterval_set,NULL);

  fd_register_config("LOGLISTEN",_("Log when servers start listening"),
		     fd_boolconfig_get,fd_boolconfig_set,&loglisten);
  fd_register_config("LOGCONNECT",_("Log server connections"),
		     fd_boolconfig_get,fd_boolconfig_set,&logconnect);
  fd_register_config("LOGTRANSACT",_("Log client/server transactions"),
		     fd_boolconfig_get,fd_boolconfig_set,&logconnect);

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
  
  u8_server_init(&fdwebserver,
		 max_backlog,servlet_ntasks,servlet_threads,
		 simply_accept,webservefn,close_webclient);
  if (loglisten)
    fdwebserver.flags=fdwebserver.flags|U8_SERVER_LOG_LISTEN;
  if (logconnect)
    fdwebserver.flags=fdwebserver.flags|U8_SERVER_LOG_CONNECT;
  if (logtransact)
    fdwebserver.flags=fdwebserver.flags|U8_SERVER_LOG_TRANSACT;
  
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

  if (start_servers()<=0) {
    u8_log(LOG_CRIT,"FDSERV/STARTUP","Startup failed");
    exit(1);}

  u8_log(LOG_INFO,NULL,
	 "FramerD (%s) fdserv servlet running, %d/%d pools/indices",
	 FRAMERD_REV,fd_n_pools,
	 fd_n_primary_indices+fd_n_secondary_indices);
  u8_message("beingmeta FramerD, (C) beingmeta 2004-2012, all rights reserved");
  u8_server_loop(&fdwebserver);

  shutdown_server("exit");

  return 0;
}

