/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2011 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

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

/* Logging declarations */
static u8_mutex log_lock;
static u8_string urllogname=NULL;
static FILE *urllog=NULL;
static FILE *statlog=NULL;
static u8_string reqlogname=NULL;
static fd_dtype_stream reqlog=NULL;
static int reqloglevel=0;
/* Whether (and how much) to trace all web transactions */
static int traceweb=0;
/* Trace web transactions that take over *overtime* seconds */
static double overtime=0;

static char *pidfile;

static fd_exception CantWriteSocket=_("Can't write to socket");

/* When the server started, used by UPTIME */
static struct U8_XTIME boot_time;

#define FD_REQERRS 1 /* records only transactions which return errors */
#define FD_ALLREQS 2 /* records all requests */
#define FD_ALLRESP 3 /* records all requests and the response set back */

/* This is how old a socket file needs to be to be deleted as 'leftover' */
#define FD_LEFTOVER_AGE 30

static int load_report_freq=1000;
static time_t last_load_report=-1;

typedef struct FD_WEBCONN {
  U8_CLIENT_FIELDS;
  struct FD_DTYPE_STREAM in;
  struct U8_OUTPUT out;} FD_WEBCONN;
typedef struct FD_WEBCONN *fd_webconn;

static fdtype cgisymbol, main_symbol, setup_symbol, script_filename, uri_symbol;
static fdtype response_symbol, err_symbol, browseinfo_symbol;
static fdtype http_headers, html_headers, doctype_slotid, xmlpi_slotid, remote_info_symbol;
static fdtype content_slotid, content_type, retfile_slotid, cleanup_slotid;
static fdtype tracep_slotid, query_symbol, referer_symbol, forcelog_symbol;
static fd_lispenv server_env;
struct U8_SERVER fdwebserver;

/* This environment is the parent of all script/page environments spun off
   from this server. */
static fd_lispenv server_env=NULL;
static int servlet_ntasks=64, servlet_threads=8;
/* This is the backlog of connection requests not transactions.
   It is passed as the argument to listen() */
static int max_backlog=-1;

/* TRACEWEB config  */

static fdtype traceweb_get(fdtype var,void MAYBE_UNUSED *data)
{
  if (traceweb) return FD_INT2DTYPE(traceweb); else return FD_FALSE;
}
static int traceweb_set(fdtype var,fdtype val,void MAYBE_UNUSED *data)
{
  if (FD_FIXNUMP(val))
    traceweb=FD_FIX2INT(val);
  else if (FD_TRUEP(val))
    traceweb=1;
  else traceweb=0;
  return 1;
}

/* URLLOG config */

/* The urllog is a plain text (UTF-8) file which contains all of the
   URLs passed to the servlet.  This can be used for generating new load tests
   or for general debugging. */

static int urllog_set(fdtype var,fdtype val,void *data)
{
  if (FD_STRINGP(val)) {
    u8_string filename=FD_STRDATA(val);
    fd_lock_mutex(&log_lock);
    if (urllog) {
      fclose(urllog); urllog=NULL;
      u8_free(urllogname); urllogname=NULL;}
    urllog=u8_fopen_locked(filename,"a");
    if (urllog) {
      u8_string tmp;
      urllogname=u8_strdup(filename);
      tmp=u8_mkstring("# Log open %*lt for %s\n",u8_sessionid());
      fputs(tmp,urllog);
      fd_unlock_mutex(&log_lock);
      u8_free(tmp);
      return 1;}
    else return -1;}
  else if (FD_FALSEP(val)) {
    fd_lock_mutex(&log_lock);
    if (urllog) {
      fclose(urllog); urllog=NULL;
      u8_free(urllogname); urllogname=NULL;}
    fd_unlock_mutex(&log_lock);
    return 0;}
  else return fd_reterr
	 (fd_TypeError,"config_set_urllog",u8_strdup(_("string")),val);
}

static fdtype urllog_get(fdtype var,void *data)
{
  if (urllog)
    return fdtype_string(urllogname);
  else return FD_FALSE;
}

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
    statlogfile=u8_abspath(FD_STRDATA(val),NULL);
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
  if (rcount)
    if (statlog)
      u8_fprintf
	(statlog,
	 "[%*t][%f] run (n=%lld) min=%lld max=%lld avg=%f\n",
	 u8_elapsed_time(),rcount,rmin,rmax,((double)rsum)/rcount);
    else u8_log(LOG_INFO,"fdserv",
		"[%*t][%f] run (n=%lld) min=%lld max=%lld avg=%f",
		u8_elapsed_time(),rcount,rmin,rmax,((double)rsum)/rcount);
  u8_unlock_mutex(&(fdwebserver.lock));
  if (statlog) fflush(statlog);
}

/* REQLOG config */

/* The reqlog is a binary (DTYPE) file containing detailed debugging information.
   It contains the CGI data record processed for each transaction, together with
   any error objects returned and the actual text string sent back to the client.

   The reqloglevel determines which transactions are put in the reqlog.  There
   are currently three levels:
    1 (FD_REQERRS) records only transactions which return errors.
    2 (FD_ALLREQS) records all requests.
    3 (FD_ALLRESP) records all requests and the response set back.
*/

static int reqlog_set(fdtype var,fdtype val,void *data)
{
  if (FD_STRINGP(val)) {
    u8_string filename=FD_STRDATA(val);
    fd_lock_mutex(&log_lock);
    if ((reqlogname) && (strcmp(filename,reqlogname)==0)) {
      fd_dtsflush(reqlog);
      fd_unlock_mutex(&log_lock);
      return 0;}
    else if (reqlog) {
      fd_dtsclose(reqlog,1); reqlog=NULL;
      u8_free(reqlogname); reqlogname=NULL;}
    reqlog=u8_alloc(struct FD_DTYPE_STREAM);
    if (fd_init_dtype_file_stream(reqlog,filename,FD_DTSTREAM_WRITE,16384)) {
      u8_string logstart=
	u8_mkstring("# Log open %*lt for %s",u8_sessionid());
      fdtype logstart_entry=fd_lispstring(logstart);
      fd_endpos(reqlog);
      reqlogname=u8_strdup(filename);
      fd_dtswrite_dtype(reqlog,logstart_entry);
      fd_decref(logstart_entry);
      fd_unlock_mutex(&log_lock);
      return 1;}
    else {
      fd_unlock_mutex(&log_lock);
      u8_free(reqlog); return -1;}}
  else if (FD_FALSEP(val)) {
    fd_lock_mutex(&log_lock);
    if (reqlog) {
      fd_dtsclose(reqlog,1); reqlog=NULL;
      u8_free(reqlogname); reqlogname=NULL;}
    fd_unlock_mutex(&log_lock);
    return 0;}
  else return fd_reterr
	 (fd_TypeError,"config_set_urllog",u8_strdup(_("string")),val);
}

static fdtype reqlog_get(fdtype var,void *data)
{
  if (reqlog)
    return fdtype_string(reqlogname);
  else return FD_FALSE;
}

/* Logging primitive */

static void dolog
  (fdtype cgidata,fdtype val,u8_string response,double exectime)
{
  fd_lock_mutex(&log_lock);
  if (FD_NULLP(val)) {
    /* This is pre execution */
    if (urllog) {
      fdtype uri=fd_get(cgidata,uri_symbol,FD_VOID);
      u8_string tmp=u8_mkstring(">%s\n@%*lt %g/%g\n",
				FD_STRDATA(uri),
				exectime,
				u8_elapsed_time());
      fputs(tmp,urllog); u8_free(tmp);
      fd_decref(uri);}}
  else if (FD_ABORTP(val)) {
    if (urllog) {
      fdtype uri=fd_get(cgidata,uri_symbol,FD_VOID); u8_string tmp;
      if (FD_TROUBLEP(val)) {
	u8_exception ex=u8_erreify();
	if (ex==NULL)
	  tmp=u8_mkstring("!%s\n@%*lt %g/%g (mystery error)\n",FD_STRDATA(uri),
			  exectime,u8_elapsed_time());
	
	else if (ex->u8x_context)
	  tmp=u8_mkstring("!%s\n@%*lt %g/%g %s %s\n",FD_STRDATA(uri),
			  exectime,u8_elapsed_time(),
			  ex->u8x_cond,ex->u8x_context);
	else tmp=u8_mkstring("!%s\n@%*lt %g/%g %s\n",FD_STRDATA(uri),
			     exectime,u8_elapsed_time(),ex->u8x_cond);}
      else tmp=u8_mkstring("!%s\n@%*lt %g/%g %q\n",FD_STRDATA(uri),
			   exectime,u8_elapsed_time(),val);
      fputs(tmp,urllog); u8_free(tmp);}
    if (reqlog) {
      fd_store(cgidata,err_symbol,val);
      fd_dtswrite_dtype(reqlog,cgidata);}}
  else {
    if (urllog) {
      fdtype uri=fd_get(cgidata,uri_symbol,FD_VOID);
      u8_string tmp=u8_mkstring("<%s\n@%*lt %g/%g\n",FD_STRDATA(uri),
				exectime,u8_elapsed_time());
      fputs(tmp,urllog); u8_free(tmp);}
    if ((reqlog) && (reqloglevel>2)) 
      fd_store(cgidata,response_symbol,fdtype_string(response));
    if ((reqlog) && (reqloglevel>1))
      fd_dtswrite_dtype(reqlog,cgidata);}
  fd_unlock_mutex(&log_lock);
}

/* Writing the PID file */

static int check_pid_file(char *sockname)
{
  int fd;
  int len=strlen(sockname);
  char *dot=strchr(sockname,'.'), buf[128];
  pidfile=u8_malloc(len+8);
  if (dot) {
    strncpy(pidfile,sockname,dot-sockname);
    pidfile[dot-sockname]='\0';}
  else strcpy(pidfile,sockname);
  strcat(pidfile,".pid");
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

/* Preloads */

struct FD_PRELOAD_LIST {
  u8_string filename; time_t mtime;
  struct FD_PRELOAD_LIST *next;} *preloads=NULL;

#if FD_THREADS_ENABLED
static u8_mutex preload_lock;
#endif

static fdtype preload_get(fdtype var,void *ignored)
{
  fdtype results=FD_EMPTY_LIST; struct FD_PRELOAD_LIST *scan;
  fd_lock_mutex(&preload_lock);
  scan=preloads; while (scan) {
    results=fd_init_pair(NULL,fdtype_string(scan->filename),results);
    scan=scan->next;}
  fd_unlock_mutex(&preload_lock);
  return results;
}

static int preload_set(fdtype var,fdtype val,void *ignored)
{
  if (!(FD_STRINGP(val)))
    return fd_reterr
      (fd_TypeError,"preload_config_set",u8_strdup("string"),val);
  else {
    struct FD_PRELOAD_LIST *scan;
    u8_string filename=FD_STRDATA(val); time_t mtime;
    fd_lock_mutex(&preload_lock);
    scan=preloads; while (scan) {
      if (strcmp(filename,scan->filename)==0) {
	mtime=u8_file_mtime(filename);
	if (mtime>scan->mtime) {
	  fd_load_source(filename,server_env,"auto");
	  scan->mtime=mtime;}
	fd_unlock_mutex(&preload_lock);
	return 0;}
      else scan=scan->next;}
    if (server_env==NULL) server_env=fd_working_environment();
    scan=u8_alloc(struct FD_PRELOAD_LIST);
    scan->filename=u8_strdup(filename);
    scan->mtime=(time_t)-1;
    scan->next=preloads;
    preloads=scan;
    fd_unlock_mutex(&preload_lock);
    return 1;}
}

double last_preload_update=-1.0;

static int update_preloads()
{
  if ((last_preload_update<0) ||
      ((u8_elapsed_time()-last_preload_update)>1.0)) {
    struct FD_PRELOAD_LIST *scan; int n_reloads=0;
    fd_lock_mutex(&preload_lock);
    if ((u8_elapsed_time()-last_preload_update)<1.0) {
      fd_unlock_mutex(&preload_lock);
      return 0;}
    scan=preloads; while (scan) {
      time_t mtime=u8_file_mtime(scan->filename);
      if (mtime>scan->mtime) {
	fdtype load_result=fd_load_source(scan->filename,server_env,"auto");
	if (FD_ABORTP(load_result)) {
	  fd_unlock_mutex(&preload_lock);
	  return fd_interr(load_result);}
	n_reloads++; fd_decref(load_result);
	scan->mtime=mtime;}
      scan=scan->next;}
    last_preload_update=u8_elapsed_time();
    fd_unlock_mutex(&preload_lock);
    return n_reloads;}
  else return 0;
}


/* Getting content for pages */

static FD_HASHTABLE pagemap;

static int whitespace_stringp(u8_string s)
{
  int c=u8_sgetc(&s);
  while (c>0)
    if (u8_isspace(c)) c=u8_sgetc(&s);
    else return 0;
  return 1;
}

static fdtype loadcontent(fdtype path)
{
  u8_string pathname=FD_STRDATA(path), oldsource;
  double load_start=u8_elapsed_time();
  u8_string content=u8_filestring(pathname,NULL);
  if (traceweb>0)
    u8_log(LOG_NOTICE,"LOADING","Loading %s",pathname);
  if (content[0]=='<') {
    U8_INPUT in; FD_XML *xml; fd_lispenv env;
    fdtype lenv, ldata, parsed;
    U8_INIT_STRING_INPUT(&in,strlen(content),content);
    oldsource=fd_bind_sourcebase(pathname);
    xml=fd_read_fdxml(&in,(FD_SLOPPY_XML|FD_XML_KEEP_RAW));
    fd_restore_sourcebase(oldsource);
    if (xml==NULL) {
      u8_free(content);
      u8_log(LOG_WARN,Startup,"ERROR","Error parsing %s",pathname);
      return FD_ERROR_VALUE;}
    parsed=xml->head;
    while ((FD_PAIRP(parsed)) &&
	   (FD_STRINGP(FD_CAR(parsed))) &&
	   (whitespace_stringp(FD_STRDATA(FD_CAR(parsed))))) {
      struct FD_PAIR *old_parsed=(struct FD_PAIR *)parsed;
      parsed=FD_CDR(parsed);
      old_parsed->cdr=FD_EMPTY_LIST;}
    env=(fd_lispenv)xml->data;
    lenv=(fdtype)env; ldata=parsed;
    if (traceweb>0)
      u8_log(LOG_NOTICE,"LOADED","Loaded %s in %f secs",
		pathname,u8_elapsed_time()-load_start);
    u8_free(content); u8_free(xml);
    return fd_init_pair(NULL,ldata,lenv);}
  else {
    fd_environment newenv=
      ((server_env) ? (fd_make_env(fd_make_hashtable(NULL,17),server_env)) :
       (fd_working_environment()));
    fdtype main_proc, load_result;
    /* We reload the file.  There should really be an API call to
       evaluate a source string (fd_eval_source?).  This could then
       use that. */
    u8_free(content);
    load_result=fd_load_source(pathname,newenv,NULL);
    if (FD_TROUBLEP(load_result)) return load_result;
    fd_decref(load_result);
    main_proc=fd_eval(main_symbol,newenv);
    if (traceweb>0)
      u8_log(LOG_NOTICE,"LOADED","Loaded %s in %f secs",
		pathname,u8_elapsed_time()-load_start);
    return fd_init_pair(NULL,main_proc,(fdtype)newenv);}
}

static fdtype getcontent(fdtype path)
{
  if (FD_STRINGP(path)) {
    fdtype value=fd_hashtable_get(&pagemap,path,FD_VOID);
    if (FD_VOIDP(value)) {
      fdtype table_value, content;
      struct stat fileinfo; struct U8_XTIME mtime;
      char *lpath=u8_localpath(FD_STRDATA(path));
      int retval=stat(lpath,&fileinfo);
      if (retval<0) {
	u8_graberr(-1,"getcontent",lpath);
	return FD_ERROR_VALUE;}
      u8_init_xtime(&mtime,fileinfo.st_mtime,u8_second,0,0,0);
      content=loadcontent(path);
      if (FD_ABORTP(content)) {
	u8_free(lpath);
	return content;}
      table_value=fd_init_pair(NULL,fd_make_timestamp(&mtime),
			       fd_incref(content));
      fd_hashtable_store(&pagemap,path,table_value);
      u8_free(lpath);
      fd_decref(table_value);
      return content;}
    else {
      fdtype tval=FD_CAR(value), cval=FD_CDR(value);
      struct FD_TIMESTAMP *lmtime=
	FD_GET_CONS(tval,fd_timestamp_type,FD_TIMESTAMP *);
      struct stat fileinfo;
      char *lpath=u8_localpath(FD_STRDATA(path));
      if (stat(lpath,&fileinfo)<0) {
	u8_graberr(-1,"getcontent",u8_strdup(FD_STRDATA(path)));
	u8_free(lpath); fd_decref(value);
	return FD_ERROR_VALUE;}
      else if (fileinfo.st_mtime>lmtime->xtime.u8_tick) {
	fdtype new_content=loadcontent(path);
	struct U8_XTIME mtime;
	fdtype content_record;
	u8_init_xtime(&mtime,fileinfo.st_mtime,u8_second,0,0,0);
	content_record=
	  fd_init_pair(NULL,fd_make_timestamp(&mtime),
		       fd_incref(new_content));
	fd_hashtable_store(&pagemap,path,content_record);
	u8_free(lpath); fd_decref(content_record);
	if ((FD_PAIRP(value)) && (FD_PAIRP(FD_CDR(value))) &&
	    (FD_PRIM_TYPEP((FD_CDR(FD_CDR(value))),fd_environment_type))) {
	  fd_lispenv env=(fd_lispenv)(FD_CDR(FD_CDR(value)));
	  if (FD_HASHTABLEP(env->bindings))
	    fd_reset_hashtable((fd_hashtable)(env->bindings),0,1);}
	fd_decref(value);
	return new_content;}
      else {
	fdtype retval=fd_incref(cval);
	fd_decref(value);
	u8_free(lpath);
	return retval;}}}
  else {
    return fd_type_error("pathname","getcontent",path);}
}

/* Document generation */

#define write_string(sock,string) u8_writeall(sock,string,strlen(string))

static void output_content(fd_webconn ucl,fdtype content)
{
  if (FD_STRINGP(content))
    u8_writeall(ucl->socket,FD_STRDATA(content),FD_STRLEN(content));
  else if (FD_PACKETP(content))
    u8_writeall(ucl->socket,FD_PACKET_DATA(content),FD_PACKET_LENGTH(content));
  else {}
}

/* The error page */

/* Running the server */

static u8_client simply_accept(int sock,struct sockaddr *addr,int len)
{
  fd_webconn consed=u8_alloc(FD_WEBCONN);
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
  double start_time=u8_elapsed_time();
  double setup_time, parse_time, exec_time, write_time;
  struct rusage start_usage, end_usage;
  int forcelog=0;
  if ((status_interval>=0)&&(u8_microtime()>last_status+status_interval))
    report_status();
  /* Do this ASAP to avoid session leakage */
  fd_reset_threadvars();
  /* Clear outstanding errors from the last session */
  fd_clear_errors(1);
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
    cgidata=fd_dtsread_dtype(&(client->in)), result;
    path=fd_get(cgidata,script_filename,FD_VOID);
    if (traceweb>0) {
      fdtype referer=fd_get(cgidata,referer_symbol,FD_VOID);
      fdtype remote=fd_get(cgidata,remote_info_symbol,FD_VOID);
      fdtype uri=fd_get(cgidata,uri_symbol,FD_VOID);
      if ((FD_STRINGP(uri)) &&  (FD_STRINGP(referer)) && (FD_STRINGP(remote)))
	u8_log(LOG_NOTICE,"REQUEST","Handling request for %s from %s by %s",
	       FD_STRDATA(uri),FD_STRDATA(referer),FD_STRDATA(remote));
      else if ((FD_STRINGP(uri)) &&  (FD_STRINGP(remote)))
	u8_log(LOG_NOTICE,"REQUEST","Handling request for %s by %s",
	       FD_STRDATA(uri),FD_STRDATA(remote));
      else if ((FD_STRINGP(uri)) &&  (FD_STRINGP(referer)))
	u8_log(LOG_NOTICE,"REQUEST","Handling request for %s from %s",
	       FD_STRDATA(uri),FD_STRDATA(referer));
      else if (FD_STRINGP(uri))
	u8_log(LOG_NOTICE,"REQUEST","Handling request for %s",FD_STRDATA(uri));
      fd_decref(referer);
      fd_decref(uri);}
    proc=getcontent(path);
    fd_parse_cgidata(cgidata);
    parse_time=u8_elapsed_time();
    if ((reqlog) || (urllog))
      dolog(cgidata,FD_NULL,NULL,parse_time-start_time);}
  u8_getrusage(RUSAGE_SELF,&start_usage);
  fd_set_default_output(&(client->out));
  fd_use_reqinfo(cgidata);
  fd_thread_set(browseinfo_symbol,FD_EMPTY_CHOICE);
  if (FD_ABORTP(proc)) result=fd_incref(proc);
  else if (FD_PRIM_TYPEP(proc,fd_sproc_type)) {
    if (traceweb>1)
      u8_log(LOG_NOTICE,"START","Handling %q with Scheme procedure %q",path,proc);
    result=fd_cgiexec(proc,cgidata,0);}
  else if ((FD_PAIRP(proc)) && (FD_PRIM_TYPEP((FD_CAR(proc)),fd_sproc_type))) {
    if (traceweb>1)
      u8_log(LOG_NOTICE,"START","Handling %q with Scheme procedure %q",path,proc);
    result=fd_cgiexec(FD_CAR(proc),cgidata,0);}
  else if (FD_PAIRP(proc)) {
    fdtype xml=FD_CAR(proc), lenv=FD_CDR(proc), setup_proc=FD_VOID;
    fd_lispenv base=((FD_PTR_TYPEP(lenv,fd_environment_type)) ?
		     (FD_GET_CONS(FD_CDR(proc),fd_environment_type,fd_environment)) :
		     (NULL));
    fd_lispenv runenv=fd_make_env(fd_incref(cgidata),base);
    if (base) fd_load_latest(NULL,base,NULL);
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
    u8_context excxt=((ex->u8x_context) ? (ex->u8x_context) : ((u8_context)"somewhere"));
    u8_context exdetails=((ex->u8x_details) ? (ex->u8x_details) : ((u8_string)"no more details"));
    fdtype irritant=fd_exception_xdata(ex);
    if (FD_VOIDP(irritant))
      u8_log(LOG_INFO,excond,"Unexpected error \"%m \"for %s:@%s (%s)",
	     excond,FD_STRDATA(path),excxt,exdetails);
    else u8_log(LOG_INFO,excond,"Unexpected error \"%m\" for %s:%s (%s) %q",
		excond,FD_STRDATA(path),excxt,exdetails,irritant);
    write_string(client->socket,
		 "Content-type: text/html; charset='utf-8'\r\n\r\n");
    fd_xhtmlerrorpage(&(client->out),ex);
    u8_free_exception(ex,1);
    if ((reqlog) || (urllog))
      dolog(cgidata,result,client->out.u8_outbuf,u8_elapsed_time()-start_time);
    u8_writeall(client->socket,client->out.u8_outbuf,
		client->out.u8_outptr-client->out.u8_outbuf);
    u8_client_close(ucl);}
  else {
    U8_OUTPUT tmp; int retval, tracep;
    fdtype content=fd_get(cgidata,content_slotid,FD_VOID);
    fdtype traceval=fd_get(cgidata,tracep_slotid,FD_VOID);
    fdtype retfile=((FD_VOIDP(content))?
		    (fd_get(cgidata,retfile_slotid,FD_VOID)):
		    (FD_VOID));
    if (FD_VOIDP(traceval)) tracep=0; else tracep=1;
    U8_INIT_OUTPUT(&tmp,1024);
    fd_output_http_headers(&tmp,cgidata);
    /* if (tracep) fprintf(stderr,"%s\n",tmp.u8_outbuf); */
    u8_writeall(client->socket,tmp.u8_outbuf,tmp.u8_outptr-tmp.u8_outbuf);
    tmp.u8_outptr=tmp.u8_outbuf;
    if ((FD_VOIDP(content))&&(FD_VOIDP(retfile))) {
      /* Normal case, when the output is just sent to the client */
      if (write_headers) {
		write_headers=fd_output_xhtml_preface(&tmp,cgidata);
	u8_writeall(client->socket,tmp.u8_outbuf,tmp.u8_outptr-tmp.u8_outbuf);}
      retval=u8_writeall(client->socket,client->out.u8_outbuf,
			 client->out.u8_outptr-client->out.u8_outbuf);
      if (write_headers)
	write_string(client->socket,"</body>\n</html>\n");}
    else if (FD_STRINGP(retfile)) {
      /* This needs more error checking, signallin */
      u8_string filename=FD_STRDATA(retfile);
      FILE *f=u8_fopen(filename,"rb");
      if (f) {
	int bytes_read=0; unsigned char buf[32768];
	while ((bytes_read=fread(buf,sizeof(unsigned char),32768,f))>0) {
	  int retval=u8_writeall(client->socket,buf,bytes_read);
	  if (retval<0) break;}
	fclose(f);}}
      else {
      /* Where the servlet has specified some particular content */
      output_content(client,content);
      client->out.u8_outptr=client->out.u8_outbuf;}
    u8_client_close(ucl);
    u8_free(tmp.u8_outbuf); fd_decref(content); fd_decref(traceval);
    if ((reqlog) || (urllog))
      dolog(cgidata,result,client->out.u8_outbuf,u8_elapsed_time()-start_time);}
  if (fd_test(cgidata,cleanup_slotid,FD_VOID)) {
    fdtype cleanup=fd_get(cgidata,cleanup_slotid,FD_EMPTY_CHOICE);
    FD_DO_CHOICES(cl,cleanup) {
      fdtype retval=fd_apply(cleanup,0,NULL);
      fd_decref(retval);}}
  forcelog=fd_req_test(forcelog_symbol,FD_VOID);
  fd_use_reqinfo(FD_EMPTY_CHOICE);
  fd_thread_set(browseinfo_symbol,FD_VOID);
  fd_clear_errors(1);
  write_time=u8_elapsed_time();
  u8_getrusage(RUSAGE_SELF,&end_usage);
  if ((forcelog)||(traceweb>0)||((overtime>0)&&((write_time-start_time)>overtime))) {
    fdtype query=fd_get(cgidata,query_symbol,FD_VOID);
    if (FD_VOIDP(query))
      u8_log(LOG_NOTICE,"DONE",
	     "Handled %q in %f=setup:%f+req:%f+run:%f+write:%f secs, stime=%.2fms, utime=%.2fms.",
	     path,write_time-start_time,
	     setup_time-start_time,
	     parse_time-setup_time,
	     exec_time-parse_time,
	     write_time-exec_time,
	     (u8_dbldifftime(end_usage.ru_utime,start_usage.ru_utime))/1000.0,
	     (u8_dbldifftime(end_usage.ru_stime,start_usage.ru_stime))/1000.0);
    else u8_log(LOG_NOTICE,"DONE",
		"Handled %q q=%q in %f=setup:%f+req:%f+run:%f+write:%f secs, stime=%.2fms, utime=%.2fms.",
		path,query,
		write_time-start_time,
		setup_time-start_time,
		parse_time-setup_time,
		exec_time-parse_time,
		write_time-exec_time,
		(u8_dbldifftime(end_usage.ru_utime,start_usage.ru_utime))/1000.0,
		(u8_dbldifftime(end_usage.ru_stime,start_usage.ru_stime))/1000.0);
    if ((forcelog)||((overtime>0)&&((write_time-start_time)>overtime))) {
      u8_string cond=(((overtime>0)&&((write_time-start_time)>overtime))?
		      "OVERTIME":"FORCELOG");
      u8_string before=u8_rusage_string(&start_usage);
      u8_string after=u8_rusage_string(&end_usage);
      u8_log(LOG_NOTICE,"OVERTIME","before: %s",before);
      u8_log(LOG_NOTICE,"OVERTIME"," after: %s",after);
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
  return 1;
}

static int close_webclient(u8_client ucl)
{
  fd_webconn client=(fd_webconn)ucl;
  fd_dtsclose(&(client->in),2);
  u8_close((u8_stream)&(client->out));
}

static char *portfile=NULL;

static void shutdown_fdwebserver(u8_condition reason)
{
  if (reason) 
    u8_log(LOG_WARN,reason,
	   "Unexpected shutdown, removing socket %s and pidfile %s",
	   portfile,pidfile);
  u8_server_shutdown(&fdwebserver);
  if (portfile)
    if (remove(portfile)>=0) {
      u8_free(portfile); portfile=NULL;}
  if (pidfile) u8_removefile(pidfile);
  pidfile=NULL;
  fd_recycle_hashtable(&pagemap);
}

static void signal_shutdown(int sig)
{
#ifdef SIGHUP
  if (sig==SIGHUP) {
    shutdown_fdwebserver("SIGHUP"); return;}
#endif
#ifdef SIGHUP
  if (sig==SIGQUIT) {
    shutdown_fdwebserver("SIGQUIT"); return;}
#endif
#ifdef SIGHUP
  if (sig==SIGTERM) {
    shutdown_fdwebserver("SIGTERM"); return;}
#endif
  shutdown_fdwebserver("SIGTERM");
  return;  
}

static void normal_shutdown()
{
  shutdown_fdwebserver(NULL);
}

static void init_symbols()
{
  uri_symbol=fd_intern("REQUEST_URI");
  query_symbol=fd_intern("QUERY_STRING");
  main_symbol=fd_intern("MAIN");
  setup_symbol=fd_intern("SETUP");
  cgisymbol=fd_intern("CGIDATA");
  script_filename=fd_intern("SCRIPT_FILENAME");
  doctype_slotid=fd_intern("DOCTYPE");
  xmlpi_slotid=fd_intern("XMLPI");
  content_type=fd_intern("CONTENT-TYPE");
  content_slotid=fd_intern("CONTENT");
  retfile_slotid=fd_intern("RETFILE");
  cleanup_slotid=fd_intern("CLEANUP");
  html_headers=fd_intern("HTML-HEADERS");
  http_headers=fd_intern("HTTP-HEADERS");
  tracep_slotid=fd_intern("TRACEP");
  err_symbol=fd_intern("%ERR");
  response_symbol=fd_intern("%RESPONSE");
  browseinfo_symbol=fd_intern("BROWSEINFO");
  referer_symbol=fd_intern("HTTP_REFERER");
  remote_info_symbol=fd_intern("REMOTE_INFO");
  forcelog_symbol=fd_intern("FORCELOG");
}

/* Utility functions */

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

static fdtype get_servlet_status()
{
  u8_string status=u8_server_status(&fdwebserver,NULL,0);
  return fd_lispstring(status);
}


/* Making sure you can write the socket file */

static int mkdir_recursive(u8_string path,mode_t mode)
{
  if (u8_directoryp(path)) return 0;
  else {
    u8_string dirname=u8_dirname(path);
    int retval=mkdir_recursive(dirname,mode);
    u8_free(dirname);
    if (retval<0) return -1;
    retval=mkdir(u8_tolibc(path),mode);
    if (retval<0) {
      u8_graberr(retval,"mkdir_recursive",u8_strdup(path));
      return retval;}
    return 1;}
}

#define SOCKDIR_PERMISSIONS \
  (S_IRUSR|S_IWUSR|S_IXUSR|S_IRGRP|S_IWGRP|S_IXGRP|S_IROTH|S_IXOTH)

static int check_socket_path(char *sockarg)
{
  u8_string sockname=u8_fromlibc(sockarg);
  u8_string sockdir=u8_dirname(sockname);
  int retval=mkdir_recursive(sockdir,SOCKDIR_PERMISSIONS);
  if (retval<0) {
    u8_free(sockname);
    return retval;}
  else if ((u8_file_existsp(sockname)) ?
	   (u8_file_writablep(sockname)) :
	   (u8_file_writablep(sockdir))) {
    u8_free(sockname);
    return retval;}
  else {
    u8_seterr(fd_CantWrite,"check_socket_path",sockname);
    return -1;}
}

/* The main() event */

static void doexit(int sig)
{
  exit(0);
}

FD_EXPORT void fd_init_dbfile(void); 

int main(int argc,char **argv)
{
  int u8_version=u8_initialize();
  int fd_version; /* Wait to set this until we have a log file */
  unsigned char data[1024], *input;
  int i=1, n_threads=-1, n_tasks=-1;
  u8_string source_file=NULL;
  u8_string socket_path=NULL;

  /* Set this here, before processing any configs */
  fddb_loglevel=LOG_INFO;
  
  if (argc<2) {
    fprintf(stderr,"Usage: fdserv <socketfile> [config]*\n");
    exit(2);}
  
  while (i<argc)
    if (strchr(argv[i],'=')) i++;
    else if (socket_path) i++;
    else socket_path=argv[i++];
  i=1;
  if (!(socket_path)) {
    fprintf(stderr,"Usage: fdserv <socketfile> [config]*\n");
    exit(2);}
  else if (check_socket_path(socket_path)<0) {
    u8_clear_errors(1);
    return -1;}
  
  u8_log(LOG_WARN,Startup,"LOGFILE='%s'",getenv("LOGFILE"));

  /* We do this using the Unix environment (rather than configuration
      variables) because we want to redirect errors from the configuration
      variables themselves and we want to be able to set this in the
      environment we wrap around calls. */
  if (getenv("LOGFILE")) {
    char *logfile=u8_strdup(getenv("LOGFILE"));
    int log_fd=open(logfile,O_RDWR|O_APPEND|O_CREAT|O_SYNC,0644);
    if (log_fd<0) {
      u8_log(LOG_WARN,Startup,"Couldn't open log file %s",logfile);
      exit(1);}
    dup2(log_fd,1);
    dup2(log_fd,2);}

  if (!(check_pid_file(socket_path))) return -1;
  
  fd_version=fd_init_fdscheme();
  
  /* We register this module so that we can have pages that use the functions,
     for instance with an HTTP PROXY that can be used as a dtype server */

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

  u8_log_show_date=1;
  u8_log_show_procinfo=1;
  u8_use_syslog(1);

#if ((!(HAVE_CONSTRUCTOR_ATTRIBUTES)) || (FD_TESTCONFIG))
  fd_init_fdscheme();
  fd_init_schemeio();
  fd_init_texttools();
  fd_init_tagger();
  fd_init_fddbserv();
#else
  FD_INIT_SCHEME_BUILTINS();
  fd_init_fddbserv();
#endif

  fd_register_module("FDBSERV",fd_incref(fd_fdbserv_module),FD_MODULE_SAFE);
  fd_finish_module(fd_fdbserv_module);
  fd_persist_module(fd_fdbserv_module);

  fd_init_fdweb();
  fd_init_dbfile(); 
  init_symbols();
  
  if (server_env==NULL)
    server_env=fd_working_environment();
  fd_idefn((fdtype)server_env,fd_make_cprim0("BOOT-TIME",get_boot_time,0));
  fd_idefn((fdtype)server_env,fd_make_cprim0("UPTIME",get_uptime,0));
  fd_idefn((fdtype)server_env,
	   fd_make_cprim0("SERVLET-STATUS",get_servlet_status,0));


  fd_register_config("TRACEWEB",_("Trace all web transactions"),
		     fd_boolconfig_get,fd_boolconfig_set,&traceweb);
  fd_register_config("OVERTIME",_("Trace web transactions over N seconds"),
		     fd_dblconfig_get,fd_dblconfig_set,&traceweb);
  fd_register_config("PRELOAD",
		     _("Files to preload into the shared environment"),
		     preload_get,preload_set,NULL);
  fd_register_config("BACKLOG",
		     _("Number of pending connection requests allowed"),
		     fd_intconfig_get,fd_intconfig_set,&max_backlog);
  fd_register_config("MAXQUEUE",_("Max number of requests to keep queued"),
		     fd_intconfig_get,fd_intconfig_set,&servlet_ntasks);
  fd_register_config("NTHREADS",_("Number of threads in the thread pool"),
		     fd_intconfig_get,fd_intconfig_set,&servlet_threads);
  fd_register_config("URLLOG",_("Where to write URLs where were requested"),
		     urllog_get,urllog_set,NULL);
  fd_register_config("REQLOG",_("Where to write request objects"),
		     reqlog_get,reqlog_set,NULL);
  fd_register_config("REQLOGLEVEL",_("Level of transaction logging"),
		     fd_intconfig_get,fd_intconfig_set,&reqloglevel);
  fd_register_config("STATLOG",_("File for recording status reports"),
		     statlog_get,statlog_set,NULL);
  fd_register_config
    ("STATINTERVAL",_("Milliseconds (roughly) between status reports"),
     statinterval_get,statinterval_set,NULL);

#if FD_THREADS_ENABLED
  fd_init_mutex(&log_lock);
#endif

  u8_log(LOG_NOTICE,"LAUNCH","fdserv %s",socket_path);

  while (i<argc)
    if (strchr(argv[i],'=')) {
      u8_log(LOG_NOTICE,"CONFIG","   %s",argv[i]);
      fd_config_assignment(argv[i++]);}
    else i++;
  
  update_preloads();

  fd_make_hashtable(&pagemap,0);
  u8_server_init(&fdwebserver,
		 max_backlog,servlet_ntasks,servlet_threads,
		 simply_accept,webservefn,close_webclient);
  fdwebserver.flags=fdwebserver.flags|U8_SERVER_LOG_LISTEN;
  atexit(normal_shutdown);

#ifdef SIGHUP
  signal(SIGHUP,signal_shutdown);
#endif
#ifdef SIGTERM
  signal(SIGTERM,signal_shutdown);
#endif
#ifdef SIGQUIT
  signal(SIGQUIT,signal_shutdown);
#endif

#if HAVE_SIGPROCMASK
 {
   sigset_t newset, oldset, newer;
   sigemptyset(&newset);
   sigemptyset(&oldset);
   sigemptyset(&newer);
#if SIGQUIT
   sigaddset(&newset,SIGQUIT);
#endif
#if SIGTERM
   sigaddset(&newset,SIGTERM);
#endif
#if SIGHUP
   sigaddset(&newset,SIGHUP);
#endif
   if (sigprocmask(SIG_UNBLOCK,&newset,&oldset)<0)
     u8_log(LOG_WARN,"Sigerror","Error setting signal mask");
 }
#elif HAVE_SIGSETMASK
  /* We set this here because otherwise, it will often inherit
     the signal mask of its apache parent, which is inappropriate. */
 sigsetmask(0);
#endif

  /* We check this now, to kludge around some race conditions */
  if (u8_file_existsp(socket_path)) {
    if (((time(NULL))-(u8_file_mtime(socket_path)))<FD_LEFTOVER_AGE) {
      u8_log(LOG_CRIT,"FDSERV/SOCKETRACE",
	     "Aborting due to recent socket file %s",socket_path);
      return -1;}
    else {
      u8_log(LOG_WARN,"FDSERV/SOCKETZAP",
	     "Removing leftover socket file %s",socket_path);
      remove(socket_path);}}

 if (u8_add_server(&fdwebserver,socket_path,-1)<0) {
    fd_recycle_hashtable(&pagemap);
    fd_clear_errors(1);
    return -1;}
  chmod(socket_path,0777);

  portfile=u8_strdup(socket_path);

  u8_log(LOG_INFO,NULL,
	 "FramerD (%s) fdserv servlet running, %d/%d pools/indices",
	 FDB_SVNREV,fd_n_pools,
	 fd_n_primary_indices+fd_n_secondary_indices);
  u8_message("beingmeta FramerD, (C) beingmeta 2004-2011, all rights reserved");
  u8_server_loop(&fdwebserver);

  if (pidfile) {
    u8_removefile(pidfile);
    u8_free(pidfile);
    pidfile=NULL;}

  return 0;
}

