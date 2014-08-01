/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2014 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/tables.h"
#include "framerd/support.h"
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
#if HAVE_SYS_WAIT_H
#include <sys/wait.h>
#endif
#include <time.h>
#include <signal.h>
#include <math.h>

#if HAVE_FLOCK
#if HAVE_SYS_FILE_H
#include <sys/file.h>
#endif
#elif HAVE_LOCKF
#include <unistd.h>
#elif ((HAVE_FCNTL)&&(HAVE_FCNTL_H))
#include <unistd.h>
#include <fcntl.h>
#endif


#include "main.h"

/* This is the size of file to return all at once. */
#define FD_FILEBUF_MAX (256*256)

#include "main.c"

static u8_condition ServletStartup=_("FDServlet Startup");
static u8_condition NoServers=_("No servers configured");
#define Startup ServletStartup

FD_EXPORT int fd_init_fddbserv(void);

#include "webcommon.h"

#define nobytes(in,nbytes) (FD_EXPECT_FALSE(!(fd_needs_bytes(in,nbytes))))
#define havebytes(in,nbytes) (FD_EXPECT_TRUE(fd_needs_bytes(in,nbytes)))

#define HTML_UTF8_CTYPE_HEADER "Content-type: text/html; charset=utf-8\r\n\r\n"

/* This lets the u8_server loop do I/O buffering to keep threads from
   waiting on I/O. */
static int async_mode=1;

/* Logging declarations */
static FILE *statlog=NULL; int statusout=-1;
static double overtime=0;
static int stealsockets=0;

/* Tracking ports */
static u8_string server_id=NULL;
static u8_string *ports=NULL;
static int n_ports=0, max_ports=0;
static u8_mutex server_port_lock;

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

u8_string pid_file=NULL;

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

#ifndef DEFAULT_POLL_TIMEOUT
#define DEFAULT_POLL_TIMEOUT 1000
#endif

static int poll_timeout=DEFAULT_POLL_TIMEOUT;

static fd_lispenv server_env;
struct U8_SERVER fdwebserver;
static int server_running=0;

static u8_condition fdservWriteError="FDServelt write error";

/* STATLOG config */

/* The statlog is a plain text (UTF-8) file which contains server
   connection/thread information.  */

#ifndef DEFAULT_STATUS_INTERVAL
#define DEFAULT_STATUS_INTERVAL 1000000
#endif
#ifndef DEFAULT_STATLOG_INTERVAL
#define DEFAULT_STATLOG_INTERVAL 60000000
#endif

static u8_string statlogfile, statfile;
static long long last_status=0, last_statlog=0;
static long status_interval=DEFAULT_STATUS_INTERVAL;
static long statlog_interval=DEFAULT_STATUS_INTERVAL;

static void close_statlog();

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
    if ((statlog==NULL)&&(u8_file_existsp(statlogfile))) {
      int rv=u8_removefile(statlogfile);
      if (rv>=0) statlog=u8_fopen_locked(statlogfile,"a");}
    if (statlog) {
      u8_string tmp;
      tmp=u8_mkstring("# Log open %*lt for %s\n",u8_sessionid());
      u8_chmod(statlogfile,0774);
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
    close_statlog();
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
  else if (FD_FALSEP(val)) status_interval=-1;
  else if (FD_STRINGP(val)) {
    int flag=fd_boolstring(FD_STRDATA(val),-1);
    if (flag<0) {
      u8_log(LOG_WARN,"statinterval_set","Unknown value: %s",FD_STRDATA(val));
      return 0;}
    else if (flag) {
      if (status_interval>0) return 0;
      status_interval=DEFAULT_STATUS_INTERVAL;}
    else if (status_interval<0)
      return 0;
    else status_interval=-1;}
  else return fd_reterr
	 (fd_TypeError,"statinterval_set",
	  u8_strdup(_("fixnum")),val);
  return 1;
}

static fdtype statloginterval_get(fdtype var,void *data)
{
  if (statlog_interval<0) return FD_FALSE;
  else return FD_FIX2INT(statlog_interval);
}

static int statloginterval_set(fdtype var,fdtype val,void *data)
{
  if (FD_FIXNUMP(val)) {
    int intval=FD_FIX2INT(val);
    if (intval>=0)  statlog_interval=intval*1000;
    else {
      return fd_reterr
	(fd_TypeError,"statloginterval_set",
	 u8_strdup(_("fixnum")),val);}}
  else if (FD_FALSEP(val)) statlog_interval=-1;
  else if (FD_STRINGP(val)) {
    int flag=fd_boolstring(FD_STRDATA(val),-1);
    if (flag<0) {
      u8_log(LOG_WARN,"statloginterval_set","Unknown value: %s",FD_STRDATA(val));
      return 0;}
    else if (flag) {
      if (statlog_interval>0) return 0;
      statlog_interval=DEFAULT_STATLOG_INTERVAL;}
    else if (statlog_interval<0)
      return 0;
    else statlog_interval=-1;}
  else return fd_reterr
	 (fd_TypeError,"config_set_statloginterval",
	  u8_strdup(_("fixnum")),val);
  return 1;
}

static fdtype statinterval_get(fdtype var,void *data)
{
  if (status_interval<0) return FD_FALSE;
  else return FD_FIX2INT(status_interval);
}

#define STATUS_LINE_CURRENT \
  "[%*t] %d/%d/%d/%d current busy/waiting/connections/threads\t\t[@%f]\n"
#define STATUS_LINE_AGGREGATE \
  "[%*t] %d/%d/%d connected/requested/failed over %0.3f%s uptime\n"
#define STATUS_LINE_TIMING \
  "[%*t] Average %0.3f%s response (max=%0.3f%s), %0.3f%s run (max=%0.3f%s)\n"
#define STATUS_LOG_SNAPSHOT \
  "[%*t][%f] %d/%d/%d/%d busy/waiting/connections/threads, %d/%d/%d reqs/resps/errs, response: %0.2fus, max=%ldus, run: %0.2fus, max=%ldus"
#define STATUSLOG_LINE "%*t\t%f\t%d\t%d\t%d\t%d\t%0.2f\t%0.2f\n"
#define STATUS_LINEXN "[%*t][%f] %s: %s mean=%0.2fus max=%lldus sd=%0.2f (n=%d)\n"
#define STATUS_LINEX "%s: %s mean=%0.2fus max=%lldus sd=%0.2f (n=%d)"

#define stdev(v,v2,n) \
  ((double)(sqrt((((double)v2)/((double)n))-			\
		 ((((double)v)/((double)n))*(((double)v)/((double)n))))))
#define getmean(v,n) (((double)v)/((double)n))

static int log_status=-1;

static double getinterval(double usecs,char **units)
{
  if (usecs>259200000000.0) {
    *units="days"; return (usecs/86400000000.0);}
  else if (usecs>4800000000.0) {
    *units="hours"; return (usecs/3600000000.0);}
  else if (usecs>60000000.0) {
    *units="min"; return (usecs/60000000.0);}
  else if (usecs>1000000.0) {
    *units="s"; return (usecs/1000000.0);}
  else if (usecs>1000.0) {
    *units="ms"; return (usecs/1000.0);}
  else {*units="us"; return usecs;}
}


static void update_status()
{
  double elapsed=u8_elapsed_time();
  struct U8_SERVER_STATS stats;
  FILE *log=statlog; int mon=statusout;
  if (((mon<0)&&(statfile))||
      ((!(log))&&(statlogfile))) {
    fd_lock_mutex(&log_lock);
    if (statusout>=0) mon=statusout;
    else if (statfile) {
      mon=statusout=(open(statfile,O_WRONLY|O_CREAT,0644));
      if (mon>=0) u8_lock_fd(mon,1);}
    else mon=-1;
    if (statlog) log=statlog;
    else if (statlogfile) 
      log=statlog=(u8_fopen_locked(statlogfile,"a"));
    else log=NULL;
    fd_unlock_mutex(&log_lock);}
  if (mon>=0) {
    off_t rv=lseek(mon,0,SEEK_SET);
    if (rv>=0) rv=ftruncate(mon,0);
    if (rv<0) {
      u8_log(LOG_WARN,"output_status","File truncate failed (%s:%d)",
	     strerror(errno),errno);
      errno=0;}
    else {
      rv=lseek(mon,0,SEEK_SET); errno=0;}}

  u8_server_statistics(&fdwebserver,&stats);
  
  if ((log!=NULL)||(log_status>0)) {
    long long now=u8_microtime();
    if ((statlog_interval>=0)&&(now>(last_statlog+statlog_interval))) {
      last_statlog=now;
      if (log_status>0)
	u8_log(log_status,"FDServlet",STATUS_LOG_SNAPSHOT,elapsed,
	       fdwebserver.n_busy,fdwebserver.n_queued,
	       fdwebserver.n_clients,fdwebserver.n_threads,
	       fdwebserver.n_accepted,fdwebserver.n_trans,fdwebserver.n_errs,
	       (((double)(stats.tsum))/((double)(stats.tsum))),stats.tmax,
	       (((double)(stats.xsum))/((double)(stats.xsum))),stats.xmax);
      if (log)
	u8_fprintf(log,"%*t\t%f\t%d\t%d\t%d\t%d\t%0.2f\t%0.2f\n",elapsed,
		   fdwebserver.n_busy,fdwebserver.n_queued,
		   fdwebserver.n_clients,fdwebserver.n_threads,
		   (((double)(stats.tsum))/((double)(stats.tsum))),
		   (((double)(stats.xsum))/((double)(stats.xsum))));}}
      
  if (mon>=0) {
    int written=0, len, delta=0;
    double rmean=((stats.tcount)?
		  (((double)(stats.tsum))/((double)(stats.tcount))):
		  (0.0));
    double rmax=(double)(stats.rmax);
    double xmean=((stats.xcount)?
		  (((double)(stats.xsum))/((double)(stats.xcount))):
		  (0.0));
    double xmax=(double)(stats.xmax);
    char *rmean_units, *rmax_units, *xmean_units, *xmax_units;
    double uptime; char *uptime_units;
    struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,4096);
    u8_printf(&out,STATUS_LINE_CURRENT,
	      fdwebserver.n_busy,fdwebserver.n_queued,
	      fdwebserver.n_clients,fdwebserver.n_threads,
	      elapsed);
    uptime=getinterval(elapsed,&uptime_units);
    u8_printf(&out,STATUS_LINE_AGGREGATE,
	      fdwebserver.n_accepted,fdwebserver.n_trans,fdwebserver.n_errs,
	      uptime,uptime_units);
    rmean=getinterval(rmean,&rmean_units); rmax=getinterval(rmax,&rmax_units);
    xmean=getinterval(xmean,&xmean_units); xmax=getinterval(xmax,&xmax_units);
    u8_printf(&out,STATUS_LINE_TIMING,
	      rmean,rmean_units,rmax,rmax_units,
	      xmean,xmean_units,xmax,xmax_units);
    u8_list_clients(&out,&fdwebserver);
    len=out.u8_outptr-out.u8_outbuf;
    while ((delta=write(mon,out.u8_outbuf+written,len-written))>0)
      written=written+delta;;
    fsync(mon);
    u8_free(out.u8_outbuf);}
}

static int statlog_server_update(struct U8_SERVER *server){
  long long now=u8_microtime();
  if ((status_interval>=0)&&(now>(last_status+status_interval))) {
    last_status=now;
    update_status();}
  return 0;}
static int statlog_client_update(struct U8_CLIENT *client){
  long long now=u8_microtime();
  if ((status_interval>=0)&&(u8_microtime()>(last_status+status_interval))) {
    last_status=now;
    update_status();}
  return 0;}

static void setup_status_file()
{
  if (statfile==NULL) {
    u8_string filename=fd_runbase_filename(".status");
    statfile=filename;}
}

static int lock_fd(int fd,int lock,int block)
{
  int lv=0;
#if ((HAVE_FLOCK)&&(HAVE_SYS_FILE_H))
  lv=flock(fd,((lock)?(LOCK_EX):(LOCK_UN))|((block)?(0):(LOCK_NB)));
#elif HAVE_LOCKF
  lv=lockf(fd,((lock)?(F_LOCK):(F_ULOCK))|((block)?(0):(F_TEST)),1);
else if HAVE_FCNTL
  struct flock lockinfo={F_WRLCK,SEEK_SET,0,1};
  if (lock) lockinfo.l_type=F_WRLCK; sle lockinfo.l_type=F_UNLCK;
  lv=fcntl(fd,((block)?(F_SETLKW):(F_SETLK)),&lockinfo);
#endif
  return lv;
}

static void cleanup_pid_file()
{
  u8_string exit_filename=fd_runbase_filename(".exit");
  FILE *exitfile=u8_fopen(exit_filename,"w");
  if (pid_file) {
    if (pid_fd>=0) {
      int lv=lock_fd(pid_fd,0,0);
      if (lv<0) {
	u8_log(LOG_CRIT,"Waiting to release lock on PID file %s",pid_file);
	errno=0; lv=lock_fd(pid_fd,0,1);}
      if (lv<0) {
	u8_graberr(errno,"cleanup_pid_file",u8_strdup(pid_file));
	fd_clear_errors(1);}
      close(pid_fd);
      pid_fd=-1;}
    u8_removefile(pid_file);}
  if (exitfile) {
    struct U8_XTIME xt; struct U8_OUTPUT out;
    char timebuf[64]; double elapsed=u8_elapsed_time();
    u8_now(&xt); U8_INIT_FIXED_OUTPUT(&out,sizeof(timebuf),timebuf);
    u8_xtime_to_iso8601(&out,&xt);
    fprintf(exitfile,"%d@%s(%f)\n",getpid(),timebuf,elapsed);
    fclose(exitfile);}
  u8_free(exit_filename);
}

static int write_pid_file()
{
  char *abspath=u8_abspath(pid_file,NULL);
  u8_byte buf[512]; 
  struct stat fileinfo;
  int lv, sv=stat(abspath,&fileinfo), stat_err=0, exists=0;
  if (sv<0) {stat_err=errno; errno=0;}
  else {
    exists=1;
    time_t ctime=fileinfo.st_ctime;
    time_t mtime=fileinfo.st_mtime;
    uid_t uid=fileinfo.st_uid;
    char cbuf[64], mbuf[64];
    u8_log(LOG_WARN,
	   "Existing PID file %s created %s, last modified %s, owned by %d",
	   ctime_r(&ctime,cbuf),ctime_r(&mtime,mbuf),
	   uid);}
  pid_fd=open(abspath,O_CREAT|O_RDWR|O_TRUNC|O_DIRECT,
	      S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP);
  if (pid_fd<0) {
    if (stat_err) u8_graberr(stat_err,"write_pid_file",u8_strdup(pid_file));
    u8_graberr(errno,"write_pid_file",u8_strdup(pid_file));
    return -1;}
  lv=lock_fd(pid_fd,1,0);
  if (lv<0) {
    u8_log(LOG_CRIT,"write_pid_file","Can't get lock on PID file %s",
	   pid_file);
    u8_graberr(errno,"write_pid_file",u8_strdup(pid_file));
    close(pid_fd);
    return -1;}
  else {
    if (exists)
      u8_log(LOGWARN,"write_pid_file","Bogarted existing PID file %s",abspath);
    sprintf(buf,"%d\n",getpid());
    write(pid_fd,buf,strlen(buf));
    atexit(cleanup_pid_file);
    /* It's now okay to steal sockets and other files */
    stealsockets=1;
    return pid_fd;}
}

static void close_statlog()
{
  if (statlog) {
    fd_lock_mutex(&log_lock);
    fclose(statlog); statlog=NULL;
    fd_unlock_mutex(&log_lock);}
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

/* Document generation */

#define write_string(sock,string) u8_writeall(sock,string,strlen(string))

static int output_content(fd_webconn ucl,fdtype content)
{
  if (FD_STRINGP(content)) {
    int rv=u8_writeall(ucl->socket,FD_STRDATA(content),FD_STRLEN(content));
    if (rv<0) {
      u8_log(LOG_CRIT,fdservWriteError,
	     "Unexpected error writing %ld bytes to mod_fdserv",
	     FD_STRLEN(content));
      return rv;}
    return FD_STRLEN(content);}
  else if (FD_PACKETP(content)) {
    int rv=u8_writeall(ucl->socket,FD_PACKET_DATA(content),FD_PACKET_LENGTH(content));
    if (rv<0) {
      u8_log(LOG_CRIT,fdservWriteError,
	     "Unexpected error writing %ld bytes to mod_fdserv",
	     FD_STRLEN(content));
      return rv;}
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
  u8_log(LOG_INFO,"webclient/open","Created web client (0x%lx) %s",
	 consed,
	 ((consed->idstring==NULL)?((u8_string)""):(consed->idstring)));
  return (u8_client) consed;
}

static int max_error_depth=128;

static int webservefn(u8_client ucl)
{
  fdtype proc=FD_VOID, result=FD_VOID;
  fdtype cgidata=FD_VOID, init_cgidata=FD_VOID, path=FD_VOID, precheck;
  fdtype content=FD_VOID, retfile=FD_VOID;
  fd_lispenv base_env=NULL;
  fd_webconn client=(fd_webconn)ucl;
  int write_headers=1, close_html=0;
  double start_time, setup_time, parse_time, exec_time, write_time;
  struct FD_THREAD_CACHE *threadcache=NULL;
  struct rusage start_usage, end_usage;
  double start_load[]={-1,-1,-1}, end_load[]={-1,-1,-1};
  int forcelog=0, retval=0;
  size_t http_len=0, head_len=0, content_len=0;
  fd_dtype_stream stream=&(client->in);
  u8_output outstream=&(client->out);
  int async=((async_mode)&&((client->server->flags)&U8_SERVER_ASYNC));
  int return_code=0, buffered=0, recovered=1;
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
  else if ((client->writing>0)&&(u8_client_finished(ucl))) {
    /* All done */
    if (ucl->status) {u8_free(ucl->status); ucl->status=NULL;}
    return 0;}
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
      int MAYBE_UNUSED dtcode=fd_dtsread_byte(stream);
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
	u8_log(LOG_NOTICE,"FDServlet/webservefn","Client %s (sock=%d) closing",
	       client->idstring,client->socket);
      if (ucl->status) {u8_free(ucl->status); ucl->status=NULL;}
      u8_client_close(ucl);
      return -1;}
    else if (!(FD_TABLEP(cgidata))) {
      u8_log(LOG_CRIT,"FDServlet/webservefn",
	     "Bad fdservlet request on client %s (sock=%d), closing",
	     client->idstring,client->socket);
      if (ucl->status) {u8_free(ucl->status); ucl->status=NULL;}
      u8_client_close(ucl);
      return -1;}
    else {}
    if (docroot) webcommon_adjust_docroot(cgidata,docroot);
    path=fd_get(cgidata,script_filename,FD_VOID);
    /* This is where we parse all the CGI variables, etc */
    fd_parse_cgidata(cgidata);
    forcelog=fd_req_test(forcelog_symbol,FD_VOID);
    if ((forcelog)||(traceweb>0)) {
      fdtype referer=fd_get(cgidata,referer_symbol,FD_VOID);
      fdtype remote=fd_get(cgidata,remote_info,FD_VOID);
      fdtype uri=fd_get(cgidata,uri_symbol,FD_VOID);
      if (FD_STRINGP(uri))
	ucl->status=u8_strdup(FD_STRDATA(uri));
      if ((FD_STRINGP(uri)) &&
	  (FD_STRINGP(referer)) &&
	  (FD_STRINGP(remote)))
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
	u8_log(LOG_NOTICE,"REQUEST","Handling request for %s (q=%s)",FD_STRDATA(uri));
      fd_decref(remote);
      fd_decref(referer);
      fd_decref(uri);}
    else {
      fdtype uri=fd_get(cgidata,uri_symbol,FD_VOID);
      if (FD_STRINGP(uri))
	ucl->status=u8_strdup(FD_STRDATA(uri));
      fd_decref(uri);}

    /* This is what we'll execute, be it a procedure or FDXML */
    proc=getcontent(path);}

  u8_set_default_output(outstream);
  init_cgidata=fd_deep_copy(cgidata);
  fd_use_reqinfo(cgidata); fd_reqlog(1);
  fd_thread_set(browseinfo_symbol,FD_EMPTY_CHOICE);
  parse_time=u8_elapsed_time();
  if ((reqlog) || (urllog) || (trace_cgidata))
    dolog(cgidata,FD_NULL,NULL,-1,parse_time-start_time);
  if (!(FD_ABORTP(proc)))
    precheck=run_preflight();
  if (FD_ABORTP(proc)) result=fd_incref(proc);
  else if (!((FD_FALSEP(precheck))||
	     (FD_VOIDP(precheck))||
	     (FD_EMPTY_CHOICEP(precheck))))
    result=precheck;
  else if (FD_SPROCP(proc)) {
    struct FD_SPROC *sp=FD_GET_CONS(proc,fd_sproc_type,fd_sproc);
    if ((forcelog)||(traceweb>1))
      u8_log(LOG_NOTICE,"START","Handling %q with Scheme procedure %q",
	     path,proc);
    base_env=sp->env;
    threadcache=checkthreadcache(sp->env);
    result=fd_cgiexec(proc,cgidata);}
  else if ((FD_PAIRP(proc))&&
	   (FD_SPROCP((FD_CAR(proc))))) {
    struct FD_SPROC *sp=FD_GET_CONS(FD_CAR(proc),fd_sproc_type,fd_sproc);
    if ((forcelog)||(traceweb>1))
      u8_log(LOG_NOTICE,"START","Handling %q with Scheme procedure %q",
	     path,proc);
    threadcache=checkthreadcache(sp->env);
    /* This should possibly put the CDR of proc into the environment chain,
       but it no longer does. ?? */
    base_env=sp->env;
    result=fd_cgiexec(FD_CAR(proc),cgidata);}
  else if (FD_PAIRP(proc)) {
    /* This is handling FDXML */
    fdtype lenv=FD_CDR(proc), setup_proc=FD_VOID;
    fd_lispenv base=
      ((FD_ENVIRONMENTP(lenv)) ?
       (FD_GET_CONS(FD_CDR(proc),fd_environment_type,fd_environment)) :
       (NULL));
    fd_lispenv runenv=fd_make_env(fd_incref(cgidata),base);
    base_env=base;
    if (base) fd_load_latest(NULL,base,NULL);
    threadcache=checkthreadcache(base);
    if ((forcelog)||(traceweb>1))
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
    /* We assume that the FDXML contains headers, so we won't add them. */
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
      result=fd_err(fd_TypeError,"FDServlet/content","string or packet",
		    content);}
    if ((!(FD_VOIDP(retfile)))&&
	((!(FD_STRINGP(retfile)))||
	 (!(u8_file_existsp(FD_STRDATA(retfile)))))) {
      fd_decref(result);
      result=fd_err(u8_CantOpenFile,"FDServlet/retfile","existing filename",
		    retfile);}}
  if (!(FD_TROUBLEP(result))) u8_set_default_output(NULL);
  else recovered=0;
  if (FD_TROUBLEP(result)) {
    u8_exception ex=u8_erreify(), exscan=ex;
    /* errorpage is used when errors occur.  Currently, it can be a
       procedure (to be called) or an HTML string to be returned.  */
    fdtype errorpage=((base_env)?
		      (fd_symeval(errorpage_symbol,base_env)):
		      (FD_VOID));
    int depth=0;
    if (((FD_VOIDP(errorpage))||(errorpage==FD_UNBOUND))&&
	(!(FD_VOIDP(default_errorpage)))) {
      fd_incref(default_errorpage);
      errorpage=default_errorpage;}
    while ((exscan)&&(depth<max_error_depth)) {
      /* Log everything, just in case */
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
      if (!(FD_VOIDP(irritant))) u8_log(LOG_ERR,excond,"Irritant: %q",irritant);
      exscan=exscan->u8x_prev; depth++;}
    /* First we try to apply the error page if it's defined */
    if (FD_APPLICABLEP(errorpage)) {
      fdtype err_value=fd_init_exception(NULL,ex);
      fd_push_reqinfo(init_cgidata);
      fd_store(init_cgidata,error_symbol,err_value); fd_decref(err_value);
      fd_store(init_cgidata,reqdata_symbol,cgidata); fd_decref(cgidata);
      if (outstream->u8_outptr>outstream->u8_outbuf) {
	/* Get all the output to date as a string and store it in the
	   request. */
	fdtype output=fd_make_string
	  (NULL,outstream->u8_outptr-outstream->u8_outbuf,
	   outstream->u8_outbuf);
	/* Save the output to date on the request */
	fd_store(init_cgidata,output_symbol,output);
	fd_decref(output);}
      fd_decref(cgidata);
      cgidata=fd_deep_copy(init_cgidata);
      fd_use_reqinfo(cgidata); 
      /* Reset the output stream */
      outstream->u8_outptr=outstream->u8_outbuf;
      /* Apply the error page object */
      result=fd_cgiexec(errorpage,cgidata);
      if (FD_ABORTP(result)) {
	u8_exception newex=u8_current_exception, lastex=newex;
	fdtype crisispage=((base_env)?
			  (fd_symeval(crisispage_symbol,base_env)):
			  (FD_VOID));
	if (((FD_VOIDP(crisispage))||(crisispage==FD_UNBOUND))&&
	    (!(FD_VOIDP(default_crisispage)))) {
	  fd_incref(default_crisispage);
	  crisispage=default_crisispage;}
	exscan=newex; depth=0;
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
	  if (!(FD_VOIDP(irritant))) u8_log(LOG_ERR,excond,"Irritant: %q",irritant);
	  lastex=exscan; exscan=exscan->u8x_prev; depth++;}
	while (exscan) {
	  lastex=exscan; exscan=exscan->u8x_prev; depth++;}
	/* Add the previous exception to this one as we go forward */
	if (lastex) lastex->u8x_prev=ex;
	fd_decref(errorpage); errorpage=FD_VOID;
	if (FD_STRINGP(crisispage)) errorpage=crisispage;}
      else {
	fd_clear_errors(1);
	recovered=1;}}
    if (!(FD_TROUBLEP(result))) {
      /* We got something to return, so we don't bother
	 with all the various other error cases.  */ }
    else if ((FD_STRINGP(errorpage))&&
	     (strstr(FD_STRDATA(errorpage),"\n")!=NULL)) {
      /* Assume that the error page is a string of HTML */
      ex=u8_erreify();
      http_len=http_len+strlen(HTML_UTF8_CTYPE_HEADER);
      write_string(client->socket,HTML_UTF8_CTYPE_HEADER);
      write_string(client->socket,FD_STRDATA(errorpage));}
    else if ((FD_STRINGP(errorpage))&&
	     ((FD_STRDATA(errorpage)[0]=='/')||
	      (u8_has_prefix(FD_STRDATA(errorpage),"http:",0))||
	      (u8_has_prefix(FD_STRDATA(errorpage),"https:",0)))) {
      struct U8_OUTPUT tmpout; U8_INIT_OUTPUT(&tmpout,1024);
      write_string(client->socket,"Status: 307\r\nLocation: ");
      write_string(client->socket,FD_STRDATA(errorpage));
      write_string(client->socket,"\r\n\r\n");
      u8_printf(&tmpout,"<html>\n<head>\n<title>Sorry, redirecting...</title>\n</head>\n<body>\n");
      u8_printf(&tmpout,"<p>Redirecting to <a href='%s'>%s</a></p>\n</body>\n</html>\n",errorpage,errorpage);
      write_string(client->socket,tmpout.u8_outbuf);
      u8_free(tmpout.u8_outbuf);}
    else if (FD_STRINGP(errorpage)) {
      /* This should check for redirect URLs, but for now it
	 just dumps the error page as plain text.  */
      http_len=http_len+
	strlen("Content-type: text/plain; charset=utf-8\r\n\r\n");
      write_string(client->socket,
		   "Content-type: text/plain; charset=utf-8\r\n\r\n");
      write_string(client->socket,FD_STRDATA(errorpage));}
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
    if (!(recovered)) {
      u8_free_exception(ex,1);
      if ((reqlog) || (urllog) || (trace_cgidata))
	dolog(cgidata,result,outstream->u8_outbuf,
	      outstream->u8_outptr-outstream->u8_outbuf,
	      u8_elapsed_time()-start_time);
      content_len=content_len+(outstream->u8_outptr-outstream->u8_outbuf);
      /* We do a hanging write in this, hoping it's not common case */
      u8_writeall(client->socket,outstream->u8_outbuf,
		  outstream->u8_outptr-outstream->u8_outbuf);
      return_code=-1;
      fd_decref(errorpage);
      /* And close the client for good measure */
      if (ucl->status) {u8_free(ucl->status); ucl->status=NULL;}
      u8_client_close(ucl);}}
  if (recovered) {
    U8_OUTPUT httphead, htmlhead; int tracep;
    fdtype traceval=fd_get(cgidata,tracep_slotid,FD_VOID);
    if (FD_VOIDP(traceval)) tracep=0; else tracep=1;
    U8_INIT_OUTPUT(&httphead,1024); U8_INIT_OUTPUT(&htmlhead,1024);
    fd_output_http_headers(&httphead,cgidata);
    if ((FD_VOIDP(content))&&(FD_VOIDP(retfile))) {
      char clen_header[128]; size_t bundle_len=0;
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
	retval=u8_writeall(client->socket,httphead.u8_outbuf,http_len);
	if (retval>=0)
	  retval=u8_writeall(client->socket,htmlhead.u8_outbuf,head_len);
	if (retval>=0)
	  retval=u8_writeall(client->socket,outstream->u8_outbuf,content_len);
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
	unsigned char *filebuf=NULL; fd_off_t total_len=-1;
	u8_printf(&httphead,"Content-length: %ld\r\n\r\n",
		  (long int)(fileinfo.st_size));
	http_len=httphead.u8_outptr-httphead.u8_outbuf;
	total_len=http_len+fileinfo.st_size;
	if ((async)&&(total_len<FD_FILEBUF_MAX))
	  filebuf=u8_malloc(total_len);
	if (filebuf) {
	  /* This is the case where we hand off a buffer to mod_fdserv
	     to write for us. */
	  unsigned char *write=filebuf+http_len;
	  fd_off_t to_read=fileinfo.st_size;
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
	  retval=u8_writeall(client->socket,httphead.u8_outbuf,http_len);
	  if (retval<0) return_code=-1;
	  else {
	    while ((bytes_read=fread(buf,sizeof(uchar),32768,f))>0) {
	      content_len=content_len+bytes_read;
	      retval=u8_writeall(client->socket,buf,bytes_read);
	      if (retval<0) break;}
	    return_code=0;}}}
      else {/* Error here */}}
    else if (FD_STRINGP(content)) {
      int bundle_len; unsigned char *outbuf=NULL;
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
	retval=u8_writeall(client->socket,httphead.u8_outbuf,
			   httphead.u8_outptr-httphead.u8_outbuf);
	if (retval>=0)
	  retval=u8_writeall(client->socket,FD_STRDATA(content),FD_STRLEN(content));}}
    else if (FD_PACKETP(content)) {
      int bundle_len; unsigned char *outbuf=NULL;
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
	retval=u8_writeall(client->socket,httphead.u8_outbuf,
			   httphead.u8_outptr-httphead.u8_outbuf);
	if (retval>=0)
	  retval=u8_writeall(client->socket,FD_PACKET_DATA(content),
			     FD_PACKET_LENGTH(content));}}
    else {
      /* Where the servlet has specified some particular content */
      content_len=content_len+output_content(client,content);}
    /* Reset the stream */
    outstream->u8_outptr=outstream->u8_outbuf;
    /* If we're not still in the transaction, call u8_client_done() */
    if (!(return_code)) {u8_client_done(ucl);}
    if ((forcelog)||(traceweb>2))
      u8_log(LOG_NOTICE,"HTTPHEAD","HTTPHEAD=%s",httphead.u8_outbuf);
    u8_free(httphead.u8_outbuf); u8_free(htmlhead.u8_outbuf);
    fd_decref(content); fd_decref(traceval);
    if (retval<0)
      u8_log(LOG_ERROR,"BADRET","Bad retval from writing data");
    if ((reqlog) || (urllog) || (trace_cgidata) || (tracep))
      dolog(cgidata,result,client->out.u8_outbuf,
	    outstream->u8_outptr-outstream->u8_outbuf,
	    u8_elapsed_time()-start_time);}
  if (fd_test(cgidata,cleanup_slotid,FD_VOID)) {
    fdtype cleanup=fd_get(cgidata,cleanup_slotid,FD_EMPTY_CHOICE);
    FD_DO_CHOICES(cl,cleanup) {
      fdtype cleanup_val=fd_apply(cleanup,0,NULL);
      fd_decref(cleanup_val);}}
  run_postflight();
  if (threadcache) fd_pop_threadcache(threadcache);
  fd_use_reqinfo(FD_EMPTY_CHOICE);
  fd_reqlog(-1);
  fd_thread_set(browseinfo_symbol,FD_VOID);
  fd_decref(init_cgidata); init_cgidata=FD_VOID;
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
  fd_decref(cgidata);
  fd_swapcheck();
  /* Task is done */
  if (return_code<=0) {
    if (ucl->status) {u8_free(ucl->status); ucl->status=NULL;}}
  return return_code;
}

static int close_webclient(u8_client ucl)
{
  fd_webconn client=(fd_webconn)ucl;
  u8_log(LOG_INFO,"webclient/close","Closing web client %s (0x%lx#%d.%d)",
	 ucl->idstring,ucl,ucl->clientid,ucl->socket);
  fd_decref(client->cgidata); client->cgidata=FD_VOID;
  fd_dtsclose(&(client->in),2);
  u8_close((u8_stream)&(client->out));
  return 1;
}

static int reuse_webclient(u8_client ucl)
{
  fd_webconn client=(fd_webconn)ucl;
  fdtype cgidata=client->cgidata;
  int refcount=((FD_CONSP(cgidata))?
		(FD_CONS_REFCOUNT((fd_cons)cgidata)):(0));
  u8_log(LOG_INFO,"webclient/reuse","Reusing web client %s (0x%lx)",
	 ucl->idstring,ucl);
  fd_decref(cgidata); client->cgidata=FD_VOID;
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
  webcommon_shutdown(reason);
  while (i>=0) {
    u8_string spec=ports[i];
    if (!(spec)) {}
    else if (strchr(spec,'/')) {
      if (remove(spec)<0) 
	u8_log(LOG_WARN,"FDServlet/shutdown",
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
      u8_log(LOG_WARN,
	     "FDServlet/shutdown","Couldn't remove pid file %s",pidfile);
    u8_free(pidfile);}
  pidfile=NULL;
  fd_recycle_hashtable(&pagemap);
}

static fdtype servlet_status_string()
{
  u8_string status=u8_server_status(&fdwebserver,NULL,0);
  return fd_lispstring(status);
}

/* Managing your dependent (for restarting servers) */

static pid_t dependent=-1;
static void kill_dependent_onexit(){
  u8_string ppid_file=fd_runbase_filename(".ppid");
  pid_t dep=dependent; dependent=-1;
  if (dep>0) kill(dep,SIGTERM);
  if (u8_file_existsp(ppid_file)) {
    u8_removefile(ppid_file);
    u8_free(ppid_file);}}
static void kill_dependent_onsignal(int sig){
  u8_string ppid_file=fd_runbase_filename(".ppid");
  pid_t dep=dependent; dependent=-1;
  if (dep>0)
    u8_log(LOG_WARN,"FDServer/signal",
	   "FDServer controller %d got signal %d, passing to %d",
	   getpid(),sig,dep);
  if (dep>0) kill(dep,sig);
  if (u8_file_existsp(ppid_file)) {
    u8_removefile(ppid_file);
    u8_free(ppid_file);}}

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
	  u8_log(LOG_WARN,"FDServlet/start","Couldn't remove socket file %s",
		 port);}
      else {
	u8_log(LOG_WARN,"FDServlet/start","Socket file %s already exists",
	       port);}}}
  i=0; while (i<lim) {
    int retval=add_server(ports[i]);
    if (retval<0) {
      u8_log(LOG_CRIT,"FDServlet/START","Couldn't start server %s",ports[i]);
      u8_clear_errors(1);}
    i++;}
  server_running=1;
  u8_unlock_mutex(&server_port_lock);
  return i;
}

/* The main() event */

FD_EXPORT void fd_init_dbfile(void); 
static int launch_servlet(u8_string socket_spec);
static int fork_servlet(u8_string socket_spec);

int main(int argc,char **argv)
{
  int u8_version=u8_initialize();
  int fd_version; /* Wait to set this until we have a log file */
  int i=1;
  u8_string socket_spec=NULL, load_source=NULL, load_config=NULL;
  char *logfile=NULL;

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
  
  if (!(socket_spec)) {}
  else if (strchr(socket_spec,'/')) 
    socket_spec=u8_abspath(socket_spec,NULL);
  else if ((strchr(socket_spec,':'))||(strchr(socket_spec,'@')))
    socket_spec=u8_strdup(socket_spec);
  else {
    u8_string sockets_dir=u8_mkpath(FD_RUN_DIR,"fdserv");
    socket_spec=u8_mkpath(sockets_dir,socket_spec);
    u8_free(sockets_dir);}

  if (socket_spec) {
    ports=u8_malloc(sizeof(u8_string)*8);
    max_ports=8; n_ports=1;
    server_id=ports[0]=u8_strdup(socket_spec);}

  fd_register_config("PORT",_("Ports for listening for connections"),
		     getfdservports,addfdservport,NULL);
  fd_register_config("ASYNCMODE",_("Whether to run in asynchronous mode"),
		     fd_boolconfig_get,fd_boolconfig_set,&async_mode);
  
  if (getenv("LOGFILE"))
    logfile=u8_strdup(getenv("LOGFILE"));
  else if ((getenv("LOGDIR"))&&(socket_spec)) {
    u8_string base=u8_basename(socket_spec,"*");
    u8_string logname=u8_mkstring("%s.log",base);
    logfile=u8_mkpath(getenv("LOGDIR"),logname);
    u8_free(base); u8_free(logname);}
  else u8_log(LOG_WARN,ServletStartup,"No logfile, using stdout");
  
  /* Close and reopen STDIN */ 
  close(0);  if (open("/dev/null",O_RDONLY) == -1) {
    u8_log(LOG_CRIT,"fdserver","Unable to reopen stdin for daemon");
    exit(1);}

  /* We do this using the Unix environment (rather than configuration
     variables) for two reasons.  First, we want to redirect errors
     from the processing of the configuration variables themselves
     (where lots of errors could happen); second, we want to be able
     to set this in the environment we wrap around calls (which is how
     mod_fdserv does it). */
  if (logfile) {
    int logsync=((getenv("LOGSYNC")==NULL)?(0):(O_SYNC));
    int log_fd=open(logfile,O_RDWR|O_APPEND|O_CREAT|logsync,0644);
    if (log_fd<0) {
      u8_log(LOG_WARN,ServletStartup,"Couldn't open log file %s",logfile);
      exit(1);}
    dup2(log_fd,1);
    dup2(log_fd,2);}

  fd_version=fd_init_fdscheme();

  if (fd_version<0) {
    u8_log(LOG_WARN,ServletStartup,"Couldn't initialize FramerD");
    exit(EXIT_FAILURE);}

  if (load_config) fd_load_config(load_config);

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
  u8_use_syslog(0);

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
  fd_register_config("WEBTHREADS",_("Number of threads in the thread pool"),
		     fd_intconfig_get,fd_intconfig_set,&servlet_threads);
  /* This one, NTHREADS, is deprecated */
  fd_register_config("NTHREADS",_("Number of threads in the thread pool"),
		     fd_intconfig_get,fd_intconfig_set,&servlet_threads);
  fd_register_config("STATLOG",_("File for recording status reports"),
		     statlog_get,statlog_set,NULL);
  fd_register_config
    ("STATINTERVAL",_("Milliseconds (roughly) between updates to .status"),
     statinterval_get,statinterval_set,NULL);
  fd_register_config
    ("STATLOGINTERVAL",_("Milliseconds (roughly) between logging status information"),
     statloginterval_get,statloginterval_set,NULL);
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

  u8_log(LOG_NOTICE,"LAUNCH","FDServlet %s",socket_spec);

  /* Process the config statements */
  while (i<argc)
    if (strchr(argv[i],'=')) {
      u8_log(LOG_NOTICE,"CONFIG","   %s",argv[i]);
      fd_config_assignment(argv[i++]);}
    else i++;

  fd_setapp(socket_spec,NULL);

  fd_boot_message();
  u8_now(&boot_time);

  if (!(server_id)) {
    u8_uuid tmp=u8_getuuid(NULL);
    server_id=u8_uuidstring(tmp,NULL);}

  pid_file=fd_runbase_filename(".pid");

  if (!(load_source)) {}
  else if ((u8_has_suffix(load_source,".scm",1))||
	   (u8_has_suffix(load_source,".fdcgi",1))||
	   (u8_has_suffix(load_source,".fdxml",1))) {
    fdtype path=fdtype_string(load_source);
    fdtype result=getcontent(path);
    fd_decref(path); fd_decref(result);}
  else {}

  if (getenv("FD_FOREGROUND"))
    return launch_servlet(socket_spec);
  else return fork_servlet(socket_spec);
}

static int launch_servlet(u8_string socket_spec)
{
  int rv=write_pid_file();
  if (rv<0)  {
    /* Error here, rather than repeatedly */
    fd_clear_errors(1);
    exit(EXIT_FAILURE);}
  else setup_status_file();

  if ((strchr(socket_spec,'/'))&&
      (u8_file_existsp(socket_spec))&&
      (!((stealsockets)||(getenv("FD_STEALSOCKETS"))))) {
    u8_log(LOG_CRIT,"Socket exists","Socket file %s already exists!",
	   socket_spec);
    exit(1);}

  u8_log(LOG_DEBUG,ServletStartup,"Updating preloads");
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
     U8_SERVER_TIMEOUT,poll_timeout,
     U8_SERVER_MAX_QUEUE,max_queue,
     U8_SERVER_MAX_CLIENTS,max_conn,
     U8_SERVER_END_INIT); 

  fdwebserver.xserverfn=statlog_server_update;
  fdwebserver.xclientfn=statlog_client_update;

  fd_register_config("U8LOGLISTEN",
		     _("Whether to have libu8 log each monitored address"),
		     config_get_u8server_flag,config_set_u8server_flag,
		     (void *)(U8_SERVER_LOG_LISTEN));
  fd_register_config("U8POLLTIMEOUT",
		     _("Timeout for the poll loop (lower bound of status updates)"),
		     config_get_u8server_flag,config_set_u8server_flag,
		     (void *)(U8_SERVER_TIMEOUT));
  fd_register_config("U8LOGCONNECT",
		     _("Whether to have libu8 log each connection"),
		     config_get_u8server_flag,config_set_u8server_flag,
		     (void *)(U8_SERVER_LOG_CONNECT));
  fd_register_config("U8LOGTRANSACT",
		     _("Whether to have libu8 log each transaction"),
		     config_get_u8server_flag,config_set_u8server_flag,
		     (void *)(U8_SERVER_LOG_TRANSACT));
  fd_register_config("U8LOGQUEUE",
		     _("Whether to have libu8 log queue activity"),
		     config_get_u8server_flag,config_set_u8server_flag,
		     (void *)(U8_SERVER_LOG_QUEUE));
#ifdef U8_SERVER_LOG_TRANSFER
  fd_register_config
    ("U8LOGTRANSFER",
     _("Whether to have libu8 log data transmission/receiption"),
     config_get_u8server_flag,config_set_u8server_flag,
     (void *)(U8_SERVER_LOG_TRANSFER));
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

  update_preloads();

  if (start_servers()<=0) {
    u8_log(LOG_CRIT,"FDServlet/STARTUP","Startup failed");
    exit(1);}

  u8_log(LOG_INFO,NULL,
	 "FramerD (%s) FDServlet running, %d/%d pools/indices",
	 FRAMERD_REVISION,fd_n_pools,
	 fd_n_primary_indices+fd_n_secondary_indices);
  u8_message("beingmeta FramerD, (C) beingmeta 2004-2014, all rights reserved");
  if (fdwebserver.n_servers>0) u8_server_loop(&fdwebserver);
  else {
    u8_log(LOG_CRIT,NoServers,"No servers configured, exiting...");
    exit(-1);
    return -1;}

  u8_message("FDServlet, normal exit of u8_server_loop()");
  
  shutdown_server("exit");

  exit(0);

  return 0;
}

static int sustain_servlet(pid_t grandchild,u8_string socket_spec);

static int fork_servlet(u8_string socket_spec)
{
  pid_t child, grandchild; double start=u8_elapsed_time();
  if ((getenv("FD_FOREGROUND"))&&(getenv("FD_DAEMONIZE"))) {
    /* This is the scenario where we stay in the foreground but
       restart automatically.  */
    if ((child=fork())) {
      if (child<0) {
	u8_log(LOG_CRIT,"fork_servlet","Fork failed for %s",socket_spec);
	exit(1);}
      else {
	u8_log(LOG_NOTICE,"fork_servlet","Running server %s has PID %d",
	       socket_spec,child);
	return sustain_servlet(child,socket_spec);}}
    else return launch_servlet(socket_spec);}
  else if ((child=fork()))  {
    /* The grandparent waits until the parent exits and then
       waits until the .pid file has been written. */
    int count=60; double done; int status=0;
    if (child<0) {
      u8_log(LOG_CRIT,"fork_servlet","Fork failed for %s\n",socket_spec);
      exit(1);}
    else u8_log(LOG_WARN,"fork_servlet","Initial fork spawned pid %d from %d",
		child,getpid());
#if HAVE_WAITPID
    if (waitpid(child,&status,0)<0) {
      u8_log(LOG_CRIT,ServletStartup,"Fork wait failed");
      exit(1);}
    if (!(WIFEXITED(status))) {
      u8_log(LOG_CRIT,ServletStartup,"First fork failed to finish");
      exit(1);}
    else if (WEXITSTATUS(status)!=0) {
      u8_log(LOG_CRIT,ServletStartup,"First fork return non-zero status");
      exit(1);}
#endif
    /* If the parent has exited, we wait around for the pid_file to be created
       by our grandchild. */
    while ((count>0)&&(!(u8_file_existsp(pid_file)))) {
      if ((count%10)==0)
	u8_log(LOG_WARN,ServletStartup,"Waiting for PID file %s",pid_file);
      count--; sleep(1);}
    done=u8_elapsed_time();
    if (u8_file_existsp(pid_file)) 
      u8_log(LOG_NOTICE,"fdservlet","Servlet %s launched in %02fs",
	     socket_spec,done-start);
    else u8_log(LOG_CRIT,"fdservlet",
		"Servlet %s hasn't launched after %02fs",
		socket_spec,done-start);
    exit(0);}
  else {
    /* If we get here, we're the parent, and we start by trying to
       become session leader */
    if (setsid()==-1) {
      u8_log(LOG_CRIT,"fork_servlet",
	     "Process %d failed to become session leader for %s (%s)",
	     getpid(),socket_spec,strerror(errno));
      errno=0;
      exit(1);}
    else u8_log(LOG_INFO,"fork_servlet",
		"Process %d become session leader for %s",getpid(),socket_spec);
    /* Now we fork again.  In the normal case, this fork (the grandchild) is
       the actual server.  If we're auto-restarting, this fork is the one which
       does the restarting. */
    if ((grandchild=fork())) {
      if (grandchild<0) {
	u8_log(LOG_CRIT,"fork_servlet","Second fork failed for %s",socket_spec);
	exit(1);}
      else if (getenv("FD_DAEMONIZE"))
	u8_log(LOG_NOTICE,"fork_servlet","Restart monitor for %s has PID %d",
	       socket_spec,grandchild);
      else u8_log(LOG_NOTICE,"fork_servlet","Running server %s has PID %d",
		  socket_spec,grandchild);
      /* This is the parent, which always exits */
      exit(0);}
    else if (getenv("FD_DAEMONIZE")) {
      pid_t worker;
      if ((worker=fork())) {
	if (worker<0) 
	  u8_log(LOG_CRIT,"fork_servlet","Worker fork failed for %s",socket_spec);
	else {
	  u8_log(LOG_NOTICE,"fork_servlet","Running server %s has PID %d",
		 socket_spec,worker);
	  return sustain_servlet(worker,socket_spec);}}
      else return launch_servlet(socket_spec);}
    else return launch_servlet(socket_spec);}
  exit(0);
}

static int sustain_servlet(pid_t grandchild,u8_string socket_spec)
{
  u8_string ppid_filename=fd_runbase_filename(".ppid");
  FILE *f=fopen(ppid_filename,"w");
  char *restartval=getenv("FD_DAEMONIZE");
  int status=-1, sleepfor=atoi(restartval); 
  if (f) {
    fprintf(f,"%ld\n",(long)getpid());
    fclose(f);
    u8_free(ppid_filename);}
  else {
    u8_log(LOG_WARN,"CantWritePPID","Couldn't write ppid file %s",
	   ppid_filename);
    u8_free(ppid_filename);}
  /* Don't try to catch an error here */
  if (sleepfor<0) sleepfor=7;
  else if (sleepfor>60) sleepfor=60;
  errno=0;
  /* Update the global variable with our current dependent grandchild */
  dependent=grandchild;
  /* Setup atexit and signal handlers to kill our dependent when we're
     gone. */
  u8_log(LOG_WARN,"FDServer/sustain %s pid=%d",socket_spec,grandchild);
  atexit(kill_dependent_onexit);
#ifdef SIGTERM
  signal(SIGTERM,kill_dependent_onsignal);
#endif
#ifdef SIGQUIT
  signal(SIGQUIT,kill_dependent_onsignal);
#endif
  while (waitpid(grandchild,&status,0)) {
    if (WIFSIGNALED(status))
      u8_log(LOG_WARN,"FDServer/restart",
	     "Server %s(%d) terminated on signal %d",
	     socket_spec,grandchild,WTERMSIG(status));
    else if (WIFEXITED(status))
      u8_log(LOG_NOTICE,"FDServer/restart",
	     "Server %s(%d) terminated normally with status %d",
	     socket_spec,grandchild,status);
    else continue;
    if (dependent<0) {
      u8_log(LOG_WARN,"FDServer/done",
	     "Terminating restart process for %s",socket_spec);
      exit(0);}
    sleep(sleepfor);
    if ((grandchild=fork())) {
      u8_log(LOG_NOTICE,"FDServer/restart",
	     "Server %s restarted with pid %d",
	     socket_spec,grandchild);
      dependent=grandchild;
      continue;}
    else return launch_servlet(socket_spec);}
  exit(0);
  return 0;
}



