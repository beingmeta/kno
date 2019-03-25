/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/defines.h"
#include "framerd/dtype.h"
#include "framerd/tables.h"
#include "framerd/support.h"
#include "framerd/numbers.h"
#include "framerd/eval.h"
#include "framerd/storage.h"
#include "framerd/fdweb.h"
#include "framerd/ports.h"
#include "framerd/fileprims.h"

#include <libu8/libu8.h>
#include <libu8/libu8io.h>
#include <libu8/u8logging.h>
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

static u8_condition ServletStartup=_("FDServlet/STARTUP");
static u8_condition ServletAbort=_("FDServlet/ABORT");
static u8_condition NoServers=_("No servers configured");
#define Startup ServletStartup

static int daemonize = 0, foreground = 0, pidwait = 1;

static const sigset_t *server_sigmask;

FD_EXPORT int fd_init_dbserv(void);

#include "webcommon.h"

#define nobytes(in,nbytes) (PRED_FALSE(!(fd_request_bytes(in,nbytes))))
#define havebytes(in,nbytes) (FD_EXPECT_TRUE(fd_request_bytes(in,nbytes)))

#define HTML_UTF8_CTYPE_HEADER "Content-type: text/html; charset = utf-8\r\n\r\n"

/* This lets the u8_server loop do I/O buffering to keep threads from
   waiting on I/O. */
static int async_mode = 1;

/* Logging declarations */
static FILE *statlog = NULL; int statusout = -1;
static double overtime = 0;
static int stealsockets = 0;

/* Tracking ports */
static u8_string server_id = NULL;
static u8_string *ports = NULL;
static int n_ports = 0, max_ports = 0;
static u8_mutex server_port_lock;

/* This environment is the parent of all script/page environments spun off
   from this server. */
static int init_clients = 64, servlet_threads = 8, max_queue = 256, max_conn = 0;
/* This is the backlog of connection requests not transactions.
   It is passed as the argument to listen() */
static int max_backlog = -1;

/* This is how long (μs) to wait for clients to finish when shutting down the
   server.  Note that the server stops listening for new connections right
   away, so we can start another server.  */
static int shutdown_grace = 30000000; /* 30 seconds */

static lispval fallback_notfoundpage = VOID;

u8_string pid_file = NULL, cmd_file = NULL, inject_file = NULL;

/* When the server started, used by UPTIME */
static struct U8_XTIME boot_time;

/* This is how old a socket file needs to be to be deleted as 'leftover' */
#define FD_LEFTOVER_AGE 30
static int ignore_leftovers = 0;

typedef struct FD_WEBCONN {
  U8_CLIENT_FIELDS;
  lispval cgidata;
  struct FD_STREAM in;
  struct U8_OUTPUT out;} FD_WEBCONN;
typedef struct FD_WEBCONN *fd_webconn;

#ifndef DEFAULT_POLL_TIMEOUT
#define DEFAULT_POLL_TIMEOUT 1000
#endif

static int poll_timeout = DEFAULT_POLL_TIMEOUT;

struct U8_SERVER fdwebserver;
static int server_running = 0;

static u8_condition fdservWriteError="FDServelt write error";

/* Checking for (good) injections */

static int check_for_injection()
{
  if (server_env == NULL) return 0;
  else if (inject_file == NULL) return 0;
  else if (u8_file_existsp(inject_file)) {
    u8_string temp_file = u8_string_append(inject_file,".loading",NULL);
    int rv = u8_movefile(inject_file,temp_file);
    if (rv<0) {
      u8_log(LOG_WARN,"FDServlet/InjectionIgnored",
             "Can't stage injection file %s to %s",
             inject_file,temp_file);
      fd_clear_errors(1);
      u8_free(temp_file);
      return 0;}
    else {
      u8_string content = u8_filestring(temp_file,NULL);
      if (content == NULL)  {
        u8_log(LOG_WARN,"FDServlet/InjectionCantRead",
               "Can't read %s",temp_file);
        fd_clear_errors(1);
        u8_free(temp_file);
        return -1;}
      else {
        lispval result;
        u8_log(LOG_WARN,"FDServlet/InjectLoad",
               "From %s\n\"%s\"",temp_file,content);
        result = fd_load_source(temp_file,server_env,NULL);
        if (FD_ABORTP(result)) {
          u8_exception ex = u8_current_exception;
          if (!(ex)) {
            u8_log(LOG_CRIT,"FDServlet/InjectError",
                   "Unknown error processing injection from %s: \"%s\"",
                   inject_file,content);}
          else if ((ex->u8x_context!=NULL)&&
                   (ex->u8x_details!=NULL))
            u8_log(LOG_CRIT,"FDServlet/InjectionError",
                   "Error %s (%s) processing injection %s: %s\n\"%s\"",
                   ex->u8x_cond,ex->u8x_context,inject_file,
                   ex->u8x_details,content);
          else if (ex->u8x_context!=NULL)
            u8_log(LOG_CRIT,"FDServlet/InjectionError",
                   "Error %s (%s) processing injection %s\n\"%s\"",
                   ex->u8x_cond,ex->u8x_context,inject_file,content);
          else u8_log(LOG_CRIT,"FDServlet/InjectionError",
                      "Error %s processing injection %s\n\"%s\"",
                      ex->u8x_cond,inject_file,content);
          fd_clear_errors(1);
          return -1;}
        else {
          u8_log(LOG_WARN,"FDServlet/InjectionDone",
                 "Finished from %s",inject_file);}
        rv = u8_removefile(temp_file);
        if (rv<0) {
          u8_log(LOG_CRIT,"FDServlet/InjectionCleanup",
                 "Error removing %s",temp_file);
          fd_clear_errors(1);}
        fd_decref(result);
        u8_free(content);
        u8_free(temp_file);
        return 1;}}}
  else return 0;
}

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
static long long last_status = 0, last_statlog = 0;
static long status_interval = DEFAULT_STATUS_INTERVAL;
static long statlog_interval = DEFAULT_STATUS_INTERVAL;

static void close_statlog();

static int statlog_set(lispval var,lispval val,void *data)
{
  if (STRINGP(val)) {
    u8_string filename = CSTRING(val);
    u8_lock_mutex(&log_lock);
    if (statlog) {
      fclose(statlog); statlog = NULL;
      u8_free(statlogfile); statlogfile = NULL;}
    statlogfile = u8_abspath(filename,NULL);
    statlog = u8_fopen_locked(statlogfile,"a");
    if ((statlog == NULL)&&(u8_file_existsp(statlogfile))) {
      int rv = u8_removefile(statlogfile);
      if (rv>=0) statlog = u8_fopen_locked(statlogfile,"a");}
    if (statlog) {
      u8_string tmp;
      tmp = u8_mkstring("# Log open %*lt for %s\n",u8_sessionid());
      u8_chmod(statlogfile,0774);
      fputs(tmp,statlog);
      u8_unlock_mutex(&log_lock);
      u8_free(tmp);
      return 1;}
    else {
      u8_log(LOG_WARN,"no file","Couldn't open %s",statlogfile);
      u8_unlock_mutex(&log_lock);
      u8_free(statlogfile); statlogfile = NULL;
      return 0;}}
  else if (FALSEP(val)) {
    close_statlog();
    return 0;}
  else return fd_reterr
         (fd_TypeError,"config_set_statlog",u8_strdup(_("string")),val);
}

static lispval statlog_get(lispval var,void *data)
{
  if (statlog)
    return fdstring(statlogfile);
  else return FD_FALSE;
}

static int statinterval_set(lispval var,lispval val,void *data)
{
  if (FD_UINTP(val)) {
    int intval = FD_FIX2INT(val);
    if (intval>=0)  status_interval = intval*1000;
    else {
      return fd_reterr
        (fd_TypeError,"config_set_statinterval",
         u8_strdup(_("fixnum")),val);}}
  else if (FALSEP(val)) status_interval = -1;
  else if (STRINGP(val)) {
    int flag = fd_boolstring(CSTRING(val),-1);
    if (flag<0) {
      u8_log(LOG_WARN,"statinterval_set","Unknown value: %s",CSTRING(val));
      return 0;}
    else if (flag) {
      if (status_interval>0) return 0;
      status_interval = DEFAULT_STATUS_INTERVAL;}
    else if (status_interval<0)
      return 0;
    else status_interval = -1;}
  else return fd_reterr
         (fd_TypeError,"statinterval_set",
          u8_strdup(_("fixnum")),val);
  return 1;
}

static lispval statloginterval_get(lispval var,void *data)
{
  if (statlog_interval<0) return FD_FALSE;
  else return FD_INT(statlog_interval);
}

static int statloginterval_set(lispval var,lispval val,void *data)
{
  if (FD_UINTP(val)) {
    int intval = FD_FIX2INT(val);
    if (intval>=0)  statlog_interval = intval*1000;
    else {
      return fd_reterr
        (fd_TypeError,"statloginterval_set",
         u8_strdup(_("fixnum")),val);}}
  else if (FALSEP(val)) statlog_interval = -1;
  else if (STRINGP(val)) {
    int flag = fd_boolstring(CSTRING(val),-1);
    if (flag<0) {
      u8_log(LOG_WARN,"statloginterval_set",
             "Unknown value: %s",CSTRING(val));
      return 0;}
    else if (flag) {
      if (statlog_interval>0) return 0;
      statlog_interval = DEFAULT_STATLOG_INTERVAL;}
    else if (statlog_interval<0)
      return 0;
    else statlog_interval = -1;}
  else return fd_reterr
         (fd_TypeError,"config_set_statloginterval",
          u8_strdup(_("fixnum")),val);
  return 1;
}

static lispval statinterval_get(lispval var,void *data)
{
  if (status_interval<0) return FD_FALSE;
  else return FD_INT(status_interval);
}

#define STATUS_LINE_CURRENT \
  "[%*t] %d/%d/%d/%d current busy/waiting/connections/threads\t\t[@%f]\n"
#define STATUS_LINE_AGGREGATE \
  "[%*t] %d/%d/%d accepts/requests/failures over %0.3f%s uptime\n"
#define STATUS_LINE_TIMING \
  "[%*t] Average %0.3f%s response (max=%0.3f%s), %0.3f%s avg cpu (max=%0.3f%s)\n"
#define STATUS_LOG_SNAPSHOT \
  "[%*t][%f] %d/%d/%d/%d busy/waiting/connections/threads, %d/%d/%d reqs/resps/errs, response: %0.2fus, max=%ldus, run: %0.2fus, max=%ldus"
#define STATUSLOG_LINE "%*t\t%f\t%d\t%d\t%d\t%d\t%0.2f\t%0.2f\n"
#define STATUS_LINEXN "[%*t][%f] %s: %s mean=%0.2fus max=%lldus sd=%0.2f (n=%d)\n"
#define STATUS_LINEX "%s: %s mean=%0.2fus max=%lldus sd=%0.2f (n=%d)"

#define stdev(v,v2,n) \
  ((double)(sqrt((((double)v2)/((double)n))-                    \
                 ((((double)v)/((double)n))*(((double)v)/((double)n))))))
#define getmean(v,n) (((double)v)/((double)n))

static int log_status = -1;

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
  double elapsed = u8_elapsed_time();
  struct U8_SERVER_STATS stats;
  FILE *log = statlog; int mon = statusout;
  if (((mon<0)&&(statfile))||
      ((!(log))&&(statlogfile))) {
    u8_lock_mutex(&log_lock);
    if (statusout>=0) mon = statusout;
    else if (statfile) {
      mon = statusout = (open(statfile,O_WRONLY|O_CREAT,0644));
      if (mon>=0) u8_lock_fd(mon,1);}
    else mon = -1;
    if (statlog) log = statlog;
    else if (statlogfile)
      log = statlog = (u8_fopen_locked(statlogfile,"a"));
    else log = NULL;
    u8_unlock_mutex(&log_lock);}
  if (mon>=0) {
    off_t rv = lseek(mon,0,SEEK_SET);
    if (rv>=0) rv = ftruncate(mon,0);
    if (rv<0) {
      u8_log(LOG_WARN,"output_status","File truncate failed (%s:%d)",
             strerror(errno),errno);
      errno = 0;}
    else {
      rv = lseek(mon,0,SEEK_SET); errno = 0;}}

  u8_server_statistics(&fdwebserver,&stats);

  if ((log!=NULL)||(log_status>0)) {
    long long now = u8_microtime();
    if ((statlog_interval>=0)&&(now>(last_statlog+statlog_interval))) {
      last_statlog = now;
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
    int written = 0, len, delta = 0;
    double rmean = ((stats.tcount)?
                  (((double)(stats.tsum))/((double)(stats.tcount))):
                  (0.0));
    double rmax = (double)(stats.rmax);
    double xmean = ((stats.xcount)?
                  (((double)(stats.xsum))/((double)(stats.xcount))):
                  (0.0));
    double xmax = (double)(stats.xmax);
    char *rmean_units, *rmax_units, *xmean_units, *xmax_units;
    double uptime; char *uptime_units;
    struct U8_OUTPUT out; U8_INIT_STATIC_OUTPUT(out,4096);
    u8_printf(&out,STATUS_LINE_CURRENT,
              fdwebserver.n_busy,fdwebserver.n_queued,
              fdwebserver.n_clients,fdwebserver.n_threads,
              elapsed);
    uptime = getinterval(elapsed,&uptime_units);
    u8_printf(&out,STATUS_LINE_AGGREGATE,
              fdwebserver.n_accepted,fdwebserver.n_trans,fdwebserver.n_errs,
              uptime,uptime_units);
    rmean = getinterval(rmean,&rmean_units); rmax = getinterval(rmax,&rmax_units);
    xmean = getinterval(xmean,&xmean_units); xmax = getinterval(xmax,&xmax_units);
    u8_printf(&out,STATUS_LINE_TIMING,
              rmean,rmean_units,rmax,rmax_units,
              xmean,xmean_units,xmax,xmax_units);
    u8_list_clients(&out,&fdwebserver);
    len = out.u8_write-out.u8_outbuf;
    while ((delta = write(mon,out.u8_outbuf+written,len-written))>0)
      written = written+delta;;
    fsync(mon);
    u8_free(out.u8_outbuf);}
}

static int server_loopfn(struct U8_SERVER *server)
{
  long long now = u8_microtime();
  if ((status_interval>=0)&&(now>(last_status+status_interval))) {
    last_status = now;
    check_for_injection();
    update_status();}
  return 0;
}
static int client_loopfn(struct U8_CLIENT *client)
{
  long long now = u8_microtime();
  if ((status_interval>=0)&&(u8_microtime()>(last_status+status_interval))) {
    last_status = now;
    update_status();}
  return 0;
}

static void setup_status_file()
{
  if (statfile == NULL) {
    u8_string filename = fd_runbase_filename(".status");
    statfile = filename;}
}

static int lock_fd(int fd,int lock,int block)
{
  int lv = 0;
#if ((HAVE_FLOCK)&&(HAVE_SYS_FILE_H))
  lv = flock(fd,((lock)?(LOCK_EX):(LOCK_UN))|((block)?(0):(LOCK_NB)));
#elif HAVE_LOCKF
  lv = lockf(fd,((lock)?(F_LOCK):(F_ULOCK))|((block)?(0):(F_TEST)),1);
else if HAVE_FCNTL
  struct flock lockinfo={F_WRLCK,SEEK_SET,0,1};
  if (lock) lockinfo.l_type = F_WRLCK; sle lockinfo.l_type = F_UNLCK;
  lv = fcntl(fd,((block)?(F_SETLKW):(F_SETLK)),&lockinfo);
#endif
  return lv;
}

static void cleanup_pid_file()
{
  u8_string exit_filename = fd_runbase_filename(".exit");
  FILE *exitfile = u8_fopen(exit_filename,"w");
  if (pid_file) {
    u8_string filename = pid_file;
    if (pid_fd>=0) {
      int lv = lock_fd(pid_fd,0,0);
      if (lv<0) {
        u8_log(LOG_CRIT,"FDServlet/cleanup",
               "Waiting to release lock on PID file %s",pid_file);
        errno = 0; lv = lock_fd(pid_fd,0,1);}
      if (lv<0) {
        u8_graberr(errno,"cleanup_pid_file",u8_strdup(pid_file));
        fd_clear_errors(1);}
      close(pid_fd);
      pid_fd = -1;}
    pid_file = NULL;
    if (u8_file_existsp(filename)) u8_removefile(filename);
    u8_free(filename);}
  if (cmd_file) {
    u8_string filename = cmd_file; cmd_file = NULL;
    if (u8_file_existsp(filename)) u8_removefile(filename);
    u8_free(filename);}
  if (exitfile) {
    struct U8_XTIME xt; struct U8_OUTPUT out;
    char timebuf[64]; double elapsed = u8_elapsed_time();
    u8_now(&xt); U8_INIT_FIXED_OUTPUT(&out,sizeof(timebuf),timebuf);
    u8_xtime_to_iso8601(&out,&xt);
    fprintf(exitfile,"%d@%s(%f)\n",getpid(),timebuf,elapsed);
    fclose(exitfile);}
  u8_free(exit_filename);
}

static int write_pid_file()
{
  const char *abspath = u8_abspath(pid_file,NULL);
  u8_byte buf[512];
  struct stat fileinfo;
  int lv, sv = stat(abspath,&fileinfo), stat_err = 0, exists = 0;
  if (sv<0) {stat_err = errno; errno = 0;}
  else {
    exists = 1;
    time_t ctime = fileinfo.st_ctime;
    time_t mtime = fileinfo.st_mtime;
    u8_uid uid = fileinfo.st_uid;
    u8_string uname = u8_username(uid);
    u8_log(LOG_WARN,"FDServlet/Leftover PID file",
           "File %s created %t, modified %t, owned by %s (%d)",
           abspath,ctime,mtime,uname,uid);
    if (uname) u8_free(uname);}
  pid_fd = open(abspath,O_CREAT|O_RDWR|O_TRUNC,
              S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH);
  if (pid_fd<0) {
    if (stat_err) u8_graberr(stat_err,"write_pid_file",u8_strdup(pid_file));
    u8_graberr(errno,"write_pid_file",u8_strdup(pid_file));
    u8_free(abspath);
    return -1;}
  lv = lock_fd(pid_fd,1,0);
  if (lv<0) {
    u8_log(LOG_CRIT,"FDServlet/write_pid_file",
           "Can't get lock on PID file %s",
           pid_file);
    u8_graberr(errno,"write_pid_file",u8_strdup(pid_file));
    close(pid_fd);
    u8_free(abspath);
    return -1;}
  else {
    if (exists)
      u8_log(LOG_WARN,"FDServlet/write_pid_file",
             "Bogarted existing PID file %s",abspath);
    sprintf(buf,"%d\n",getpid());
    int rv = write(pid_fd,buf,strlen(buf));
    if (rv<0) {
      int got_err = errno; errno=0;
      u8_log(LOG_WARN,"CantWriteFile",
             "Can't write PID file %s (errno=%d:%s)",
             abspath,got_err,u8_strerror(got_err));}
    atexit(cleanup_pid_file);
    /* It's now okay to steal sockets and other files */
    stealsockets = 1;
    u8_free(abspath);
    return pid_fd;}
}

static void close_statlog()
{
  if (statlog) {
    u8_lock_mutex(&log_lock);
    fclose(statlog); statlog = NULL;
    u8_unlock_mutex(&log_lock);}
}

static lispval servlet_status()
{
  lispval result = fd_init_slotmap(NULL,0,NULL);
  struct U8_SERVER_STATS stats, livestats, curstats;

  fd_store(result,fd_intern("NTHREADS"),FD_INT(fdwebserver.n_threads));
  fd_store(result,fd_intern("NQUEUED"),FD_INT(fdwebserver.n_queued));
  fd_store(result,fd_intern("NBUSY"),FD_INT(fdwebserver.n_busy));
  fd_store(result,fd_intern("NCLIENTS"),FD_INT(fdwebserver.n_clients));
  fd_store(result,fd_intern("TOTALTRANS"),FD_INT(fdwebserver.n_trans));
  fd_store(result,fd_intern("TOTALCONN"),FD_INT(fdwebserver.n_accepted));

  u8_server_statistics(&fdwebserver,&stats);
  u8_server_livestats(&fdwebserver,&livestats);
  u8_server_curstats(&fdwebserver,&curstats);

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

/* Document generation */

#define write_string(sock,string) u8_writeall(sock,string,strlen(string))

static int output_content(fd_webconn ucl,lispval content)
{
  if (STRINGP(content)) {
    ssize_t rv = u8_writeall(ucl->socket,CSTRING(content),STRLEN(content));
    if (rv<0) {
      u8_log(LOG_CRIT,fdservWriteError,
             "Unexpected error writing %ld bytes to mod_fdserv",
             STRLEN(content));
      return rv;}
    return STRLEN(content);}
  else if (FD_PACKETP(content)) {
    ssize_t rv = u8_writeall(ucl->socket,FD_PACKET_DATA(content),FD_PACKET_LENGTH(content));
    if (rv<0) {
      u8_log(LOG_CRIT,fdservWriteError,
             "Unexpected error writing %ld bytes to mod_fdserv",
             STRLEN(content));
      return rv;}
    return FD_PACKET_LENGTH(content);}
  else return 0;
}

/* Configuring u8server fields */

static lispval config_get_u8server_flag(lispval var,void *data)
{
  fd_ptrbits bigmask = (fd_ptrbits)data;
  unsigned int mask = (unsigned int)(bigmask&0xFFFFFFFF);
  unsigned int flags = fdwebserver.flags;
  if ((flags)&(mask)) return FD_TRUE; else return FD_FALSE;
}

static int config_set_u8server_flag(lispval var,lispval val,void *data)
{
  fd_ptrbits bigmask = (fd_ptrbits)data;
  unsigned int mask = (bigmask&0xFFFFFFFF);
  unsigned int flags = fdwebserver.flags;
  unsigned int *flagsp = &(fdwebserver.flags);
  if (FALSEP(val))
    *flagsp = flags&(~(mask));
  else if ((STRINGP(val))&&(STRLEN(val)==0))
    *flagsp = flags&(~(mask));
  else if (STRINGP(val)) {
    u8_string s = CSTRING(val);
    int bool = fd_boolstring(CSTRING(val),-1);
    if (bool<0) {
      int guess = (((s[0]=='y')||(s[0]=='Y'))?(1):
                 ((s[0]=='N')||(s[0]=='n'))?(0):
                 (-1));
      if (guess<0) {
        u8_log(LOG_WARN,"FDServlet/SERVERFLAG",
               "Unknown boolean setting %s",s);
        return fd_reterr(fd_TypeError,"setserverflag","boolean value",val);}
      else u8_log(LOG_WARN,"FDSerlvet/SERVERFLAG",
                  "Unfamiliar boolean setting %s, assuming %s",
                  s,((guess)?("true"):("false")));
      if (!(guess<0)) bool = guess;}
    if (bool) *flagsp = flags|mask;
    else *flagsp = flags&(~(mask));}
  else *flagsp = flags|mask;
  return 1;
}

/* Running the server */

static u8_client simply_accept(u8_server srv,u8_socket sock,
                               struct sockaddr *addr,size_t len)
{
  /* We could do access control here. */
  fd_webconn consed = (fd_webconn)
    u8_client_init(NULL,sizeof(FD_WEBCONN),addr,len,sock,srv);
  fd_init_stream(&(consed->in),consed->idstring,sock,
                 FD_STREAM_SOCKET|FD_STREAM_DOSYNC|FD_STREAM_OWNS_FILENO,
                 FD_NETWORK_BUFSIZE);
  U8_INIT_STATIC_OUTPUT((consed->out),8192);
  u8_set_nodelay(sock,1);
  consed->cgidata = VOID;
  u8_log(LOG_INFO,"FDServlet/open","Created web client (#%lx) %s",
         consed,
         ((consed->idstring == NULL)?((u8_string)""):(consed->idstring)));
  return (u8_client) consed;
}

static int max_error_depth = 128;

static void add_reqdata_to_error(lispval ex,lispval reqdata)
{
  if (FD_TYPEP(ex,fd_exception_type)) {
    struct FD_EXCEPTION *exinfo = (fd_exception) ex;
    lispval context = exinfo->ex_context;
    lispval copied = fd_deep_copy(reqdata);
    if (FD_VOIDP(context))
      exinfo->ex_context=context=fd_make_slotmap(2,0,NULL);
    else if (!(FD_SLOTMAPP(context))) {
      lispval new_context = fd_make_slotmap(2,0,NULL);
      fd_store(new_context,fd_intern("MISC"),context);
      fd_decref(context);
      exinfo->ex_context = new_context;
      context = new_context;}
    else NO_ELSE;
    fd_store(context,fd_intern("REQDATA"),copied);}
}

static int webservefn(u8_client ucl)
{
  lispval proc = VOID, result = VOID;
  lispval cgidata = VOID, init_cgidata = VOID, precheck = VOID;
  lispval user = VOID, path = VOID, uri = VOID, method = VOID, addr = FD_VOID;
  lispval content = VOID, retfile = VOID;
  fd_lexenv base_env = NULL;
  fd_webconn client = (fd_webconn)ucl;
  u8_server server = client->server;
  int write_headers = 1, close_html = 0, forcelog = 0;
  double start_time, setup_time, parse_time, exec_time, write_time;
  struct FD_THREAD_CACHE *threadcache = NULL;
  struct rusage start_usage, end_usage;
  double start_load[]={-1,-1,-1}, end_load[]={-1,-1,-1};
  ssize_t retval = 0, http_len = 0, head_len = 0, content_len = 0;
  fd_stream stream = &(client->in);
  fd_inbuf inbuf = fd_readbuf(stream);
  u8_output outstream = &(client->out);
  int async = ((async_mode)&&((client->server->flags)&U8_SERVER_ASYNC));
  int return_code = 0, buffered = 0, recovered = 1, http_status = -1;
  U8_FIXED_OUTPUT(logcxt,100);

  /* Set the signal mask for the current thread.  By default, this
     only accepts synchronoyus signals. */
  /* Note that this is called on every loop, but we're presuming it's
     really fast. */
  pthread_sigmask(SIG_SETMASK,server_sigmask,NULL);

  /* Reset the streams */
  outstream->u8_write = outstream->u8_outbuf;
  inbuf->bufread = inbuf->buflim = inbuf->buffer;
  /* Handle async reading (where the server buffers incoming and outgoing data) */
  if ((client->reading>0)&&(u8_client_finished(ucl))) {
    /* We got the whole payload, set up the stream
       for reading it without waiting.  */
    inbuf->buflim = inbuf->buffer+client->len;}
  else if (client->reading>0)
    /* We shouldn't get here, but just in case.... */
    return 1;
  else if ((client->writing>0)&&(u8_client_finished(ucl))) {
    /* All done */
    if (ucl->status) {u8_free(ucl->status); ucl->status = NULL;}
    return 0;}
  else if (client->writing>0)
    /* We shouldn't get here, but just in case.... */
    return 1;
  else {
    /* We read a little to see if we can just queue up what we
       need. */
    if ( (async) && (havebytes(inbuf,1)) &&
         ((*(inbuf->bufread)) == dt_block)) {
      /* If we can be asynchronous, let's try */
      int U8_MAYBE_UNUSED dtcode = fd_read_byte(inbuf);
      int nbytes = fd_read_4bytes(inbuf);
      if (fd_has_bytes(inbuf,nbytes)) {
        /* We can execute without waiting */}
      else {
        int need_size = 5+nbytes;
        /* Allocate enough space for what we need to read */
        if (inbuf->buflen<need_size) {
          fd_grow_byte_input(fd_readbuf(stream),need_size);
          inbuf->buflen = need_size;}
        struct FD_RAWBUF *raw = (struct FD_RAWBUF *)inbuf;
        size_t byte_offset = raw->buflim-raw->buffer;
        /* Set up the client for async input */
        if (u8_client_read(ucl,raw->buffer,need_size,byte_offset)) {
          /* We got the whole payload, set up the stream
             for reading it without waiting.  */
          raw->buflim = raw->buffer+client->len;}
        else return 1;}}
    else {}}
  /* Do this ASAP to avoid session leakage */
  fd_reset_threadvars();
  /* Clear outstanding errors from the last session */
  fd_clear_errors(1);
  /* Begin with the new request */
  start_time = u8_elapsed_time();
  getloadavg(start_load,3);
  u8_getrusage(RUSAGE_SELF,&start_usage);

  /* Start doing your stuff */
  if (fd_update_file_modules(0)<0) {
    u8_condition c; u8_context cxt; u8_string details = NULL;
    lispval irritant;
    u8_log(LOG_CRIT,"ModuleUpdateFailed","Failure updating file modules");
    setup_time = parse_time = u8_elapsed_time();
    if (fd_poperr(&c,&cxt,&details,&irritant))
      proc = fd_err(c,cxt,details,irritant);
    if (details) u8_free(details);
    fd_decref(irritant);
    setup_time = u8_elapsed_time();
    cgidata = fd_read_dtype(inbuf);}
  else if (update_preloads()<0) {
    u8_condition c; u8_context cxt; u8_string details = NULL;
    lispval irritant;
    setup_time = parse_time = u8_elapsed_time();
    u8_log(LOG_CRIT,"PreloadUpdateFailed","Failure updating preloads");
    if (fd_poperr(&c,&cxt,&details,&irritant))
      proc = fd_err(c,cxt,details,irritant);
    if (details) u8_free(details);
    fd_decref(irritant);
    setup_time = u8_elapsed_time();
    cgidata = fd_read_dtype(inbuf);}
  else {
    /* This is where we usually end up, when all the updates
       and preloads go without a hitch. */
    setup_time = u8_elapsed_time();
    /* Now we extract arguments and figure out what we're going to
       run to respond to the request. */
    cgidata = fd_read_dtype(inbuf);
    if (cgidata == FD_EOD) {
      if (traceweb>0)
        u8_log(LOG_NOTICE,"FDServlet/webservefn",
               "Client %s (sock=%d) closing",
               client->idstring,client->socket);
      if (ucl->status) {u8_free(ucl->status); ucl->status = NULL;}
      u8_client_done(ucl);
      u8_client_close(ucl);
      return -1;}
    else if (!(TABLEP(cgidata))) {
      u8_log(LOG_CRIT,"FDServlet/webservefn",
             "Bad fdservlet request on client %s (sock=%d), closing",
             client->idstring,client->socket);
      if (ucl->status) {u8_free(ucl->status); ucl->status = NULL;}
      u8_client_done(ucl);
      u8_client_close(ucl);
      return -1;}
    else {}
    if (docroot) webcommon_adjust_docroot(cgidata,docroot);
    user   = fd_get(cgidata,remote_user,VOID);
    path   = fd_get(cgidata,script_filename,VOID);
    uri    = fd_get(cgidata,uri_slotid,VOID);
    method = fd_get(cgidata,request_method,VOID);
    addr   = fd_get(cgidata,remote_host,VOID);
    if (FD_VOIDP(addr)) addr = fd_get(cgidata,remote_addr,VOID);
    u8_printf(logcxtout,"%s%s%s %s %s (%s)",
              (FD_STRINGP(user)) ? (FD_CSTRING(user)) : (U8S("")),
              (FD_STRINGP(user)) ? ("@") : (""),
              (FD_STRINGP(addr)) ? (FD_CSTRING(addr)) : U8S("?remote?"),
              (FD_STRINGP(method)) ? (FD_CSTRING(method)) :
              (FD_SYMBOLP(method)) ? (FD_SYMBOL_NAME(method)) : (U8S("?OP?")),
              (FD_STRINGP(uri)) ? (FD_CSTRING(uri)) : U8S("?uri?"),
              (FD_STRINGP(path)) ? (FD_CSTRING(path)) : U8S("?scriptfile?"));
    u8_set_log_context(logcxt.u8_outbuf);

    /* This is where we parse all the CGI variables, etc */

    /* First record both when we got the request and when we started
       processing it */
    double now = u8_elapsed_time();
    time_t tick = time(NULL);
    lispval ltime = fd_make_double(now-start_time);
    lispval etime = fd_make_double(now);
    lispval tickval = FD_INT(tick);
    fd_store(cgidata,reqstart_symbol,etime);
    fd_store(cgidata,reqtick_symbol,ltime);
    fd_store(cgidata,loadtime_symbol,ltime);
    fd_decref(etime); fd_decref(ltime); fd_decref(tickval);

    fd_parse_cgidata(cgidata);
    forcelog = fd_req_test(forcelog_slotid,VOID);
    if ((forcelog)||(traceweb>0)) {
      lispval referer = fd_get(cgidata,referer_slotid,VOID);
      lispval remote = fd_get(cgidata,remote_info,VOID);
      if (STRINGP(uri))
        ucl->status = u8_strdup(CSTRING(uri));
      if ((STRINGP(uri)) &&
          (STRINGP(referer)) &&
          (STRINGP(remote)))
        u8_log(LOG_NOTICE,"REQUEST",
               "Handling 0x%lx (load=%f/%f/%f)\n\t> %q %s\n\t> from %s\n\t> for %s",
               (unsigned long)ucl,start_load[0],start_load[1],start_load[2],
               method,CSTRING(uri),CSTRING(referer),CSTRING(remote));
      else if ((STRINGP(uri)) &&  (STRINGP(remote)))
        u8_log(LOG_NOTICE,"REQUEST",
               "Handling 0x%lx (load=%f/%f/%f)\n\t> %q %s\n\t> for %s",
               (unsigned long)ucl,start_load[0],start_load[1],start_load[2],
               method,CSTRING(uri),CSTRING(remote));
      else if ((STRINGP(uri)) &&  (STRINGP(referer)))
        u8_log(LOG_NOTICE,"REQUEST",
               "Handling 0x%lx (load=%f/%f/%f)\n\t> %q %s\n\t> from %s",
               (unsigned long)ucl,start_load[0],start_load[1],start_load[2],
               method,CSTRING(uri),CSTRING(referer));
      else if (STRINGP(uri))
        u8_log(LOG_NOTICE,"REQUEST",
               "Handling 0x%lx (load=%f/%f/%f)\n\t> %q %s",
               (unsigned long)ucl,start_load[0],start_load[1],start_load[2],
               method,CSTRING(uri));
      fd_decref(remote);
      fd_decref(referer);}
    else {
      if (STRINGP(uri))
        ucl->status = u8_strdup(CSTRING(uri));}

    /* This is what we'll execute, be it a procedure or FDXML */
    proc = getcontent(path);}

  u8_set_default_output(outstream);
  init_cgidata = fd_deep_copy(cgidata);
  fd_use_reqinfo(cgidata); fd_reqlog(1);
  fd_thread_set(browseinfo_symbol,EMPTY);
  parse_time = u8_elapsed_time();
  if ((FD_ABORTP(proc))&&(u8_current_exception!=NULL)) {
    u8_log(LOG_WARN,u8_current_exception->u8x_cond,
           "Problem getting content from %q for %q",
           path,uri);
    if (u8_current_exception->u8x_cond == fd_FileNotFound) {
      fd_clear_errors(1);
      if (FD_VOIDP(default_notfoundpage)) {
        fd_incref(fallback_notfoundpage); fd_decref(proc);
        proc = fallback_notfoundpage;}
      else if (STRINGP(default_notfoundpage)) {
        u8_string errtext = CSTRING(default_notfoundpage);
        fd_req_store(status_symbol,FD_INT(404));
        u8_log(LOG_WARN,"NotFound","Using %s",errtext);
        if (strchr(errtext,'\n'))
          fd_req_store(content_slotid,default_notfoundpage);
        else if (*errtext=='/') {
          fd_req_store(doctype_slotid,FD_FALSE);
          fd_req_store(sendfile_slotid,default_notfoundpage);}
        else {
          fd_req_store(doctype_slotid,FD_FALSE);
          fd_req_store(redirect_slotid,default_notfoundpage);}
        fd_decref(proc); proc = VOID;}
      else {
        fd_incref(default_notfoundpage); fd_decref(proc);
        proc = default_notfoundpage;}}
    if (FD_ABORTP(proc)) {
      if (!(FD_VOIDP(default_nocontentpage)))
        fd_incref(default_nocontentpage);
      proc = default_nocontentpage;}}
  else if (FD_ABORTP(proc)) {
    u8_log(LOG_CRIT,"BadWebProc",
           "Getting procedure failed for request %q",cgidata);}
  else {}
  if ((reqlog) || (urllog) || (trace_cgidata))
    dolog(cgidata,FD_NULL,NULL,-1,parse_time-start_time);
  if (!(FD_ABORTP(proc)))
    precheck = run_preflight();
  if (FD_ABORTP(proc)) result = fd_incref(proc);
  else if (!((FALSEP(precheck))||
             (VOIDP(precheck))||
             (EMPTYP(precheck))))
    result = precheck;
  else if (TYPEP(proc,fd_cprim_type)) {
    if ((forcelog)||(traceweb>1))
      u8_log(LOG_NOTICE,"START",
             "Handling %q (%q) with primitive procedure %q (#%lx)",
             uri,path,proc,(unsigned long)ucl);
    result = fd_apply(proc,0,NULL);}
  else if (FD_LAMBDAP(proc)) {
    struct FD_LAMBDA *sp = FD_CONSPTR(fd_lambda,proc);
    if ((forcelog)||(traceweb>1))
      u8_log(LOG_NOTICE,"START",
             "Handling %q (%q) with lambda procedure %q (#%lx)",
             uri,path,proc,(unsigned long)ucl);
    base_env = sp->lambda_env;
    threadcache = checkthreadcache(sp->lambda_env);
    result = fd_cgiexec(proc,cgidata);}
  else if ((FD_PAIRP(proc))&&
           (FD_LAMBDAP((FD_CAR(proc))))) {
    struct FD_LAMBDA *sp = FD_CONSPTR(fd_lambda,FD_CAR(proc));
    if ((forcelog)||(traceweb>1))
      u8_log(LOG_NOTICE,"START",
             "Handling %q (%q) with lambda procedure %q (#%lx)",
             uri,path,proc,(unsigned long)ucl);
    threadcache = checkthreadcache(sp->lambda_env);
    /* This should possibly put the CDR of proc into the environment chain,
       but it no longer does. ?? */
    base_env = sp->lambda_env;
    result = fd_cgiexec(FD_CAR(proc),cgidata);}
  else if (FD_PAIRP(proc)) {
    /* This is handling FDXML */
    lispval setup_proc = VOID;
    fd_lexenv base = fd_consptr(fd_lexenv,FD_CDR(proc),fd_lexenv_type);
    fd_lexenv runenv = fd_make_env(fd_incref(cgidata),base);
    base_env = base;
    if (base) fd_load_latest(NULL,base,NULL);
    threadcache = checkthreadcache(base);
    if ((forcelog)||(traceweb>1))
      u8_log(LOG_NOTICE,"START",
             "Handling %q (%q) with template (#%lx)",
             uri,path,(unsigned long)ucl);
    setup_proc = fd_symeval(setup_symbol,base);
    /* Run setup procs */
    if (FD_VOIDP(setup_proc)) {}
    else if (CHOICEP(setup_proc)) {
      FD_DO_CHOICES(proc,setup_proc)
        if (FD_APPLICABLEP(proc)) {
          lispval v = fd_apply(proc,0,NULL);
          fd_decref(v);}}
    else if (FD_APPLICABLEP(setup_proc)) {
      lispval v = fd_apply(setup_proc,0,NULL);
      fd_decref(v);}
    fd_decref(setup_proc);
    /* We assume that the FDXML contains headers, so we won't add them. */
    write_headers = 0;
    fd_output_xml_preface(&(client->out),cgidata);
    if (FD_PAIRP(FD_CAR(proc))) {
      FD_DOLIST(expr,FD_CAR(proc)) {
        fd_decref(result);
        result = fd_xmleval(&(client->out),expr,runenv);
        if (FD_ABORTP(result)) break;}}
    else result = fd_xmleval(&(client->out),FD_CAR(proc),runenv);
    fd_decref((lispval)runenv);}
  else {} /* This is the case where the error was found earlier. */
  exec_time = u8_elapsed_time();
  /* We're now done with all the core computation. */
  if (!(FD_TROUBLEP(result))) {
    /* See if the content or retfile will get us into trouble. */
    content = fd_get(cgidata,content_slotid,VOID);
    retfile = (FD_VOIDP(content)) ?
      (fd_get(cgidata,sendfile_slotid,VOID)):
      (VOID);
    if ((!(FD_VOIDP(content)))&&
        (!((STRINGP(content))||(PACKETP(content))))) {
      fd_decref(result);
      result = fd_err(fd_TypeError,"FDServlet/content","string or packet",
                      content);}
    else if ((!(FD_VOIDP(retfile)))&&
             ((!(STRINGP(retfile)))||
              (!(u8_file_existsp(CSTRING(retfile)))))) {
      fd_decref(result);
      result = fd_err(u8_CantOpenFile,"FDServlet/retfile","existing filename",
                      retfile);}}
  /* Output is done, so stop writing */
  if (!(FD_TROUBLEP(result))) u8_set_default_output(NULL);
  else recovered = 0;
  if (FD_TROUBLEP(result)) {
    u8_exception ex = u8_erreify();
    /* errorpage is used when errors occur.  Currently, it can be a
       procedure (to be called) or an HTML string to be returned.  */
    lispval errorpage = (base_env) ?
      (fd_symeval(errorpage_symbol,base_env)) :
      (VOID);
    int depth = 0;
    u8_byte tmpbuf[100];
    lispval irritant = fd_get_irritant(ex);
    u8_string irritation = (FD_VOIDP(irritant)) ? (NULL) :
      (u8_bprintf(tmpbuf,"\n\t    irritant=%q",irritant));
    if (((FD_VOIDP(errorpage))||(errorpage == FD_UNBOUND))&&
        (!(FD_VOIDP(default_errorpage)))) {
      fd_incref(default_errorpage);
      errorpage = default_errorpage;}
    if ( (STRINGP(path)) && (STRINGP(uri)) && (STRINGP(method)) )
      u8_log(LOG_ERR,ex->u8x_cond,
             "Unexpected error \"%m\" @%s (%s) (#%lx) %s"
             "\n\t  using '%s' to"
             "\n%s %s",
             ex->u8x_cond,ex->u8x_context,
             ex->u8x_details,(unsigned long)ucl,irritation,
             FD_CSTRING(path),FD_CSTRING(method),FD_CSTRING(uri));
    else if (STRINGP(path))
      u8_log(LOG_ERR,ex->u8x_cond,
             "Unexpected error \"%m\" @%s (%s) (#%lx)%s\n from '%s'",
             ex->u8x_cond,ex->u8x_context,
             ex->u8x_details,(unsigned long)ucl,
             irritation,
             FD_CSTRING(path));
    else if (STRINGP(uri))
      u8_log(LOG_ERR,ex->u8x_cond,
             "Unexpected error \"%m\" @%s (%s) (#%lx)%s\n to %s %s",
             ex->u8x_cond,ex->u8x_context,
             ex->u8x_details,(unsigned long)ucl,
             irritation,
             ( (FD_STRINGP(method)) ? (FD_CSTRING(method)) : (U8S(""))),
             FD_CSTRING(uri)) ;
    else u8_log(LOG_ERR,ex->u8x_cond,
                "Unexpected error \"%m\" %s:@%s (%s) (#%lx)%s",
                ex->u8x_cond,ex->u8x_context,ex->u8x_details,
                (unsigned long)ucl,
                irritation);
    if (logstack) {
      lispval backtrace=FD_U8X_STACK(ex);
      if (FD_PAIRP(backtrace)) {
        FD_DOLIST(entry,backtrace) {
          u8_log(LOG_ERR,"Backtrace","%Q",entry);}}}
    if (FD_APPLICABLEP(errorpage)) {
      lispval err_value = fd_wrap_exception(ex);
      add_reqdata_to_error(err_value,cgidata);
      fd_push_reqinfo(init_cgidata);
      fd_store(init_cgidata,error_symbol,err_value); fd_decref(err_value);
      fd_store(init_cgidata,reqdata_symbol,cgidata); fd_decref(cgidata);
      if (outstream->u8_write>outstream->u8_outbuf) {
        /* Get all the output to date as a string and store it in the
           request. */
        lispval output = fd_make_string
          (NULL,outstream->u8_write-outstream->u8_outbuf,
           outstream->u8_outbuf);
        /* Save the output to date on the request */
        fd_store(init_cgidata,output_symbol,output);
        fd_decref(output);}
      fd_decref(cgidata);
      cgidata = fd_deep_copy(init_cgidata);
      fd_use_reqinfo(cgidata);
      /* Reset the output stream */
      outstream->u8_write = outstream->u8_outbuf;
      /* Apply the error page object */
      result = fd_cgiexec(errorpage,cgidata);
      if (FD_ABORTP(result)) {
        u8_exception newex = u8_erreify(), lastex = newex;
        u8_exception exscan = newex; depth=0;
        lispval crisispage = (base_env) ?
          (fd_symeval(crisispage_symbol,base_env)):
          (VOID);
        if (((FD_VOIDP(crisispage))||(crisispage == FD_UNBOUND))&&
            (!(FD_VOIDP(default_crisispage)))) {
          fd_incref(default_crisispage);
          crisispage = default_crisispage;}
        while ((exscan)&&(depth<max_error_depth)) {
          u8_condition excond = exscan->u8x_cond;
          u8_context excxt = ((exscan->u8x_context) ? (exscan->u8x_context) :
                            ((u8_context)"somewhere"));
          u8_context exdetails = ((exscan->u8x_details) ? (exscan->u8x_details) :
                                ((u8_string)"no more details"));
          lispval irritant = fd_exception_xdata(exscan);
          u8_string spath = (STRINGP(path)) ? (CSTRING(path)) : (NULL);
          u8_string suri = (STRINGP(uri)) ? (CSTRING(uri)) : (NULL);
          if ( (spath) || (suri) )
            u8_log(LOG_ERR,excond,
                   "Unexpected recursive error '%m' @%s (%s) (#%lx)\n  %s (%s)",
                   excond,excxt,exdetails,(unsigned long)ucl,
                   suri,spath);
          else u8_log(LOG_ERR,excond,
                      "Unexpected recursive error '%m' %s:@%s (%s) (#%lx)",
                      excond,excxt,exdetails,(unsigned long)ucl);
          if (!(FD_VOIDP(irritant)))
            u8_log(LOG_ERR,excond,"Irritant: %q",irritant);
          lastex = exscan; exscan = exscan->u8x_prev; depth++;}
        while (exscan) {
          lastex = exscan;
          exscan = exscan->u8x_prev;
          depth++;}
        /* Add the previous exception to this one as we go forward */
        if (lastex) {
          lastex->u8x_prev = ex;
          ex = newex;}
        fd_decref(errorpage); errorpage = VOID;
        if (STRINGP(crisispage)) errorpage = crisispage;}
      else {
        fd_clear_errors(1);
        recovered = 1;}}
    if (!(FD_TROUBLEP(result))) {
      /* We got something to return, so we don't bother
         with all the various other error cases.  */ }
    else if ((STRINGP(errorpage))&&
             (strstr(CSTRING(errorpage),"\n")!=NULL)) {
      /* Assume that the error page is a string of HTML */
      http_len = http_len+strlen(HTML_UTF8_CTYPE_HEADER);
      write_string(client->socket,HTML_UTF8_CTYPE_HEADER);
      write_string(client->socket,CSTRING(errorpage));}
    else if ((STRINGP(errorpage))&&
             ((CSTRING(errorpage)[0]=='/')||
              (u8_has_prefix(CSTRING(errorpage),"http:",0))||
              (u8_has_prefix(CSTRING(errorpage),"https:",0)))) {
      struct U8_OUTPUT tmpout; U8_INIT_STATIC_OUTPUT(tmpout,1024);
      write_string(client->socket,"Status: 307\r\nLocation: ");
      write_string(client->socket,CSTRING(errorpage));
      write_string(client->socket,"\r\n\r\n");
      u8_printf(&tmpout,"<html>\n"
                "<head>\n<title>Sorry, redirecting...</title>\n</head>\n"
                "<body>\n");
      u8_printf(&tmpout,"<p>Redirecting to <a href='%s'>%s</a></p>"
                "\n</body>\n</html>\n",
                errorpage,errorpage);
      write_string(client->socket,tmpout.u8_outbuf);
      u8_free(tmpout.u8_outbuf);}
    else if (STRINGP(errorpage)) {
      /* This should check for redirect URLs, but for now it
         just dumps the error page as plain text.  */
      http_len = http_len+
        strlen("Content-type: text/plain; charset = utf-8\r\n\r\n");
      write_string(client->socket,
                   "Content-type: text/plain; charset = utf-8\r\n\r\n");
      write_string(client->socket,CSTRING(errorpage));}
    else if ((webdebug)||
             ((weballowdebug)&&(fd_req_test(webdebug_symbol,VOID)))) {
      http_len = http_len+
        strlen("Content-type: text/html; charset = utf-8\r\n\r\n");
      write_string(client->socket,
                   "Content-type: text/html; charset = utf-8\r\n\r\n");
      fd_xhtmldebugpage(&(client->out),ex);}
    else {
      http_len = http_len+
        strlen("Content-type: text/html; charset = utf-8\r\n\r\n");
      write_string(client->socket,
                   "Content-type: text/html; charset = utf-8\r\n\r\n");
      fd_xhtmlerrorpage(&(client->out),ex);}
    if (!(recovered)) {
      if ((reqlog) || (urllog) || (trace_cgidata))
        dolog(cgidata,result,outstream->u8_outbuf,
              outstream->u8_write-outstream->u8_outbuf,
              u8_elapsed_time()-start_time);
      content_len = content_len+(outstream->u8_write-outstream->u8_outbuf);
      /* We do a hanging write in this, hoping it's not common case */
      u8_writeall(client->socket,outstream->u8_outbuf,
                  outstream->u8_write-outstream->u8_outbuf);
      return_code = -1;
      fd_decref(errorpage);
      /* And close the client for good measure */
      if (ucl->status) {u8_free(ucl->status); ucl->status = NULL;}
      u8_client_done(ucl);
      u8_client_close(ucl);}
    if (ex) u8_free_exception(ex,1);}
  if (recovered) {
    U8_OUTPUT httphead, htmlhead; int tracep;
    lispval traceval = fd_get(cgidata,tracep_slotid,VOID);
    if (FD_VOIDP(traceval)) tracep = 0; else tracep = 1;
    U8_INIT_STATIC_OUTPUT(httphead,1024); U8_INIT_STATIC_OUTPUT(htmlhead,1024);
    http_status = fd_output_http_headers(&httphead,cgidata);
    if ((FD_VOIDP(content))&&(FD_VOIDP(retfile))) {
      char clen_header[128]; size_t bundle_len = 0;
      if (write_headers) {
        close_html = fd_output_xhtml_preface(&htmlhead,cgidata);
        head_len = (htmlhead.u8_write-htmlhead.u8_outbuf);
        if (close_html) u8_puts(outstream,"\n</body>\n</html>\n");}
      content_len = head_len+(outstream->u8_write-outstream->u8_outbuf);
      sprintf(clen_header,"Content-length: %lu\r\n\r\n",
              (unsigned long)content_len);
      u8_puts(&httphead,clen_header);
      content_len = outstream->u8_write-outstream->u8_outbuf;
      http_len = httphead.u8_write-httphead.u8_outbuf;
      head_len = htmlhead.u8_write-htmlhead.u8_outbuf;
      bundle_len = http_len+head_len+content_len;
      if (!(async)) {
        retval = u8_writeall(client->socket,httphead.u8_outbuf,http_len);
        if (retval>=0)
          retval = u8_writeall(client->socket,htmlhead.u8_outbuf,head_len);
        if (retval>=0)
          retval = u8_writeall(client->socket,outstream->u8_outbuf,content_len);
        return_code = 0;}
      else {
        u8_byte *start;
        ssize_t rv = u8_grow_stream
          ((u8_stream)outstream,(head_len+http_len+U8_BUF_MIN_GROW));
        if (rv>0) {
          start = outstream->u8_outbuf;
          memmove(start+head_len+http_len,start,content_len);
          strncpy(start,httphead.u8_outbuf,http_len);
          strncpy(start+http_len,htmlhead.u8_outbuf,head_len);
          outstream->u8_write = start+http_len+head_len+content_len;
          u8_client_write(ucl,start,bundle_len,0);
          buffered = 1;
          return_code = 1;}
        else {/* To be written */}}}
    else if ((STRINGP(retfile))&&(fd_sendfile_header)) {
      u8_byte *copy;
      u8_log(LOG_NOTICE,"FDServlet/Sendfile","Using %s to pass %s (#%lx)",
             fd_sendfile_header,CSTRING(retfile),(unsigned long)ucl);
      /* The web server supports a sendfile header, so we use that */
      u8_printf(&httphead,"\r\n");
      http_len = httphead.u8_write-httphead.u8_outbuf;
      copy = u8_strdup(httphead.u8_outbuf);
      u8_client_write_x(ucl,copy,http_len,0,U8_CLIENT_WRITE_OWNBUF);
      buffered = 1; return_code = 1;}
    else if (STRINGP(retfile)) {
      /* This needs more error checking, signalling, etc */
      u8_string filename = CSTRING(retfile);
      struct stat fileinfo; FILE *f;
      if ((stat(filename,&fileinfo)==0)&&(f = u8_fopen(filename,"rb")))  {
        int bytes_read = 0;
        unsigned char *filebuf = NULL; fd_off_t total_len = -1;
        u8_log(LOG_NOTICE,"FDServlet/Sendfile","Returning content of %s (#%lx)",
               CSTRING(retfile),(unsigned long)ucl);
        u8_printf(&httphead,"Content-length: %ld\r\n\r\n",
                  (long int)(fileinfo.st_size));
        http_len = httphead.u8_write-httphead.u8_outbuf;
        total_len = http_len+fileinfo.st_size;
        if ((async)&&(total_len<FD_FILEBUF_MAX))
          filebuf = u8_malloc(total_len+1);
        if (filebuf) {
          /* This is the case where we hand off a buffer to u8_server
             to write for us. */
          unsigned char *write = filebuf+http_len;
          fd_off_t to_read = fileinfo.st_size;
          memcpy(filebuf,httphead.u8_outbuf,http_len);
          /* Copy the whole file */
          while ((to_read>0)&&
                 ((bytes_read = fread(write,sizeof(uchar),to_read,f))>0)) {
            write = write+bytes_read;
            to_read = to_read-bytes_read;}
          if (((server->flags)&(U8_SERVER_LOG_TRANSACT))||
              ((client->flags)&(U8_CLIENT_LOG_TRANSACT)))
            u8_log(LOG_WARN,"FDServlet/Buffering/file",
                   "Queued %d+%d=%d file bytes of 0x%lx for output (#%lx)",
                   http_len,to_read,total_len,(unsigned long)filebuf,
                   (unsigned long)ucl);
          u8_client_write_x(ucl,filebuf,total_len,0,U8_CLIENT_WRITE_OWNBUF);
          buffered = 1; return_code = 1;
          fclose(f);}
        else {
          char buf[32768];
          /* This is the case where we hang while we write. */
          retval = u8_writeall(client->socket,httphead.u8_outbuf,http_len);
          if (retval<0) return_code = -1;
          else {
            while ((bytes_read = fread(buf,sizeof(uchar),32768,f))>0) {
              content_len = content_len+bytes_read;
              retval = u8_writeall(client->socket,buf,bytes_read);
              if (retval<0) break;}
            return_code = 0;}}}
      else {
        u8_log(LOG_NOTICE,"FDServlet/Sendfile",
               "The content file %s does not exist (#%lx)",
               CSTRING(retfile),(unsigned long)ucl);
        u8_seterr(fd_FileNotFound,"fdservlet/sendfile",
                  u8_strdup(CSTRING(retfile)));
        result = FD_ERROR_VALUE;}}
    else if (STRINGP(content)) {
      int bundle_len; unsigned char *outbuf = NULL;
      content_len = STRLEN(content);
      u8_printf(&httphead,"Content-length: %ld\r\n\r\n",content_len);
      http_len = httphead.u8_write-httphead.u8_outbuf;
      bundle_len = http_len+content_len;
      if (async) outbuf = u8_malloc(bundle_len+1);
      if (outbuf) {
        memcpy(outbuf,httphead.u8_outbuf,http_len);
        memcpy(outbuf+http_len,CSTRING(content),content_len);
        outbuf[bundle_len]='\0';
        u8_client_write_x(ucl,outbuf,bundle_len,0,U8_CLIENT_WRITE_OWNBUF);
        if (((server->flags)&(U8_SERVER_LOG_TRANSACT))||
            ((client->flags)&(U8_CLIENT_LOG_TRANSACT)))
          u8_log(LOG_WARN,"FDServlet/Buffering/text",
                 "Queued %d+%d=%d string bytes of 0x%lx for output (#%lx)",
                 http_len,content_len,bundle_len,(unsigned long)outbuf,
                 (unsigned long)ucl);
        buffered = 1; return_code = 1;}
      else  {
        retval = u8_writeall(client->socket,httphead.u8_outbuf,
                           httphead.u8_write-httphead.u8_outbuf);
        if (retval>=0)
          retval = u8_writeall(client->socket,CSTRING(content),STRLEN(content));}}
    else if (PACKETP(content)) {
      int bundle_len; unsigned char *outbuf = NULL;
      content_len = FD_PACKET_LENGTH(content);
      u8_printf(&httphead,"Content-length: %ld\r\n\r\n",content_len);
      http_len = httphead.u8_write-httphead.u8_outbuf;
      bundle_len = http_len+content_len;
      if (async) outbuf = u8_malloc(bundle_len);
      if (outbuf) {
        memcpy(outbuf,httphead.u8_outbuf,http_len);
        memcpy(outbuf+http_len,FD_PACKET_DATA(content),content_len);
        if (((server->flags)&(U8_SERVER_LOG_TRANSACT))||
            ((client->flags)&(U8_CLIENT_LOG_TRANSACT)))
          u8_log(LOG_WARN,"FDServlet/buffering/packet",
                 "Queued %d+%d=%d packet bytes of 0x%lx for output (#%lx)",
                 http_len,content_len,bundle_len,(unsigned long)outbuf,
                 (unsigned long)ucl);
        u8_client_write_x(ucl,outbuf,bundle_len,0,U8_CLIENT_WRITE_OWNBUF);
        buffered = 1; return_code = 1;}
      else {
        retval = u8_writeall(client->socket,httphead.u8_outbuf,
                             httphead.u8_write-httphead.u8_outbuf);
        if (retval>=0)
          retval = u8_writeall(client->socket,FD_PACKET_DATA(content),
                               FD_PACKET_LENGTH(content));}}
    else {
      /* Where the servlet has specified some particular content */
      content_len = content_len+output_content(client,content);}
    /* Reset the stream */
    outstream->u8_write = outstream->u8_outbuf;
    /* If we're not still in the transaction, call u8_client_done() */
    /* if (!(return_code)) {u8_client_done(ucl);} */
    if ((forcelog)||(traceweb>2))
      u8_log(LOG_NOTICE,"FDServlet/HTTPHEAD",
             "(#%lx) HTTPHEAD=%s",(unsigned long)ucl,httphead.u8_outbuf);
    u8_free(httphead.u8_outbuf); u8_free(htmlhead.u8_outbuf);
    fd_decref(content); fd_decref(traceval); fd_decref(retfile);
    if (retval<0)
      u8_log(LOG_ERROR,"FDServlet/BADRET",
             "Bad retval from writing data (#%lx)",(unsigned long)ucl);
    if ((reqlog) || (urllog) || (trace_cgidata) || (tracep))
      dolog(cgidata,result,client->out.u8_outbuf,
            outstream->u8_write-outstream->u8_outbuf,
            u8_elapsed_time()-start_time);}
  if (fd_test(cgidata,cleanup_slotid,VOID)) {
    lispval cleanup = fd_get(cgidata,cleanup_slotid,FD_EMPTY);
    FD_DO_CHOICES(cl,cleanup) {
      lispval cleanup_val = fd_apply(cleanup,0,NULL);
      fd_decref(cleanup_val);}}
  run_postflight();
  if (threadcache) fd_pop_threadcache(threadcache);
  fd_use_reqinfo(EMPTY);
  fd_reqlog(-1);
  fd_thread_set(browseinfo_symbol,VOID);
  fd_decref(init_cgidata); init_cgidata = VOID;
  fd_clear_errors(1);
  write_time = u8_elapsed_time();
  getloadavg(end_load,3);
  u8_getrusage(RUSAGE_SELF,&end_usage);
  if ((forcelog)||(traceweb>0)||
      ((overtime>0)&&((write_time-start_time)>overtime))) {
    lispval query = fd_get(cgidata,query_slotid,VOID);
    lispval redirect = fd_get(cgidata,redirect_slotid,VOID);
    lispval sendfile = fd_get(cgidata,sendfile_slotid,VOID);
    lispval xredirect = fd_get(cgidata,xredirect_slotid,VOID);
    if (!(FD_VOIDP(redirect)))
      u8_log(LOG_NOTICE,"FDServlet/REQUEST/REDIRECT","to %q",redirect);
    else if (!(FD_VOIDP(xredirect)))
      u8_log(LOG_NOTICE,"FDServlet/REQUEST/XREDIRECT","to %q",xredirect);
    else if (!(FD_VOIDP(sendfile)))
      u8_log(LOG_NOTICE,"FDServlet/REQUEST/SENDFILE","to %q",sendfile);
    else {}
    if ((FD_VOIDP(query))||((STRINGP(query))&&(STRLEN(query)==0)))
      u8_log(LOG_NOTICE,"FDServlet/REQUEST/DONE",
             "%q (%d) %s %d=%d+%d+%d bytes (#%lx)\n\t< %q %s\n\t< generated "
             "by %q\n\t< taking %f = setup:%f+req:%f+run:%f+write:%f secs, "
             "stime=%.2fms, utime=%.2fms, load=%f/%f/%f",
             method,http_status,((buffered)?("buffered"):("sent")),
             http_len+head_len+content_len,http_len,head_len,content_len,
             (unsigned long)ucl,method,CSTRING(uri),path,
             write_time-start_time,
             setup_time-start_time,
             parse_time-setup_time,
             exec_time-parse_time,
             write_time-exec_time,
             (u8_dbldifftime(end_usage.ru_utime,start_usage.ru_utime))/1000.0,
             (u8_dbldifftime(end_usage.ru_stime,start_usage.ru_stime))/1000.0,
             end_load[0],end_load[1],end_load[2]);
    else u8_log(LOG_NOTICE,"FDSerlvet/REQUEST/DONE",
                "%q (%d) %s %d=%d+%d+%d bytes (#%lx)\n\t< %q %s\n\t< "
                "generated by %q\n\t< from query %q\n\t< "
                "taking %f = setup:%f+req:%f+run:%f+write:%f secs, "
                "stime=%.2fms, utime=%.2fms, load=%f/%f/%f",
                method,http_status,((buffered)?("buffered"):("sent")),
                http_len+head_len+content_len,http_len,head_len,content_len,
                (unsigned long)ucl,method,CSTRING(uri),path,query,
                write_time-start_time,
                setup_time-start_time,
                parse_time-setup_time,
                exec_time-parse_time,
                write_time-exec_time,
                (u8_dbldifftime(end_usage.ru_utime,start_usage.ru_utime))/1000.0,
                (u8_dbldifftime(end_usage.ru_stime,start_usage.ru_stime))/1000.0,
                end_load[0],end_load[1],end_load[2]);
    if ((forcelog)||((overtime>0)&&((write_time-start_time)>overtime))) {
      u8_string cond = (((overtime>0)&&((write_time-start_time)>overtime))?
                      ("OVERTIME"):("FORCELOG"));
      u8_string before = u8_rusage_string(&start_usage);
      u8_string after = u8_rusage_string(&end_usage);
      u8_log(LOG_NOTICE,cond,"before: %s",before);
      u8_log(LOG_NOTICE,cond," after: %s",after);
      u8_free(before); u8_free(after);}
    /* If we're calling traceweb, keep the log files up to date also. */
    u8_lock_mutex(&log_lock);
    if (urllog) fflush(urllog);
    if (reqlog) fd_flush_stream(reqlog);
    u8_unlock_mutex(&log_lock);
    fd_decref(xredirect);
    fd_decref(redirect);
    fd_decref(sendfile);
    fd_decref(query);}
  else {}
  u8_set_log_context(NULL);
  fd_decref(proc);
  fd_decref(result);
  fd_decref(path);
  fd_decref(addr);
  fd_decref(uri);
  fd_decref(user);
  fd_decref(method);
  fd_decref(cgidata);
  fd_swapcheck();
  /* Task is done */
  if (return_code<=0) {
    if (ucl->status) {u8_free(ucl->status); ucl->status = NULL;}}
  return return_code;
}

static int close_webclient(u8_client ucl)
{
  fd_webconn client = (fd_webconn)ucl;
  u8_log(LOG_INFO,"FDServlet/close","Closing web client %s (#%lx#%d.%d)",
         ucl->idstring,ucl,ucl->clientid,ucl->socket);
  fd_decref(client->cgidata); client->cgidata = VOID;
  /* fd_close_stream(&(client->in),FD_STREAM_NOCLOSE); */
  fd_close_stream(&(client->in),0);
  u8_close((u8_stream)&(client->out));
  return 1;
}

static int reuse_webclient(u8_client ucl)
{
  fd_webconn client = (fd_webconn)ucl;
  lispval cgidata = client->cgidata;
  u8_log(LOG_INFO,"FDServlet/reuse","Reusing web client %s (#%lx)",
         ucl->idstring,ucl);
  fd_decref(cgidata); client->cgidata = VOID;
  return 1;
}

static void shutdown_server()
{
  u8_server_shutdown(&fdwebserver,shutdown_grace);
}

static void shutdown_servlet(u8_condition reason)
{
  if (reason == NULL) reason = "fate";
  int i = n_ports-1;
  u8_lock_mutex(&server_port_lock); i = n_ports-1;
  if (reason)
    u8_log(LOG_WARN,reason,
           "Shutting down, removing socket files and pidfile %s",
           pidfile);
  webcommon_shutdown(reason);
  while (i>=0) {
    u8_string spec = ports[i];
    if (!(spec)) {}
    else if (strchr(spec,'/')) {
      if (remove(spec)<0)
        u8_log(LOG_WARN,"FDServlet/shutdown",
               "Couldn't remove portfile %s",spec);
      u8_free(spec); ports[i]=NULL;}
    else {u8_free(spec); ports[i]=NULL;}
    i--;}
  u8_free(ports);
  ports = NULL; n_ports = 0; max_ports = 0;
  u8_unlock_mutex(&server_port_lock);
  if (pidfile) {
    u8_string filename = pidfile; pidfile = NULL;
    int retval = (u8_file_existsp(filename))?(u8_removefile(filename)):(0);
    if (retval<0)
      u8_log(LOG_WARN,"FDServlet/shutdown",
             "Couldn't remove pid file %s",pidfile);
    u8_free(filename);}
}

static lispval servlet_status_string()
{
  u8_string status = u8_server_status(&fdwebserver,NULL,0);
  return fd_lispstring(status);
}

/* Making sure you can write the socket file */

#define SOCKDIR_PERMISSIONS \
  (S_IRUSR|S_IWUSR|S_IXUSR|S_IRGRP|S_IWGRP|S_IXGRP|S_IROTH|S_IXOTH)

static int check_socket_path(u8_string sockname)
{
  u8_string sockfile = u8_abspath(sockname,NULL);
  u8_string sockdir = u8_dirname(sockfile);
  int retval = u8_mkdirs(sockdir,SOCKDIR_PERMISSIONS);
  if (retval<0) {
    u8_free(sockfile);
    u8_free(sockdir);
    return retval;}
  else if ((u8_file_existsp(sockname)) ?
           (u8_file_writablep(sockname)) :
           (u8_file_writablep(sockdir))) {
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
  int file_socket = ((strchr(spec,'/'))!=NULL);
  int retval;
  if (spec[0]==':') spec = spec+1;
  /* else if (spec[len-1]=='@') spec[len-1]='\0'; */
  else {}
  retval = u8_add_server(&fdwebserver,spec,((file_socket)?(-1):(0)));
  if (retval<0) return retval;
  else if (file_socket) chmod(spec,0777);
  return 0;
}

static int addfdservport(lispval var,lispval val,void *data)
{
  u8_string new_port = NULL;
  u8_lock_mutex(&server_port_lock);
  if (STRINGP(val)) {
    u8_string spec = CSTRING(val);
    if (strchr(spec,'/')) {
      if (check_socket_path(spec)>0) {
        new_port = u8_abspath(spec,NULL);}
      else {
        u8_seterr("Can't write socket file","setportconfig",
                  u8_abspath(spec,NULL));
        u8_unlock_mutex(&server_port_lock);
        return -1;}}
    else if ((strchr(spec,'@'))||(strchr(spec,':')))
      new_port = u8_strdup(spec);
    else if (check_socket_path(spec)>0) {
      new_port = u8_abspath(spec,NULL);}
    else {
      u8_unlock_mutex(&server_port_lock);
      u8_seterr("Can't write socket file","setportconfig",
                u8_abspath(spec,NULL));
      return -1;}}
  else if (FD_FIXNUMP(val))
    new_port = u8_mkstring("%lld",FD_FIX2INT(val));
  else {
    fd_incref(val);
    fd_seterr(fd_TypeError,"setportconfig",NULL,val);
    return -1;}
  if (!(server_id)) server_id = new_port;
  if (n_ports>=max_ports) {
    int new_max = ((max_ports)?(max_ports+8):(8));
    if (ports)
      ports = u8_realloc(ports,sizeof(u8_string)*new_max);
    else ports = u8_malloc(sizeof(u8_string)*new_max);}
  ports[n_ports++]=new_port;
  if (server_running) {
    add_server(new_port);
    u8_unlock_mutex(&server_port_lock);
    return 1;}
  u8_unlock_mutex(&server_port_lock);
  return 0;
}

static lispval getfdservports(lispval var,void *data)
{
  lispval result = EMPTY;
  int i = 0, lim = n_ports;
  u8_lock_mutex(&server_port_lock); lim = n_ports;
  while (i<lim) {
    lispval string = fdstring(ports[i++]);
    CHOICE_ADD(result,string);}
  u8_unlock_mutex(&server_port_lock);
  return result;
}

static int socketp(u8_string filename)
{
  int rv = 0;
  char *localized = u8_tolibc(filename);
  struct stat info;
  if (stat(localized,&info)<0)
    errno=0;
  else if (info.st_mode & S_IFSOCK)
    rv=1;
  else return 0;
  u8_free(localized);
  return rv;
}

static int start_servers()
{
  int i = 0, lim = n_ports, added=0;
  u8_lock_mutex(&server_port_lock); lim = n_ports;
  while (i<lim) {
    u8_string port = ports[i++];
    int add_port = 0;
    if ((strchr(port,'/')) && (u8_file_existsp(port))) {
      if (! (socketp(port)) )
        u8_log(LOG_CRIT,"FDServlet/start",
               "File %s exists and is not a socket",port);
      else if (stealsockets) {
        int retval = u8_removefile(port);
        if (retval<0)
          u8_log(LOG_CRIT,"FDServlet/start",
                 "Couldn't remove existing socket file %s",port);
        else add_port=1;}
      else {
        u8_log(LOG_WARN,"FDServlet/start",
               "Socket file %s already exists",port);}}
    else add_port=1;
    int retval = (add_port) ? (add_server(port)) : (0);
    if (retval<0) {
      u8_log(LOG_CRIT,"FDServlet/START","Couldn't start server %s",ports[i]);
      u8_clear_errors(1);}
    else added++;
    i++;}
  server_running = 1;
  u8_unlock_mutex(&server_port_lock);
  return added;
}

/* Initializing configs */

static void register_servlet_configs()
{
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


  fd_register_config("PORT",_("Ports for listening for connections"),
                     getfdservports,addfdservport,NULL);
  fd_register_config("ASYNCMODE",_("Whether to run in asynchronous mode"),
                     fd_boolconfig_get,fd_boolconfig_set,&async_mode);

  fd_register_config("RESTART",_("Whether to enable auto-restart"),
                     fd_boolconfig_get,fd_boolconfig_set,&daemonize);
  fd_register_config("PIDWAIT",_("Whether to wait for the servlet PID file"),
                     fd_boolconfig_get,fd_boolconfig_set,&pidwait);

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
  if (async_mode) fdwebserver.flags = fdwebserver.flags|U8_SERVER_ASYNC;
  fd_register_config("U8ASYNC",
                     _("Whether to support thread-asynchronous transactions"),
                     config_get_u8server_flag,config_set_u8server_flag,
                     (void *)(U8_SERVER_ASYNC));
#endif
}

/* Fallback pages */

static lispval notfoundpage()
{
  lispval title, ctype = fdstring("text/html");
  struct U8_OUTPUT *body = u8_current_output;
  struct U8_OUTPUT tmpout; U8_INIT_STATIC_OUTPUT(tmpout,1024);
  fd_req_store(status_symbol,FD_INT(404));
  fd_req_store(content_type,ctype);
  fd_req_store(doctype_slotid,FD_FALSE);
  u8_printf(&tmpout,"<title>We couldn't find the named file</title>");
  title = fd_init_string(NULL,-1,tmpout.u8_outbuf);
  fd_req_add(html_headers,title);
  u8_printf(body,"\n<p>We weren't able to find what you were looking for</p>\n");
  fd_decref(ctype); fd_decref(title);
  return VOID;
}

/* The main() event */

FD_EXPORT int fd_init_drivers(void);
static int run_servlet(u8_string socket_spec);

static void exit_fdservlet()
{
  if (!(fd_be_vewy_quiet))
    fd_log_status("Exit(fdservlet)");
}

int main(int argc,char **argv)
{
  int i = 1;

  u8_log_show_date=1;
  u8_log_show_procinfo=1;
  u8_log_show_threadinfo=1;
  u8_log_show_elapsed=1;

  int u8_version = u8_initialize();
  int dtype_version = fd_init_lisp_types();
  int fd_version; /* Wait to set this until we have a log file */
  /* Bit map of args which we handle */
  unsigned char arg_mask[argc];  memset(arg_mask,0,argc);
  u8_string socket_spec = NULL, load_source = NULL;
  u8_string logfile = NULL;

  FD_INIT_STACK();

  if (u8_version<0) {
    u8_log(LOG_CRIT,ServletAbort,"Can't initialize libu8");
    exit(1);}
  if (dtype_version<0) {
    u8_log(LOG_CRIT,ServletAbort,"Can't initialize DTYPE library");
    exit(1);}

  fd_main_errno_ptr = &errno;

  server_sigmask = fd_default_sigmask;

  /* Initialize the libu8 stdio library if it won't happen automatically. */
#if (!(HAVE_CONSTRUCTOR_ATTRIBUTES))
  u8_initialize_u8stdio();
  u8_init_chardata_c();
#endif

  fd_register_config("FOREGROUND",_("Whether to run in the foreground"),
                     fd_boolconfig_get,fd_boolconfig_set,&foreground);

  /* Find the socket spec (the non-config arg) */
  i = 1; while (i<argc) {
    if (isconfig(argv[i])) {
      u8_log(LOG_NOTICE,"FDServletConfig","    %s",argv[i]);
      if (strncasecmp(argv[i],"foreground=",strlen("foreground="))==0) {
        fd_config_assignment(argv[i]);}
      i++;}
    else if (socket_spec) i++;
    else {
      socket_spec = argv[i];
      arg_mask[i] = 'X';
      i++;}
  }
  i = 1;

  if (getenv("STDLOG")) {
    u8_log(LOG_WARN,Startup,"Obeying STDLOG and using stdout/stderr for logging");}
  else if (getenv("LOGFILE")) {
    char *envfile = getenv("LOGFILE");
    if ((envfile)&&(envfile[0])&&(strcmp(envfile,"-")))
      logfile = u8_fromlibc(envfile);}
  else if ((getenv("LOGDIR"))&&(socket_spec)) {
    u8_string base = u8_basename(socket_spec,"*");
    u8_string logname = u8_mkstring("%s.log",base);
    logfile = u8_mkpath(getenv("LOGDIR"),logname);
    u8_free(logname);
    u8_free(base);}
  else if ((!(foreground))&&(socket_spec)) {
    u8_string base = u8_basename(socket_spec,"*");
    u8_string logname = u8_mkstring("%s.log",base);
    logfile = u8_mkpath(FD_SERVLET_LOG_DIR,logname);
    u8_free(logname);
    u8_free(base);}
  else u8_log(LOG_WARN,Startup,"No logfile, using stdout");

  /* Close and reopen STDIN */
  close(0);  if (open("/dev/null",O_RDONLY) == -1) {
    u8_log(LOG_CRIT,ServletAbort,"Unable to reopen stdin for daemon");
    exit(1);}

  u8_string rungroup = getenv("RUNGROUP");
  if (rungroup == NULL) rungroup = FD_WEBGROUP;
  if (rungroup) {
    u8_gid gid = u8_getgid(rungroup);
    if (gid<0)
      u8_log(LOGERR,"UnknownGroupName","%s",rungroup);
    else {
      int rv = setgid(gid);
      if (rv<0) {
        u8_string errstring = u8_strerror(errno); errno=0;
        u8_log(LOGERR,"SetGroupFailed",
               "Couldn't set group to %s (%d): %s",
               rungroup,gid,errstring);}}}

  if ( (geteuid()) == 0) {
    /* Avoid running as root */
    u8_string runuser = getenv("RUNUSER");
    if (runuser == NULL) runuser = FD_WEBUSER;
    if (runuser) {
      u8_uid uid = u8_getgid(runuser);
      if (uid>=0) {
        int rv = setuid(uid);
        if (rv < 0) {
          u8_string errstring = u8_strerror(errno); errno=0;
          u8_log(LOGERR,"SetUserFailed",
                 "Couldn't set user to %s (%d): %s",
                 runuser,uid,errstring);}}
      else {
        u8_log(LOGERR,"UnknownUser","%s",runuser);
        exit(1);}}}

  if ( (geteuid()) == 0)
    u8_log(LOGCRIT,"RootUser","Running as root, probably a bad idea");

  /* We do this using the Unix environment (rather than configuration
     variables) for two reasons.  First, we want to redirect errors
     from the processing of the configuration variables themselves
     (where lots of errors could happen); second, we want to be able
     to set this in the environment we wrap around calls (which is how
     mod_fdserv does it). */
  if (logfile) {
    int logsync = ((getenv("LOGSYNC") == NULL)?(0):(O_SYNC));
    int log_fd = open(logfile,O_RDWR|O_APPEND|O_CREAT|logsync,0644);
    if (log_fd<0) {
      u8_log(LOG_CRIT,ServletAbort,"Couldn't open log file %s",logfile);
      exit(1);}
    dup2(log_fd,1);
    dup2(log_fd,2);}

  u8_init_mutex(&server_port_lock);

  if (!(socket_spec)) {}
  else if (strchr(socket_spec,'/'))
    socket_spec = u8_abspath(socket_spec,NULL);
  else if ((strchr(socket_spec,':'))||(strchr(socket_spec,'@')))
    socket_spec = u8_strdup(socket_spec);
  else {
    u8_string sockets_dir = u8_mkpath(FD_RUN_DIR,"servlets");
    socket_spec = u8_mkpath(sockets_dir,socket_spec);
    u8_free(sockets_dir);}

  register_servlet_configs();
  atexit(exit_fdservlet);

  /* Process the command line */
  fd_handle_argv(argc,argv,arg_mask,NULL);

  FD_NEW_STACK(((struct FD_STACK *)NULL),"fdservlet",NULL,VOID);
  _stack->stack_label=u8_strdup(u8_appid());
  U8_SETBITS(_stack->stack_flags,FD_STACK_FREE_LABEL);

  {
    if (argc>2) {
      struct U8_OUTPUT out; unsigned char buf[2048]; int i = 1;
      U8_INIT_OUTPUT_BUF(&out,2048,buf);
      while (i<argc) {
        unsigned char *arg = argv[i++];
        if (arg == socket_spec) u8_puts(&out," @");
        else {u8_putc(&out,' '); u8_puts(&out,arg);}}
      u8_log(LOG_WARN,Startup,"Starting beingmeta fdservlet %s with\n  %s",
             socket_spec,out.u8_outbuf);
      u8_close((U8_STREAM *)&out);}
    else u8_log(LOG_WARN,Startup,"Starting beingmeta fdservlet %s",socket_spec);
    u8_log(LOG_WARN,Startup,"Copyright (C) beingmeta 2004-2019, all rights reserved");}

  if (socket_spec) {
    ports = u8_malloc(sizeof(u8_string)*8);
    max_ports = 8; n_ports = 1;
    server_id = ports[0]=u8_strdup(socket_spec);}

  fd_version = fd_init_scheme();

  if (fd_version<0) {
    u8_log(LOG_WARN,ServletAbort,"Couldn't initialize FramerD");
    exit(EXIT_FAILURE);}

  /* INITIALIZING MODULES */
  /* Normally, modules have initialization functions called when
     dynamically loaded.  However, if we are statically linked, or we
     don't have the "constructor attributes" use to declare init functions,
     we need to call some initializers explicitly. */

  /* And now we initialize FramerD */
#if ((!(HAVE_CONSTRUCTOR_ATTRIBUTES)) || (FD_TESTCONFIG))
  fd_init_scheme();
#endif


#if ((!(HAVE_CONSTRUCTOR_ATTRIBUTES)) || (FD_TESTCONFIG))
  fd_init_schemeio();
  fd_init_texttools();
  /* May result in innocuous redundant calls */
  FD_INIT_SCHEME_BUILTINS();
  fd_init_dbserv();
#else
  FD_INIT_SCHEME_BUILTINS();
  fd_init_dbserv();
#endif

  /* This is the module where the data-access API lives */
  fd_register_module("DBSERV",fd_incref(fd_dbserv_module),FD_MODULE_SAFE);
  fd_finish_module(fd_dbserv_module);

  fd_init_fdweb();
  fd_init_drivers();

  init_webcommon_data();
  init_webcommon_symbols();

  fallback_notfoundpage = fd_make_cprim0("NOTFOUND404",notfoundpage);

  /* This is the root of all client service environments */
  if (server_env == NULL) server_env = fd_working_lexenv();
  fd_idefn((lispval)server_env,fd_make_cprim0("BOOT-TIME",get_boot_time));
  fd_idefn((lispval)server_env,fd_make_cprim0("UPTIME",get_uptime));
  fd_idefn((lispval)server_env,
           fd_make_cprim0("SERVLET-STATUS->STRING",servlet_status_string));
  fd_idefn((lispval)server_env,
           fd_make_cprim0("SERVLET-STATUS",servlet_status));

  fd_set_app_env(server_env);

  /* We keep a lock on the log, which could become a bottleneck if there are I/O problems.
     An alternative would be to log to a data structure and have a separate thread writing
     to the log.  Of course, if we have problems writing to the log, we probably have all sorts
     of other problems too! */
  u8_init_mutex(&log_lock);

  u8_log(LOG_NOTICE,Startup,"FDServlet %s",socket_spec);

  if (!(FD_VOIDP(default_notfoundpage)))
    u8_log(LOG_NOTICE,"SetPageNotFound","Handler=%q",default_notfoundpage);
  if (!socket_spec) {
    u8_log(LOG_CRIT,"USAGE","fdservlet <socket> [config]*");
    fprintf(stderr,"Usage: fdservlet <socket> [config]*\n");
    exit(1);}

  fd_setapp(socket_spec,NULL);

  fd_boot_message();
  u8_now(&boot_time);

  if (!(server_id)) {
    u8_uuid tmp = u8_getuuid(NULL);
    server_id = u8_uuidstring(tmp,NULL);}

  pid_file = fd_runbase_filename(".pid");
  cmd_file = fd_runbase_filename(".cmd");
  inject_file = fd_runbase_filename(".inj");

  write_cmd_file(cmd_file,"ServletInvocation",argc,argv);

  if (!(load_source)) {}
  else if ((u8_has_suffix(load_source,".scm",1))||
           (u8_has_suffix(load_source,".fdcgi",1))||
           (u8_has_suffix(load_source,".fdxml",1))) {
    lispval path = fdstring(load_source);
    lispval result = getcontent(path);
    fd_decref(path); fd_decref(result);}
  else {}

  u8_use_syslog(0);

  return run_servlet(socket_spec);
}

static int run_servlet(u8_string socket_spec)
{

#ifdef SIGHUP
  sigaction(SIGHUP,&sigaction_shutdown,NULL);
#endif

  setup_status_file();

  if ((strchr(socket_spec,'/'))&&
      (u8_file_existsp(socket_spec))&&
      (!((stealsockets)||(getenv("FD_STEALSOCKETS"))))) {
    u8_log(LOG_CRIT,"FDServlet/launch",
           "Socket file %s already exists!",socket_spec);
    exit(1);}

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
     U8_SERVER_TIMEOUT,poll_timeout,
     U8_SERVER_MAX_QUEUE,max_queue,
     U8_SERVER_MAX_CLIENTS,max_conn,
     U8_SERVER_END_INIT);

  fdwebserver.xserverfn = server_loopfn;
  fdwebserver.xclientfn = client_loopfn;

  /* Now that we're running, shutdowns occur normally. */
  init_webcommon_finalize();

  update_preloads();

  if (start_servers()<=0) {
    u8_log(LOG_CRIT,ServletAbort,"Startup failed");
    exit(1);}

  u8_log(LOG_INFO,ServletStartup,
         "FramerD (%s) FDServlet running, %d/%d pools/indexes",
         FRAMERD_REVISION,fd_n_pools,
         fd_n_primary_indexes+fd_n_secondary_indexes);
  u8_message("beingmeta FramerD, (C) beingmeta 2004-2019, all rights reserved");
  if (fdwebserver.n_servers>0) {
    u8_log(LOG_WARN,ServletStartup,"Listening on %d addresses",
           fdwebserver.n_servers);
    write_pid_file();
    u8_server_loop(&fdwebserver);}
  else {
    u8_log(LOG_CRIT,NoServers,"No servers configured, exiting...");
    exit(-1);
    return -1;}

  u8_message("FDServlet, normal exit of u8_server_loop()");

  shutdown_servlet(shutdown_reason);

  exit(0);

  return 0;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
