/* C Mode */

/* fdmananger: a program for running a set of inferior fdservers */
    
static char versionid[] =
  "$Id$";

#include "fdb/dtype.h"

#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>

#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <syslog.h>
#include <signal.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <pwd.h>
#include <grp.h>
#include <stdio.h>
#include <ctype.h>

#define NUL '\0'

#ifndef FD_DBSERVER
#define FD_DBSERVER "/usr/bin/fdbserver"
#endif

#ifndef O_SYNC
#ifdef O_FSYNC
#define O_SYNC O_FSYNC
#endif
#endif

#define STRPTR(x) ((x == NULL) ? "??" : (x))


static char *usage=_("fdmanager <control file> [pid file] [status file]");
static char *give_up_message=
  _("Gave up after %d seconds for fdserver %s (pid=%d) to start (errno=%d)");
static char *server_start_message=
  _("Started fdserver %s (pid=%d,nid=%s) in %d seconds");
static char *failed_termination_message=
 _("Server %s (pid=%d,nid=%s) ignored %s for %d seconds; trying %s");

struct SERVER_ENTRY {
  char *control_file, *dirname, *basename, *nid, *serverexe;
  long int wait, dependent, sleeping;
  char **argv;
  pid_t pid;};

static struct SERVER_ENTRY *servers;
static int n_servers=0, max_servers, terminating=0;
static char *fdserver, *status_file, *pid_file;

static fd_exception SecurityAbort=_("Security abort");
static fd_exception SecurityEvent=_("Security event");

static uid_t runas_uid=(uid_t)-1;
static gid_t runas_gid=(gid_t)-1;

/* Utilities */

static char *get_basename(char *string)
{
  char *copy=u8_strdup(string);
  int len=strlen(string); char *scan=copy+len;
  while ((scan>copy) && (*scan != '.')) scan--;
  if (scan>copy) *scan=NUL;
  return copy;
}

static char *path_append(char *base,char *suffix)
{
  char *result=u8_malloc(strlen(base)+strlen(suffix)+1);
  strcpy(result,base); strcat(result,suffix);
  return result;
}

int empty_linep(char *s)
{
  while ((*s) && (!(isprint(*s)))) s++;
  if (*s == NUL) return 1; else return 0;
}

static char *read_file(char *path)
{
  FILE *f=fopen(path,"r"); char buf[256], *scan, *result; int len;
  /* fprintf(stderr,"Opened path %s: 0x%lx\n",path,f); */
  result=fgets(buf,256,f); fclose(f);
  if (result) {
    int len=strlen(result); char *scan=result+len-1; 
    while (scan > buf)
      if (*scan == '\n') {*scan=NUL; break;}
    return u8_strdup(result);}
  else return NULL;
}

static char *skip_whitespace(char *string)
{
  while ((*string) && (isspace(*string))) string++;
  return string;
}

static char *skip_arg(char *string)
{
  while ((*string) && (!(isspace(*string))))
    if (*string == '"') {
      while ((*string) && (*string != '"')) string++;
      if (*string) return string+1; return NULL;}
    else if (*string == '\\') string=string+2;
    else string++;
  return string;
}

static int setup_runas()
{
  uid_t uid=getuid();
  gid_t gid=getgid();
  struct group *gentry=NULL;
  if ((uid<0) || (gid<0)) {
    u8_warn(SecurityAbort,"Couldn't determine user or group");
    exit(2);}
  if (getenv("FDAEMON_GROUP"))
    gentry=getgrnam(getenv("FDAEMON_GROUP"));
  if (gentry==NULL) gentry=getgrnam("fdaemon");
  if (gentry==NULL) gentry=getgrnam("daemon");
  if (gentry==NULL) gentry=getgrnam("nogroup");
  if (gentry) {
    u8_warn(SecurityEvent,"Servers will run as group '%s' (%d)",
	    gentry->gr_name,gentry->gr_gid);
    runas_gid=gentry->gr_gid;}
  if (uid==0) {
    /* When running as root, change your uid and possibly group */
    struct passwd *uentry=NULL; 
    if (getenv("FDAEMON_USER"))
      uentry=getpwnam(getenv("FDAEMON_USER"));
    if (uentry==NULL) uentry=getpwnam("fdaemon");
    if (uentry==NULL) uentry=getpwnam("daemon");
    if (uentry==NULL) uentry=getpwnam("nobody");
    if ((uentry) && (uentry->pw_uid)) {
      u8_warn(SecurityEvent,"Servers will run as user '%s' (%d)",
	      uentry->pw_name,uentry->pw_uid);
      runas_uid=uentry->pw_uid;}
    else {
      u8_warn(SecurityAbort,"Couldn't find non-root UID to use");
      exit(1);}}
}

static int become_runas()
{
  if (setgid(runas_gid)<0) {
    u8_graberr(-1,"server startup",NULL);
    u8_warn(SecurityAbort,"Couldn't change GID (%d)",runas_gid);
    exit(1);}
  if (runas_uid)
    if (setuid(runas_uid)<0) {
      u8_graberr(-1,"server startup",NULL);
      u8_warn(SecurityAbort,"Couldn't change UID to %d",runas_uid);
      exit(1);}
}


/* Creating server entries */

static char **generate_argv(char *exe,char *control_file,char *string)
{
  char **argvec=u8_malloc(sizeof(char *)*64), **result;
  int argc=2, max_args=16;
  char *start=skip_whitespace(string), *end=skip_arg(start);
  char *lim=start+strlen(start);
  argvec[0]=u8_strdup(exe);
  argvec[1]=u8_strdup(control_file);
  while (end>start) {
    if (argc+1 >= max_args) {
      argvec=u8_realloc(argvec,sizeof(char *)*(max_args+16));
      max_args=max_args+16;}
    *end=NUL; argvec[argc++]=u8_strdup(start);
    start=skip_whitespace(end+1);
    end=skip_arg(start);}
  argvec[argc]=NULL;
  return argvec;
}

static char *init_server_entry(struct SERVER_ENTRY *e,char *control_line)
{
  char *scan, *base, *pid_file, *nid_file, wait=10;
  if (strncmp(control_line,"dependent ",10) == 0) {
    e->dependent=1; control_line=control_line+10;}
  else e->dependent=0;
  if (strstr(control_line,"wait=")) {
    char *waitv=strstr(control_line,"wait=")+5;
    sscanf(waitv,"%d ",&(e->wait));
    control_line=skip_arg(waitv);}
  else e->wait=10;
  if (strstr(control_line,"executable=")) {
    int len;
    char *execval=strstr(control_line,"executable=")+11;
    control_line=skip_arg(execval); len=control_line-execval;
    e->serverexe=u8_malloc(1+len);
    strncpy(e->serverexe,execval,len); e->serverexe[len]='\0';}
  else e->serverexe=u8_strdup(fdserver);
  e->sleeping=0;
  /* Make a copy of the command line */
  control_line=skip_whitespace(control_line);
  e->control_file=u8_strdup(control_line);
  /* Terminate the control file after the space */
  scan=e->control_file;
  while ((*scan) && (!(isspace(*scan)))) scan++; *scan++=NUL;
  if (!(((e->control_file[0] == '/') || (e->control_file[0] == '\\')) &&
	(u8_file_existsp(e->control_file))) ) {
    /* The control file isn't valid, so we report that, clean up, and exit. */
    syslog(LOG_CRIT,_("The file %s is not a valid server control file: %s"),
	   e->control_file,control_line);
    u8_free(e->control_file); return NULL;}
  /* Generate the arg vector */
  e->argv=generate_argv(e->serverexe,e->control_file,scan);
  /* Generate the dirname */
  e->dirname=u8_dirname(e->control_file); 
  /* Generate the  basename */
  base=e->basename=get_basename(e->control_file); 
  return base;
}


/* Starting and restarting servers */

static void set_stdio(char *base)
{
  int log_fd, err_fd; mode_t omode=umask(0x0);
  log_fd=open(path_append(base,".log"),
	      (O_CREAT|O_WRONLY|O_APPEND|O_SYNC),
	      (S_IWUSR|S_IRUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH));
  if (log_fd < 0)
    syslog(LOG_ERR,_("Can't open %s (errno=%d/%s)"),
	   path_append(base,".log"),errno,strerror(errno));
  else if (dup2(log_fd,1) < 0)
	 syslog(LOG_ERR,_("dup2 for stdout to %s failed (errno=%d/%s)"),
		path_append(base,".log"),errno,strerror(errno));
  err_fd=open(path_append(base,".err"),
	      (O_CREAT|O_WRONLY|O_APPEND|O_SYNC),
	      (S_IWUSR|S_IRUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH));
  if (err_fd < 0)
    syslog(LOG_ERR,_("Can't open %s (errno=%d/%s)"),
	   path_append(base,".err"),errno,strerror(errno));
  else if (dup2(err_fd,2) < 0)
    syslog(LOG_ERR,_("dup2 for stderr to %s failed (errno=%d/%s)"),
	   path_append(base,".err"),errno,strerror(errno));
  umask(omode);
}

pid_t start_fdserver(struct SERVER_ENTRY *e,char *control_line)
{
  int proc, ppid=getpid();
  char *base, *pid_file, *ppid_file, *nid_file, *sleep_file;
  base=init_server_entry(e,control_line);
  /* If the entry is invalid, return -1 */
  if (base == NULL) return -1;
  /* Clean up any old state variables */
  ppid_file=path_append(base,".ppid"); remove(ppid_file);
  pid_file=path_append(base,".pid"); remove(pid_file);
  nid_file=path_append(base,".nid"); remove(nid_file);
  sleep_file=path_append(base,".sleep"); remove(sleep_file);
  errno=0;
  if (proc=fork()) {
    struct stat sbuf; int i=0, lim=e->wait, ret; e->pid=proc; 
    /* Wait for the nid file to be created */
    while ((i<lim) && (!(u8_file_existsp(nid_file)))) {sleep(1); i++;}
    if (!(u8_file_existsp(nid_file))) {
      syslog(LOG_CRIT,give_up_message,lim,e->control_file,e->pid,errno);
      errno=0; e->nid=NULL;}
    else {
      e->nid=read_file(nid_file);
      syslog(LOG_NOTICE,
	     server_start_message,e->control_file,e->pid,STRPTR(e->nid),i);}
    u8_free(pid_file); u8_free(ppid_file);
    u8_free(nid_file); u8_free(sleep_file);}
  else {
    FILE *pid_stream=fopen(ppid_file,"w");
    fprintf(pid_stream,"%d",ppid); fclose(pid_stream);
    /* Redirect stdout and stderr */
    set_stdio(base);
    /* Go to the directory the file lives in */
    chdir(e->dirname);
    /* Become the server */
    u8_message("Forking %s for %s as %d",
	       ((e->serverexe) ? (e->serverexe) : (fdserver)),
	       e->control_file,
	       e->pid);
    become_runas();
    if (e->serverexe) execv(e->serverexe,e->argv);
    else if (fdserver) execv(fdserver,e->argv);
    else execvp("fdserver",e->argv);}
  return proc;
}

pid_t restart_fdserver(struct SERVER_ENTRY *e)
{
  pid_t proc;
  char *base=e->basename, *pid_file;
  char *ppid_file, *nid_file, *sleep_file;
  int ppid=getpid();
  /* Clean up any old state variables */
  pid_file=path_append(base,".pid"); remove(pid_file); 
  ppid_file=path_append(base,".ppid"); remove(ppid_file); 
  nid_file=path_append(base,".nid"); remove(nid_file); 
  sleep_file=path_append(base,".sleep");
  if (u8_file_existsp(sleep_file)) e->sleeping=1;
  else e->sleeping=0;
  errno=0; /* Clear errno, just in case */
  if (proc=fork()) {
    int i=0, lim=e->wait, ret; e->pid=proc; 
    if (e->sleeping == 0) /* Wait for the nid file to be created */
      while ((i<lim) && (!(u8_file_existsp(nid_file)))) {sleep(1); i++;}
    else syslog(LOG_CRIT,"Server %s is sleeping",e->control_file);
    
    if (e->sleeping) {}
    else if (!(u8_file_existsp(nid_file))) {
      syslog(LOG_CRIT,give_up_message,lim,e->control_file,e->pid,errno);
      errno=0;}
    else {
      e->nid=read_file(nid_file);
      syslog(LOG_NOTICE,server_start_message,
	     e->control_file,e->pid,STRPTR(e->nid),i);}
    /* We no longer need these */
    u8_free(pid_file); u8_free(ppid_file);
    u8_free(nid_file); u8_free(sleep_file);}
  else if (e->sleeping) {
    char *argv[3];
    char *duration=read_file(sleep_file);
    FILE *pid_stream;
    if ((duration == NULL) || (!(isdigit(*duration))))
      duration="60";
    fprintf(stderr,"duration=%s\n",duration);
    /* Redirect stdout and stderr */
    set_stdio(base);
    fprintf(stderr,"stdio redirected\n");
    /* Go to the directory the file lives in */
    chdir(e->dirname);
    fprintf(stderr,"directory changed\n");
    /* Write your pid */
    pid_stream=fopen(pid_file,"w");
    fprintf(pid_stream,"%d",getpid());
    fclose(pid_stream);
    fprintf(stderr,"pid written\n");
    /* Write your ppid */
    pid_stream=fopen(ppid_file,"w");
    fprintf(pid_stream,"%d",ppid);
    fclose(pid_stream);
    fprintf(stderr,"ppid written\n");
    /* And go to sleep */
    argv[0]="sleep"; argv[1]=duration; argv[2]=NULL;
    fprintf(stderr,"exec'ing sleep\n");
    execvp("sleep",argv);}
  else {
    FILE *pid_stream=fopen(ppid_file,"w");
    /* Write your ppid */
    fprintf(pid_stream,"%d",ppid); fclose(pid_stream);
    /* Redirect stdout and stderr */
    set_stdio(base);
    /* Go to the directory the file lives in */
    chdir(e->dirname);
    /* Become the server */
    become_runas();
    execv(fdserver,e->argv);}
  return proc;
}

/* Specific utility functions */

static void write_pid(char *file)
{
  FILE *f=fopen(file,"w");
  if (f == NULL) {
    syslog(LOG_EMERG,_("The pid file %s cannot be opened for writing"),file);
    exit(1);}
  fprintf(f,"%d",getpid());
  fclose(f);
}

static void terminate_children(int signo)
{
  int i=0;
  terminating=1;
  syslog(LOG_CRIT,"Terminating all children because of signal %d",signo);
  while (i < n_servers) {
    int status, count=0, lim=10, give_up=0;
    if ((kill(servers[i].pid,SIGTERM) < 0) && (errno == ESRCH))
      errno=0;
    else while ((count<lim) && (waitpid(servers[i].pid,&status,WNOHANG) < 0)) {
      count++; sleep(1);}
    if (count>=lim) {
      syslog(LOG_ERR,failed_termination_message,
	     servers[i].control_file,servers[i].pid,STRPTR(servers[i].nid),
	     "SIGTERM",lim,"SIGABRT");
      if ((kill(servers[i].pid,SIGABRT) < 0) &&  (errno == ESRCH)) errno=0;
      else {
	count=0; while ((count<lim) &&
			(waitpid(servers[i].pid,&status,WNOHANG) < 0)) {
	  count++; sleep(1);}
	if (count>=lim) {
	  syslog(LOG_ERR,failed_termination_message,
		 servers[i].control_file,servers[i].pid,STRPTR(servers[i].nid),
		 "SIGABRT",lim,"SIGKILL");
	  kill(servers[i].pid,SIGKILL);}}}
    i++;}
}

static void setup_signals()
{
  signal(SIGTERM,terminate_children);
  signal(SIGHUP,terminate_children);
  signal(SIGQUIT,terminate_children);
  signal(SIGABRT,terminate_children);
  signal(SIGILL,terminate_children);
  signal(SIGFPE,terminate_children);
  signal(SIGSEGV,terminate_children);
  /* signal(SIGPIPE,terminate_children); */
  signal(SIGBUS,terminate_children);
}

static void update_status()
{
  FILE *f=fopen(status_file,"w");
  int i=0; while (i < n_servers) {
    char **argv=servers[i].argv;
    fprintf(f,"%d\t%d\t%s\t%s",i,servers[i].pid,
	    STRPTR(servers[i].nid),servers[i].control_file);
    while (*argv) fprintf(f," %s",*(argv++));
    if (servers[i].sleeping) fprintf(f,"\tsleeping");
    fprintf(f,"\n"); i++;}
  fclose(f);
}

static int update_nids()
{
  int i=0, n_started=0; while (i < n_servers)
    if (servers[i].nid) {i++; n_started++;}
  else  {
    char *nid_file=path_append(servers[i].basename,".nid");
    if (u8_file_existsp(nid_file)) {
      servers[i].nid=read_file(nid_file); n_started++;
      syslog(LOG_NOTICE,
	     server_start_message,servers[i].control_file,
	     servers[i].pid,STRPTR(servers[i].nid),i);}
    i++; u8_free(nid_file);}
  return n_started;
}

static void remove_status_files()
{
  remove(status_file);
  remove(pid_file);
}

static void timer_proc(int signo)
{
  update_nids();
  update_status();
  alarm(60);
}

static int dont_be_root()
{
  uid_t uid=getuid();
  gid_t gid=getgid();
  if ((uid<0) || (gid<0)) {
    u8_warn(SecurityAbort,"Couldn't determine user or group");
    exit(2);}
  if (uid==0) {
    /* When running as root, change your uid and possibly group */
    struct passwd *uentry=NULL; struct group *gentry=NULL;
    if (getenv("FDAEMON_USER"))
      uentry=getpwnam(getenv("FDAEMON_USER"));
    if (uentry==NULL) uentry=getpwnam("fdaemon");
    if (uentry==NULL) uentry=getpwnam("daemon");
    if (uentry==NULL) uentry=getpwnam("nobody");
    if (getenv("FDAEMON_GROUP"))
      gentry=getgrnam(getenv("FDAEMON_GROUP"));
    if (gentry==NULL) gentry=getgrnam("fdaemon");
    if (gentry==NULL) gentry=getgrnam("daemon");
    if (gentry==NULL) gentry=getgrnam("nogroup");
    if (gentry)
      if (setgid(gentry->gr_gid)<0) {
	u8_graberr(-1,"server startup",NULL);
	u8_warn(SecurityAbort,"Couldn't set GID to %d (%s)",
		gentry->gr_gid,gentry->gr_name);
	exit(1);}
      else u8_warn(SecurityEvent,"Changed GID to %d (%s)",
		   gentry->gr_gid,gentry->gr_name);
    else {
      u8_warn(SecurityAbort,"Couldn't find non-root GID to use");
      exit(1);}
    if (uentry)
      if (setuid(uentry->pw_uid)<0) {
	u8_graberr(-1,"server startup",NULL);
	u8_warn(SecurityAbort,"Couldn't set UID to %d (%s)",
		uentry->pw_uid,uentry->pw_name);
	exit(1);}
      else u8_warn(SecurityEvent,"Changed uid to %d (%s)",
		   uentry->pw_uid,uentry->pw_name);
    else {
      u8_warn(SecurityAbort,"Couldn't find non-root UID to use");
      exit(1);}}
  else {
    struct passwd *uentry=getpwuid(uid);
    struct group *gentry=getgrgid(gid);
    if ((uentry==NULL) || (gentry==NULL)) {
      u8_warn(SecurityAbort,"Couldn't determine user or group");
      exit(2);}
    u8_warn(SecurityEvent,"Running as user %s (%d) and group %s (%d)",
	    uentry->pw_name,uentry->pw_uid,gentry->gr_name,gentry->gr_gid);}
}

int main(int argc,char *argv[])
{
  FILE *control_file;
  char control_line[512];
  int i=0, status, pid, n_started=0;
  fdserver=getenv("FDSERVER");
  if (fdserver==NULL) fdserver=FD_DBSERVER;
  if ((argc < 2) || (argc > 4)) {
    fprintf(stderr,_("Usage: %s\n"),usage); exit(1);}
  setup_runas();
#if defined(LOG_PERROR)
  openlog("fdmanager",(LOG_CONS|LOG_PERROR|LOG_PID),LOG_DAEMON);
#else
  openlog("fdmanager",(LOG_CONS|LOG_PID),LOG_DAEMON);
#endif
  if (argc > 2) pid_file=u8_strdup(argv[2]);
  else pid_file=path_append(argv[1],".pid");
  if (argc == 4) status_file=u8_strdup(argv[3]);
  else status_file=path_append(argv[1],".status");
  /* Could be left over */
  remove(status_file); errno=0;
  /* This is the new value */
  write_pid(pid_file); errno=0;
  /* Now, stop being root */
  /* Open the control file */
  control_file=fopen(argv[1],"r");
  if (control_file == NULL) {
    syslog(LOG_EMERG,
	   _("the master list of control files (%s) cannot be opened\n"),
	   argv[1]);
    exit(1);}
  servers=u8_malloc(sizeof(struct SERVER_ENTRY)*64); max_servers=64;
  setup_signals();
  while (fgets(control_line,512,control_file)) {
    int interval=0;
    if (empty_linep(control_line)) continue;
    if (control_line[0] == '#') continue;
    if (control_line[0] == ';') continue;
    if (sscanf(control_line,"sleep %d",&interval)) {
      sleep(interval); continue;}
    /* Grow the table if neccessary */
    if (n_servers+1 == max_servers) {
      servers=u8_realloc(servers,sizeof(char *)*(max_servers+64));
      max_servers=max_servers+64;}
    /* (Try to) Start the server */
    if (start_fdserver(&servers[n_servers],control_line) > 0) {
      if (servers[n_servers].nid) n_started++;
      n_servers++;}}
  /* Done with the control file */
  fclose(control_file);
  /* Get any servers which have started since we gave up on them */
  n_started=update_nids();
  /* Update the status file and prepare for its removal on exit */
  update_status(); atexit(remove_status_files);
  /* Now wait for your children to die (sigh) */
  syslog(LOG_INFO,_("%d servers started; %d still starting; waiting for any problems"),
	 n_started,n_servers-n_started);
  signal(SIGALRM,timer_proc); alarm(60);
  while ((pid=wait(&status))>0) {
    /* Find the server which died */
    int j=0; while (j < n_servers)
      if (servers[j].pid == pid) break; else j++;
    if (j >= n_servers) {
      syslog(LOG_EMERG,
	     _("where did this child come from?! (pid=%d,status=%d)"),
	     pid,status);
      exit(1);}
    syslog(LOG_ERR,_("FDServer %s (pid=%d,nid=%s) exited with status %d"),
	   servers[j].control_file,servers[j].pid,
	   STRPTR(servers[j].nid),status);
    sleep(6); /* This should let its port get freed up. */
    if (terminating == 0) {
      servers[j].pid=restart_fdserver(&(servers[j]));
      if ((j<n_servers) && (servers[j+1].dependent))
	kill(servers[j+1].pid,SIGTERM);}
    update_status();}
  syslog(LOG_NOTICE,"Exiting fdmanager normally");
  return 0;
}


/* File specific stuff */

/* The CVS log for this file
   $Log: fdmanager.c,v $
   Revision 1.1  2006/01/30 16:31:18  haase
   Started adding fdmanager to FDB


*/

/* Emacs local variables
;;;  Local variables: ***
;;;  compile-command: "cd ../..; make" ***
;;;  End: ***
*/
