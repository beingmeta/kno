/* C Mode */

/* mod_fdserv.c
   Copyright (C) 2002-2005 beingmeta, inc.  All Rights Reserved
   This is a Apache module supporting persistent FramerD servers

   For Apache 1.3:
     Compile with: apxs -c mod_fdserv.c
     Install with: apxs -i mod_fdserv.so
   For Apache 2.0:
     Compile with: apxs2 -c mod_fdserv.c
     Install with: apxs2 -i mod_fdserv.so

*/

#include "httpd.h"
#include "http_config.h"
#include "http_core.h"
#include "http_log.h"
#include "http_main.h"
#include "http_protocol.h"
#include "http_request.h"

#ifdef STANDARD20_MODULE_STUFF
#define APACHE20 1
#define APACHE13 0
#else
#define APACHE20 0
#define APACHE13 1
#endif

#include "util_script.h"
#if APACHE13
#include "multithread.h"
#elif APACHE20
#include "apr.h"
#include "apr_strings.h"
#endif

#include "sys/un.h"
#include "sys/stat.h"
#include "sys/types.h"
#include "unistd.h"
#include "fdb/config.h"

#ifdef HAVE_FTIME
#define TRACK_EXECUTION_TIMES 1
#else
#define TRACK_EXECUTION_TIMES 0
#endif

#if TRACK_EXECUTION_TIMES
#include "sys/timeb.h"
#endif

#define FDSERV_MAGIC_TYPE "application/x-httpd-fdserv"

/* Compatibility */

#if APACHE13
typedef pool apr_pool_t;
#define apr_palloc ap_palloc
#define apr_pstrdup ap_pstrdup
#define apr_pstrcat ap_pstrcat
#define apr_table_make ap_make_table
#define apr_table_set ap_table_set
#define apr_table_get ap_table_get
#define apr_table_elts ap_table_elts
#define apr_make_table ap_make_table
typedef table apr_table_t;
typedef array_header apr_array_header_t;
typedef table_entry apr_table_entry_t;
#define apr_is_directory ap_is_directory
#define AP_MODULE_DECLARE_DATA MODULE_VAR_EXPORT    
#define AP_INIT_TAKE1(cmd_name,handler,x,y,doc) {cmd_name,handler,x,y,TAKE1,doc}
#define AP_INIT_TAKE2(cmd_name,handler,x,y,doc) {cmd_name,handler,x,y,TAKE2,doc}
#define APLOG_HEAD APLOG_MARK,APLOG_DEBUG
static int executable_filep(apr_pool_t *p,const char *filename)
{
  struct stat finfo; 
  return ((stat(filename,&finfo) == 0) && (ap_can_exec(&finfo)));
}
#define apr_pool_create(loc,p) *loc=ap_make_sub_pool(p)
#else
#define APLOG_HEAD APLOG_MARK,APLOG_DEBUG,OK
#define ap_can_exec(file) (1)
#define ap_send_http_header(r) ;
#define ap_reset_timeout(r) ;
#define apr_is_directory(s) (0)
static int executable_filep(apr_pool_t *p,const char *filename)
{
  apr_finfo_t finfo;
  apr_uid_t uid; apr_gid_t gid;
  apr_uid_current(&uid,&gid,p);
  apr_stat(&finfo,filename,(APR_FINFO_USER|APR_FINFO_PROT),p);
  if (uid == finfo.user)
    if (finfo.protection & APR_UEXECUTE) return 1;
  if (uid == finfo.group)
    if (finfo.protection & APR_GEXECUTE) return 1;
  if (finfo.protection & APR_WEXECUTE) return 1;
  else return 0;
}
#endif

static char *prealloc(apr_pool_t *p,char *ptr,int new_size,int old_size)
{
  char *newptr=apr_palloc(p,new_size);
  if (newptr != ptr) memmove(newptr,ptr,old_size);
  return newptr;
}

module AP_MODULE_DECLARE_DATA fdserv_module;

/* Checking writability of files */

#if APACHE13
static int can_writep(apr_pool_t *p,server_rec *s,struct stat *finfo)
{
#if defined(OS2) || defined(WIN32) || defined(NETWARE)
    /* OS/2 dosen't have Users and Groups */
    return 1;
    if (s->server_uid == finfo->st_uid)
      if (finfo->st_mode & S_IWUSR) return 1;
    if (s->server_gid == finfo->st_gid)
      if (finfo->st_mode & S_IWGRP) return 1;
#endif
    return ((finfo->st_mode & S_IWOTH) != 0);
}
#elif APACHE20
static int can_writep(apr_pool_t *p,server_rec *s,apr_finfo_t *finfo)
{
  apr_uid_t uid; apr_gid_t gid;
  apr_uid_current(&uid,&gid,p);
  if (uid == finfo->user)
    if (finfo->protection & APR_UWRITE) return 1;
  if (uid == finfo->group)
    if (finfo->protection & APR_GWRITE) return 1;
  if (finfo->protection & APR_WWRITE) return 1;
  else return 0;
}
#else
static int can_writep(apr_pool_t *p,server_rec *s,struct stat *finfo)
{
  return 1;
}
#endif

#if APACHE20
#define FINFO_FLAGS \
   (APR_FINFO_USER|APR_FINFO_GROUP|APR_FINFO_PROT|APR_FINFO_TYPE)
static int file_writablep(apr_pool_t *p,server_rec *s,const char *filename)
{
  apr_finfo_t finfo;
  apr_stat(&finfo,filename,FINFO_FLAGS,p);
  if (can_writep(p,s,&finfo)) return 1;
  else {
    char buf[PATH_MAX]; int scan=strlen(filename)-1;
    strcpy(buf,filename); while ((scan>0) && (buf[scan] != '/')) scan--;
    if (scan>0) buf[scan]='\0';
    apr_stat(&finfo,filename,FINFO_FLAGS,p);
    if ((finfo.filetype == APR_DIR) && (can_writep(p,s,&finfo))) return 1;
    return 0;}
}
#else
static int file_writablep(apr_pool_t *p,server_rec *s,const char *filename)
{
  char buf[PATH_MAX]; struct stat finfo; int scan=strlen(filename)-1;
  strcpy(buf,filename);
  while ((scan>0) && (buf[scan] != '/')) scan--;
  if (scan>0) buf[scan]='\0';
  return ((stat(buf,&finfo) == 0) && (S_ISDIR(finfo.st_mode)) &&
	  (can_writep(p,s,&finfo)));
}
#endif

/* Configuration handling */

static apr_table_t *socketname_table;

struct FDSERV_SERVER_CONFIG {
  const char *server_executable;
  const char **config_args;
  const char *socket_prefix;
  uid_t uid; gid_t gid;};

struct FDSERV_DIR_CONFIG {
  const char *server_executable;
  const char **config_args;
  const char *log_file;
  const char *socket_file;
  const char *socket_prefix;};

static void *create_server_config(apr_pool_t *p,server_rec *s)
{
  struct FDSERV_SERVER_CONFIG *config=
    apr_palloc(p,sizeof(struct FDSERV_SERVER_CONFIG));
  config->server_executable=NULL;
  config->config_args=NULL;
  config->socket_prefix=NULL;
  config->uid=-1; config->gid=-1;
  return (void *) config;
}

static void *merge_server_config(apr_pool_t *p,void *base,void *new)
{
  struct FDSERV_SERVER_CONFIG *config=
    apr_palloc(p,sizeof(struct FDSERV_SERVER_CONFIG));
  struct FDSERV_SERVER_CONFIG *parent=base;
  struct FDSERV_SERVER_CONFIG *child=new;

  if (child->uid <= 0) config->uid=parent->uid; else config->uid=child->uid;
  if (child->gid <= 0) config->gid=parent->gid; else config->gid=child->gid;

  if (child->server_executable)
    config->server_executable=apr_pstrdup(p,child->server_executable);
  else if (parent->server_executable)
    config->server_executable=apr_pstrdup(p,parent->server_executable);
  else config->server_executable=NULL;
  
  if (child->config_args) {
    const char **scan=child->config_args;
    char **fresh, **write;
    int n_config=0;
    while (*scan) {n_config++; scan++;}
    fresh=apr_palloc(p,(n_config+1)*sizeof(char *));
    scan=child->config_args; write=fresh;
    while (*scan) {
      *write=apr_pstrdup(p,*scan); write++; scan++;}
    *write=NULL;
    config->config_args=(const char **)fresh;}
  else config->config_args=NULL;

  if (child->socket_prefix)
    config->socket_prefix=apr_pstrdup(p,child->socket_prefix);
  else if (parent->socket_prefix)
    config->socket_prefix=apr_pstrdup(p,parent->socket_prefix);
  else config->socket_prefix=NULL;

  if (child->socket_prefix)
    config->socket_prefix=apr_pstrdup(p,child->socket_prefix);
  else if (parent->socket_prefix)
    config->socket_prefix=apr_pstrdup(p,parent->socket_prefix);
  else config->socket_prefix=NULL;

  return (void *) config;
}

static void *create_dir_config(apr_pool_t *p,char *dir)
{
  struct FDSERV_DIR_CONFIG *config=
    apr_palloc(p,sizeof(struct FDSERV_DIR_CONFIG));
  config->server_executable=NULL;
  config->config_args=NULL;
  config->log_file=NULL;
  config->socket_file=NULL;
  config->socket_prefix=NULL;
  return (void *) config;
}

static void *merge_dir_config(apr_pool_t *p,void *base,void *new)
{
  struct FDSERV_DIR_CONFIG *config
    =apr_palloc(p,sizeof(struct FDSERV_DIR_CONFIG));
  struct FDSERV_DIR_CONFIG *parent=base;
  struct FDSERV_DIR_CONFIG *child=new;

  config->log_file=NULL; config->socket_file=NULL;

  if (child->server_executable)
    config->server_executable=apr_pstrdup(p,child->server_executable);
  else if (parent->server_executable)
    config->server_executable=apr_pstrdup(p,parent->server_executable);
  else config->server_executable=NULL;

  if (child->config_args) {
    const char **scan=child->config_args;
    char **fresh, **write;
    int n_config=0;
    while (*scan) {n_config++; scan++;}
    fresh=apr_palloc(p,(n_config+1)*sizeof(char *));
    scan=child->config_args; write=fresh;
    while (*scan) {
      *write=apr_pstrdup(p,*scan); write++; scan++;}
    *write=NULL;
    config->config_args=(const char **)fresh;}
  else config->config_args=NULL;

  if (child->socket_prefix)
    config->socket_prefix=apr_pstrdup(p,child->socket_prefix);
  else if (parent->socket_prefix)
    config->socket_prefix=apr_pstrdup(p,parent->socket_prefix);
  else config->socket_prefix=NULL;

  if (child->socket_file)
    config->socket_file=apr_pstrdup(p,child->socket_file);
  else if (parent->socket_file)
    config->socket_file=apr_pstrdup(p,parent->socket_file);
  else config->socket_file=NULL;

  if (child->log_file)
    config->log_file=apr_pstrdup(p,child->log_file);
  else if (parent->log_file)
    config->log_file=apr_pstrdup(p,parent->log_file);
  else config->log_file=NULL;

  return (void *) config;
}

/* Configuration directives */

static const char *servlet_executable
   (cmd_parms *parms,void *mconfig,const char *arg)
{
  struct FDSERV_SERVER_CONFIG *sconfig=
    ap_get_module_config(parms->server->module_config,&fdserv_module);
  struct FDSERV_DIR_CONFIG *dconfig=mconfig;
  if (executable_filep(parms->pool,arg))
    if (parms->path) {
      dconfig->server_executable=arg;
      return NULL;}
    else {
      sconfig->server_executable=arg;
      return NULL;}
  else return "server executable is not executable";
}

static char **extend_config(apr_pool_t *p,char **config_args,const char *var,const char *val)
{
  char *config_arg=apr_pstrcat(p,var,"=",val,NULL);
  if (config_args==NULL) {
    char **vec=apr_palloc(p,2*sizeof(char *));
    vec[0]=config_arg;
    return vec;}
  else {
    char **scan=config_args, **grown; int n_configs=0;
    while (*scan) {scan++; n_configs++;}
    grown=(char **)prealloc(p,(char *)config_args,
			    (n_configs+2)*sizeof(char *),
			    (n_configs+1)*sizeof(char *));
    grown[n_configs]=config_arg; grown[n_configs+1]=NULL;
    return grown;}
}

static const char *servlet_config
  (cmd_parms *parms,void *mconfig,const char *arg1,const char *arg2)
{
  struct FDSERV_DIR_CONFIG *dconfig=mconfig;
  struct FDSERV_SERVER_CONFIG *sconfig=
    ap_get_module_config(parms->server->module_config,&fdserv_module);
  apr_pool_t *p=parms->pool;
  if (parms->path) {
    dconfig->config_args=
      (const char **)extend_config(p,(char **)dconfig->config_args,arg1,arg2);
    return NULL;}
  else {
    sconfig->config_args=
      (const char **)extend_config(p,(char **)sconfig->config_args,arg1,arg2);
    return NULL;}
}

#if APACHE13
static const char *servlet_user(cmd_parms *parms,void *mconfig,const char *arg)
{
  struct FDSERV_SERVER_CONFIG *sconfig=
    ap_get_module_config(parms->server->module_config,&fdserv_module);
  if ((sconfig->uid=ap_uname2id(arg)) >= 0) return NULL;
  else return "invalid user id";
}

static const char *servlet_group(cmd_parms *parms,void *mconfig,const char *arg)
{
  struct FDSERV_SERVER_CONFIG *sconfig=
    ap_get_module_config(parms->server->module_config,&fdserv_module);
  if ((sconfig->gid=ap_gname2id(arg)) >= 0) return NULL;
  else return "invalid group id";
}
#endif

static const char *socket_prefix(cmd_parms *parms,void *mconfig,const char *arg)
{
  struct FDSERV_DIR_CONFIG *dconfig=mconfig;
  struct FDSERV_SERVER_CONFIG *sconfig=
    ap_get_module_config(parms->server->module_config,&fdserv_module);
  if (file_writablep(parms->pool,parms->server,arg))
    if (parms->path) {
      dconfig->socket_prefix=arg;
      return NULL;}
    else {
      sconfig->socket_prefix=arg;
      return NULL;}
  else return "Invalid socket prefix (directory does not exist or is not writable)";
}

static const char *socket_file(cmd_parms *parms,void *mconfig,const char *arg)
{
  struct FDSERV_DIR_CONFIG *dconfig=mconfig;
  dconfig->socket_file=arg;
  return NULL;
}

static const char *log_file(cmd_parms *parms,void *mconfig,const char *arg)
{
  struct FDSERV_DIR_CONFIG *dconfig=mconfig;
  if (file_writablep(parms->pool,parms->server,arg)) {
    dconfig->log_file=arg;
    return NULL;}
  else return "LogFile is not writable";
}

static const command_rec fdserv_cmds[] =
{
  AP_INIT_TAKE1("FDServletExecutable", servlet_executable, NULL, OR_ALL,
	       "the executable used to start a servlet"),
  AP_INIT_TAKE2("FDServletConfig", servlet_config, NULL, OR_ALL,
		"configuration parameters to the servlet"),
#if APACHE13
  AP_INIT_TAKE1("FDServletUser", servlet_user, NULL, RSRC_CONF,
	       "the user whom the fdservlet will run as"),
  AP_INIT_TAKE1("FDServletGroup", servlet_group, NULL, RSRC_CONF,
	       "the group whom the fdservlet will run as"),
#endif
  AP_INIT_TAKE1("FDServletPrefix", socket_prefix, NULL, OR_ALL,
	       "the prefix to be appended to socket names"),
  AP_INIT_TAKE1("FDServletSocket", socket_file, NULL, OR_ALL,
	       "the socket file to be used for a particular script"),
  AP_INIT_TAKE1("FDServletLog", log_file, NULL, OR_ALL,
	       "the logfile to be used for scripts"),
  {NULL}
};

/* Getting socket filenames */

static const char *get_sockname(request_rec *r,const char *spec)
{
  const char *cached=apr_table_get(socketname_table,spec);
  if (cached) return cached;
  else {
    struct FDSERV_SERVER_CONFIG *sconfig=
      ap_get_module_config(r->server->module_config,&fdserv_module);
    struct FDSERV_DIR_CONFIG *dconfig=
      ap_get_module_config(r->per_dir_config,&fdserv_module);
    if (dconfig->socket_file) {
      apr_table_set(socketname_table,spec,dconfig->socket_file);
      return (char *) dconfig->socket_file;}
    else {
      const char *prefix=
	((dconfig->socket_prefix) ? (dconfig->socket_prefix) :
	 (sconfig->socket_prefix) ? (sconfig->socket_prefix) : "/tmp/fdserv::");
      char buf[PATH_MAX], *write=buf, *read=(char *)spec;
      strcpy(buf,prefix); write=buf+strlen(prefix);
      while (*read)
	if ((*read == '/') || (*read == '\\')) {*write++=':'; read++;}
	else *write++=*read++;
      *write='\0';
      apr_table_set(socketname_table,spec,buf);
      return (char *) apr_table_get(socketname_table,spec);}}
}

/* Launching fdservlet processes */

static void spawn_fdservlet (request_rec *r,apr_pool_t *p,const char *sockname);

int connect_to_servlet(request_rec *r)
{
  int sock; char buf[512];
  sprintf(buf,"%s:%s",r->server->server_hostname,r->uri);
  const char *sockname=get_sockname(r,buf);
  struct sockaddr_un servname;

  ap_log_error
    (APLOG_HEAD,r->server,"Resolving request for %s through %s",
     r->unparsed_uri,sockname);

  sock=socket(PF_LOCAL,SOCK_STREAM,0);
  servname.sun_family=AF_LOCAL; strcpy(servname.sun_path,sockname);
  if ((connect(sock,(struct sockaddr *)&servname,SUN_LEN(&servname))) < 0) {
    spawn_fdservlet(r,r->pool,sockname);
    if ((connect(sock,(struct sockaddr *)&servname,SUN_LEN(&servname))) < 0)
      return -1;
    else return sock;}
  else return sock;  
}

#if APACHE13
struct FDSERV_INFO {
  char *exename, *filename, *sockname, *logfile;
  char **config_args;
  uid_t uid; gid_t gid;};

static int exec_fdserv(void *child_stuff, child_info *pinfo)
{
  char *argv[32], *envp[5], buf[32];
  struct FDSERV_INFO *info=(struct FDSERV_INFO *) child_stuff;
  char **scan_config=info->config_args; int n_configs=0;
  
  ap_cleanup_for_exec();
  ap_chdir_file(info->filename);
  if (info->gid>=0) setgid(info->gid);
  if (info->uid>=0) setuid(info->uid);
  if (info->logfile) {
    int log_fd=open(info->logfile,O_CREAT|O_APPEND);
    dup2(log_fd,1); dup2(log_fd,2);}
  argv[0]=info->exename;
  argv[1]=info->sockname;
  if (scan_config) {
    while (*scan_config) {
      if (n_configs>30) {/* blow up */}
      argv[2+n_configs]=*scan_config;
      scan_config++; n_configs++;}}
  argv[2+n_configs]=NULL;
  envp[0]=NULL;
  execve(info->exename,argv,envp);
  return -1; /* Should never return */
}

static void spawn_fdservlet
  (request_rec *r,apr_pool_t *p,const char *sockname) 
{
  struct FDSERV_SERVER_CONFIG *sconfig=
    ap_get_module_config(r->server->module_config,&fdserv_module);
  struct FDSERV_DIR_CONFIG *dconfig=
    ap_get_module_config(r->per_dir_config,&fdserv_module);
  struct FDSERV_INFO info;
  server_rec *s=r->server;
  struct stat stat_data;
  info.exename=(char *)
    ((dconfig->server_executable) ? (dconfig->server_executable) :
     (sconfig->server_executable) ? (sconfig->server_executable) :
     "/usr/bin/fdserv");
  info.filename=(char *)r->filename;
  info.sockname=(char *)sockname;
  info.logfile=(char *)(dconfig->log_file);
  if (sconfig->uid>=0) info.uid=sconfig->uid;
  else info.uid=r->server->server_uid;
  if (sconfig->gid>=0) info.gid=sconfig->gid;
  else info.gid=r->server->server_gid;
  
  if (stat(sockname,&stat_data) == 0) {
    if (remove(sockname) == 0)
      ap_log_error
	(APLOG_MARK,APLOG_NOTICE,s,"Removed leftover socket file %s",
	 sockname);
    else {
      ap_log_error
	(APLOG_MARK,APLOG_NOTICE,s,"Could not remove socket file %s",
	 sockname);}}
  
  errno=0;
  ap_log_error
    (APLOG_MARK,APLOG_NOTICE,s,
     "Spawning %s @%s for %s[pid=%d,uid=%d,gid=%d]",
     info.exename,sockname,r->unparsed_uri,getpid(),info.uid,info.gid);
  
  ap_bspawn_child(p,exec_fdserv,&info,0,NULL,NULL,NULL);

  /* Now wait for the file to exist */
  sleep(2); while (stat(sockname,&stat_data) < 0) {
    sleep(1); perror(sockname);}
}
#endif

#if APACHE20
static void spawn_fdservlet(request_rec *r,apr_pool_t *p,const char *sockname) 
{
  apr_proc_t pid; apr_procattr_t *attr;
  char *argv[32], *envp[5];
  struct stat stat_data; int rv, n_configs=0;

  server_rec *s=r->server;
  struct FDSERV_SERVER_CONFIG *sconfig=
    ap_get_module_config(r->server->module_config,&fdserv_module);
  struct FDSERV_DIR_CONFIG *dconfig=
    ap_get_module_config(r->per_dir_config,&fdserv_module);
  const char *exename=((dconfig->server_executable) ?
		       (dconfig->server_executable) :
		       (sconfig->server_executable) ?
		       (sconfig->server_executable) :
		       "/usr/bin/fdserv");
  const char **scan_config=((dconfig->config_args) ?
			    (dconfig->config_args) :
			    (sconfig->config_args) ?
			    (sconfig->config_args) :
			    (NULL));
  argv[0]=(char *)exename;
  argv[1]=(char *)sockname;
  if (scan_config) {
    while (*scan_config) {
      if (n_configs>30) {/* blow up */}
      argv[2+n_configs]=(char *)(*scan_config);
      scan_config++; n_configs++;}}
  argv[2+n_configs]=NULL;
  
  if (((rv=apr_procattr_create(&attr,p)) != APR_SUCCESS) ||
      ((rv=apr_procattr_cmdtype_set(attr,APR_PROGRAM)) != APR_SUCCESS) ||
      ((rv=apr_procattr_detach_set(attr,1)) != APR_SUCCESS) ||
      ((rv=apr_procattr_dir_set(attr,ap_make_dirstr_parent(p,r->filename))) != APR_SUCCESS))
    ap_log_rerror(APLOG_MARK, APLOG_ERR, rv, r,
		  "couldn't set child process attributes: %s", r->filename);
  if (dconfig->log_file) {
    int logfd=open(dconfig->log_file,(O_CREAT|O_APPEND),(S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP));
    if (logfd<0)
      ap_log_rerror(APLOG_MARK, APLOG_ERR, rv, r,"couldn't open log file %s",dconfig->log_file);
    else if ((rv=apr_procattr_io_set(attr,0,1,logfd)) != APR_SUCCESS)
      ap_log_rerror(APLOG_MARK, APLOG_ERR, rv, r,
		    "couldn't set child process i/o attributes: %s", r->filename);}
  else if ((rv=apr_procattr_io_set(attr,0,1,2)) != APR_SUCCESS)
    ap_log_rerror(APLOG_MARK, APLOG_ERR, rv, r,
		  "couldn't set child process i/o attributes: %s", r->filename);
  
  envp[0]=NULL;
  
  if (stat(sockname,&stat_data) == 0) {
    if (remove(sockname) == 0)
      ap_log_error
	(APLOG_MARK,APLOG_NOTICE,OK,s,"Removed leftover socket file %s",
	 sockname);
    else {
      ap_log_error
	(APLOG_MARK,APLOG_NOTICE,OK,s,"Could not remove socket file %s",
	 sockname);}}
  
  errno=0;
  rv=apr_proc_create(&pid,exename,(const char **)argv,(const char **)envp,attr,p);
  if (rv!=APR_SUCCESS) {
    ap_log_rerror(APLOG_MARK, APLOG_ERR, rv, r,
		  "Couldn't spawn %s @%s for %s [rv=%d,pid=%d]",
		  exename,sockname,r->unparsed_uri,rv,pid);
    return;}
  else ap_log_error
	 (APLOG_MARK,APLOG_NOTICE,rv,s,
	  "Spawned %s @%s for %s [rv=%d,pid=%d]",
	  exename,sockname,r->unparsed_uri,rv,pid);
  
  /* Now wait for the file to exist */
  sleep(2); while (stat(sockname,&stat_data) < 0) {
    sleep(5); perror(sockname);}
}
#endif

/* Writing DTYpes to BUFFs */

/* In Apache 2.0, BUFFs seem to be replaced by buckets in brigades, but that seems
    a little overhead heavy for the output buffers used here, which are just used to write
    slotmaps to servlets.  So a simple reimplementation happens. here.  */
#if (!(APACHE13))
typedef struct BUFF {
  apr_pool_t *p; unsigned char *buf, *ptr, *lim;} BUFF;
static void ap_bputc(unsigned char c,BUFF *b)
{
  if (b->ptr+1 >= b->lim) {
    int old_size=b->lim-b->buf, off=b->ptr-b->buf;
    unsigned char *nbuf=prealloc(b->p,b->buf,old_size*2,old_size);
    b->buf=nbuf; b->ptr=nbuf+off; b->lim=nbuf+old_size*2;}
  *(b->ptr++)=c;
}
static void ap_bputs(char *string,BUFF *b)
{
  int len=strlen(string);
  if (b->ptr+len+1 >= b->lim) {
    int old_size=b->lim-b->buf, off=b->ptr-b->buf, new_size=old_size, need_size=off+len+1;
    unsigned char *nbuf;
    while (new_size < need_size) new_size=new_size*2;
    nbuf=prealloc(b->p,b->buf,new_size,old_size);
    b->buf=nbuf; b->ptr=nbuf+off; b->lim=nbuf+new_size;}
  strcpy(b->ptr,string); b->ptr=b->ptr+len;
}
static void ap_bwrite(BUFF *b,char *string,int len)
{
  if (b->ptr+len+1 >= b->lim) {
    int old_size=b->lim-b->buf, off=b->ptr-b->buf, new_size=old_size, need_size=off+len+1;
    unsigned char *nbuf;
    while (new_size < need_size) new_size=new_size*2;
    nbuf=prealloc(b->p,b->buf,new_size,old_size);
    b->buf=nbuf; b->ptr=nbuf+off; b->lim=nbuf+new_size;}
  strncpy(b->ptr,string,len); b->ptr=b->ptr+len;
}
static BUFF *ap_bcreate(apr_pool_t *p,int ignore_flag)
{
  struct BUFF *b=apr_palloc(p,sizeof(struct BUFF));
  b->p=p; b->ptr=b->buf=apr_palloc(p,1024); b->lim=b->buf+1024;
  return b;
}
#endif

static void buf_write_4bytes(unsigned int i,BUFF *b)
{
  ap_bputc((i>>24),b);       ap_bputc(((i>>16)&0xFF),b);
  ap_bputc(((i>>8)&0xFF),b); ap_bputc((i&0xFF),b);
}

static void buf_write_string(char *string,BUFF *b)
{
  int len=strlen(string);
  ap_bputc(0x06,b); buf_write_4bytes(len,b); ap_bputs(string,b);
}

static void buf_write_symbol(char *string,BUFF *b)
{
  int len=strlen(string);
  ap_bputc(0x07,b); buf_write_4bytes(len,b); ap_bputs(string,b);
}

static void write_table_as_slotmap(request_rec *r,apr_table_t *t,BUFF *b,int post_size,char *post_data)
{
  const apr_array_header_t *ah=apr_table_elts(t); int n_elts=ah->nelts;
  apr_table_entry_t *scan=(apr_table_entry_t *) ah->elts, *limit=scan+n_elts;
  ap_bputc(0x42,b); ap_bputc(0xC1,b);
  if (post_size) buf_write_4bytes(n_elts*2+2,b);
  else buf_write_4bytes(n_elts*2,b);
  while (scan < limit) {
    buf_write_symbol(scan->key,b);
    buf_write_string(scan->val,b);
    scan++;}
  if (post_size) {
    buf_write_symbol("POST_DATA",b);
    ap_bputc(0x05,b); buf_write_4bytes(post_size,b);
    ap_bwrite(b,post_data,post_size);}
}

/* Handling fdserv requests */

#if APACHE13
static int fdserv_handler(request_rec *r)
{
  char sbuf[MAX_STRING_LEN], *post_data; int post_size;
#if TRACK_EXECUTION_TIMES
  struct timeb start, end; 
#endif
  BUFF *out=ap_bcreate(r->pool,B_WR|B_SOCKET);
  BUFF *in=ap_bcreate(r->pool,B_RD|B_SOCKET);
  int socket=connect_to_servlet(r);
  errno=0; if (socket < 0) {
    ap_log_error(APLOG_MARK,APLOG_ERR,r->server,
		 "Connection failed to %s for %s",
		 r->filename,r->unparsed_uri);
    r->content_type="text/html";
    ap_send_http_header(r);
    ap_rvputs(r,"<HTML>\n<HEAD>\n<TITLE>");
    ap_rprintf(r,"Cannot start fdservlet for %s",r->filename);
    ap_rvputs(r,"</HEAD>\n<BODY>\n<H1>");
    ap_rprintf(r,"Cannot start fdservlet for %s",r->filename);
    ap_rvputs(r,"</BODY>\n</HTML>");
    return HTTP_SERVICE_UNAVAILABLE;}
  else ap_log_error(APLOG_MARK,APLOG_DEBUG,r->server,
		    "Socket %d serves %s for %s",
		    socket,r->filename,r->unparsed_uri);
#if TRACK_EXECUTION_TIMES
  ftime(&start);
#endif
  ap_add_common_vars(r); ap_add_cgi_vars(r);
  ap_bpushfd(in,socket,-1);
  ap_bpushfd(out,-1,socket);
  if (r->method_number == M_POST) {
    char bigbuf[4096];
    ap_setup_client_block(r,REQUEST_CHUNKED_ERROR);
    if (ap_should_client_block(r)) {
      int bytes_read, size=0, limit=16384; char *data=ap_palloc(r->pool,16384);
      while ((bytes_read=ap_get_client_block(r,bigbuf,4096)) > 0) {
	ap_log_error(APLOG_MARK,APLOG_DEBUG,r->server,
		     "Read %d bytes from client",bytes_read);
	ap_reset_timeout(r);
	if (size+bytes_read > limit) {
	  char *newbuf=ap_palloc(r->pool,limit*2); memcpy(newbuf,data,size);
	  data=newbuf; limit=limit*2;}
	memcpy(data+size,bigbuf,bytes_read); size=size+bytes_read;}
      post_data=data; post_size=size;}
    else {post_data=NULL; post_size=0;}}
  else {post_data=NULL; post_size=0;}
  /* Still need to handle posted data */
  write_table_as_slotmap(r,r->subprocess_env,out,post_size,post_data); ap_bflush(out);
  ap_scan_script_header_err_buff(r, in, sbuf); ap_send_http_header(r);
#if 0
  {
    char buf[4096]; int bytes_read;
    while ((bytes_read=ap_bread(in,buf,4096))>0) {
      ap_rwrite(buf,bytes_read,r); ap_rflush(r);}}
#else
  ap_send_fb(in,r); ap_rflush(r);
#endif
#if TRACK_EXECUTION_TIMES
  {char buf[64]; double interval; ftime(&end);
   interval=1000*(end.time-start.time)+(end.millitm-start.millitm);
   sprintf(buf,"%lf",interval/1000.0);
   apr_table_set(r->subprocess_env,"EXECUTED","yes");
   apr_table_set(r->notes,"exectime",buf);}
#endif
  ap_bclose(out); ap_bclose(in);
  return OK;
}
#endif

#if APACHE20
typedef struct BUFFERED_SOCKET { apr_pool_t *p; int sock; BUFF buf; } BUFFSOCK;

static int sock_fgets(char *buf,int n_bytes,void *stream)
{
  int sock=(int)stream;
  char bytes[1], *write=buf, *limit=buf+n_bytes;; 
  while (read(sock,bytes,1)>0) { 
    if (write>=limit) break; else *write++=bytes[0];
    if (bytes[0] == '\n') break;}
  *write='\0';
  if (write>=limit) return write-buf;
  else return  write-buf;
}

static int sock_write(request_rec *r,char *buf,int n_bytes,int sock)
{
  int bytes_written=0, bytes_to_write=n_bytes;
  while (bytes_written < bytes_to_write) {
    int block_size=write(sock,buf,bytes_to_write-bytes_written);
    if (block_size == 0) {
      break;}
    else bytes_written=bytes_written+block_size;}
  return bytes_written;
}

static void copy_script_output(int sock,request_rec *r)
{
  char buf[4096]; int bytes_read=0;
  while ((bytes_read=read(sock,buf,4096))>0) {
    ap_rwrite(buf,bytes_read,r); ap_rflush(r);}
}

static int fdserv_handler(request_rec *r)
{
  BUFF *reqdata;
  char sbuf[MAX_STRING_LEN], *post_data, errbuf[512];
  int sock, post_size;
  conn_rec *c = r->connection;
  apr_file_t *script_out = NULL, *script_in = NULL, *script_err = NULL;
  apr_bucket_brigade *bb;
  apr_bucket *b;
#if TRACK_EXECUTION_TIMES
  struct timeb start, end; 
#endif
  if(strcmp(r->handler, FDSERV_MAGIC_TYPE) && strcmp(r->handler, "fdservlet"))
    return DECLINED;
  else if (!((r->method_number == M_GET) || (r->method_number == M_POST)))
    return DECLINED;
  else sock=connect_to_servlet(r);
  errno=0; if (sock < 0) {
    ap_log_error(APLOG_MARK,APLOG_ERR,OK,r->server,
		 "Connection failed to %s for %s",
		 r->filename,r->unparsed_uri);
    r->content_type="text/html";
    ap_send_http_header(r);
    ap_rvputs(r,"<HTML>\n<HEAD>\n<TITLE>",NULL);
    ap_rprintf(r,"Cannot start fdservlet for %s",r->filename);
    ap_rvputs(r,"</HEAD>\n<BODY>\n<H1>",NULL);
    ap_rprintf(r,"Cannot start fdservlet for %s",r->filename);
    ap_rvputs(r,"</BODY>\n</HTML>",NULL);
    return HTTP_SERVICE_UNAVAILABLE;}
  else ap_log_error(APLOG_MARK,APLOG_DEBUG,OK,r->server,
		    "Socket %d serves %s for %s",
		    sock,r->filename,r->unparsed_uri);
  reqdata=ap_bcreate(r->pool,0);
  ap_log_error(APLOG_MARK,APLOG_DEBUG,OK,r->server,
	       "Handling request for %s by using %s with socket %d",
	       r->unparsed_uri,r->filename,sock);
#if TRACK_EXECUTION_TIMES
  ftime(&start);
#endif
  ap_add_common_vars(r); ap_add_cgi_vars(r);
  if (r->method_number == M_POST) {
    char bigbuf[4096];
    ap_setup_client_block(r,REQUEST_CHUNKED_ERROR);
    if (ap_should_client_block(r)) {
      int bytes_read, size=0, limit=16384; char *data=apr_palloc(r->pool,16384);
      while ((bytes_read=ap_get_client_block(r,bigbuf,4096)) > 0) {
	ap_log_error(APLOG_MARK,APLOG_DEBUG,OK,r->server,
		     "Read %d bytes of POST data from client",bytes_read);
	ap_reset_timeout(r);
	if (size+bytes_read > limit) {
	  char *newbuf=apr_palloc(r->pool,limit*2); memcpy(newbuf,data,size);
	  data=newbuf; limit=limit*2;}
	memcpy(data+size,bigbuf,bytes_read); size=size+bytes_read;}
      post_data=data; post_size=size;}
    else {post_data=NULL; post_size=0;}}
  else {post_data=NULL; post_size=0;}
  
  ap_log_error(APLOG_MARK,APLOG_DEBUG,OK,r->server,"Composing request data as a slotmap");

  /* Write the slotmap into buf, the write the buf to the servlet socket */
  write_table_as_slotmap(r,r->subprocess_env,reqdata,post_size,post_data);
  ap_log_error(APLOG_MARK,APLOG_DEBUG,OK,r->server,"Writing %d bytes of request data to socket",
	       reqdata->ptr-reqdata->buf);
  sock_write(r,reqdata->buf,reqdata->ptr-reqdata->buf,sock);
  
  ap_log_error(APLOG_MARK,APLOG_DEBUG,OK,r->server,"Waiting for response from servlet");
  ap_scan_script_header_err_core(r,errbuf,sock_fgets,(void *)sock);
  ap_log_error(APLOG_MARK,APLOG_DEBUG,OK,r->server,"Got response, sending header");
  ap_send_http_header(r);
  ap_log_error(APLOG_MARK,APLOG_DEBUG,OK,r->server,"Header sent, sending content");
  copy_script_output(sock,r);
  ap_log_error(APLOG_MARK,APLOG_DEBUG,OK,r->server,"Done sending content");

#if TRACK_EXECUTION_TIMES
  {char buf[64]; double interval; ftime(&end);
   interval=1000*(end.time-start.time)+(end.millitm-start.millitm);
   sprintf(buf,"%lf",interval/1000.0);
   apr_table_set(r->subprocess_env,"EXECUTED","yes");
   apr_table_set(r->notes,"exectime",buf);}
#endif
  close(sock);
  return OK;
}
#endif

#if APACHE13
static void init_module(server_rec *s,apr_pool_t *p)
{
  socketname_table=apr_table_make(p,64);
#if APACHE13
  ap_add_version_component("mod_fdserv/1.0");
#elif APACHE20
  ap_add_version_component(p,"mod_fdserv/1.0");
#endif
  ap_log_error(APLOG_HEAD,s,"FDSERV Module init done");
}
#else
static int fdserv_init(apr_pool_t *p, apr_pool_t *plog, apr_pool_t *ptemp,
			server_rec *s)
{
  socketname_table=apr_table_make(p,64);
#if APACHE13
  ap_add_version_component("mod_fdserv/1.0");
#elif APACHE20
  ap_add_version_component(p,"mod_fdserv/1.0");
#endif
  ap_log_error(APLOG_HEAD,s,"FDSERV Module init done");
  return OK;
}
#endif

#if APACHE13
handler_rec fdserv_handlers[] = {
    { FDSERV_MAGIC_TYPE, fdserv_handler },
    { "fdservlet", fdserv_handler },
    { NULL }
};
#endif

#if APACHE13
module MODULE_VAR_EXPORT fdserv_module = {
    STANDARD_MODULE_STUFF,
    init_module,               /* initializer */
    create_dir_config,         /* per-dir config creator */
    merge_dir_config,          /* per-dir config merger (default: override) */
    create_server_config,      /* per-server config creator */
    merge_server_config,       /* per-server config merger (default: override) */
    fdserv_cmds,               /* command table */
    fdserv_handlers,           /* [9] content handlers */
    NULL,                      /* [2] URI-to-filename translation */
    NULL,                      /* [5] authenticate user_id */
    NULL,                      /* [6] authorize user_id */
    NULL,                      /* [4] check access (based on src & http headers) */
    NULL,                      /* [7] check/set MIME type */
    NULL,                      /* [8] fixups */
    NULL,                      /* [10] logger */
    NULL,                      /* [3] header-parser */
    NULL,                      /* process initialization */
    NULL,                      /* process exit/cleanup */
    NULL                       /* [1] post read-request handling */
};
#else
static void register_hooks(apr_pool_t *p)
{
  ap_hook_handler(fdserv_handler, NULL, NULL, APR_HOOK_MIDDLE);
  ap_hook_post_config(fdserv_init, NULL, NULL, APR_HOOK_MIDDLE);

}

module AP_MODULE_DECLARE_DATA fdserv_module =
{
    STANDARD20_MODULE_STUFF,
    create_dir_config,				/* dir config creater */
    merge_dir_config,				/* dir merger --- default is to override */
    create_server_config,			/* server config */
    merge_server_config,			/* merge server config */
    fdserv_cmds,				/* command apr_table_t */
    register_hooks				/* register hooks */
};
#endif

/* Emacs local variables
;;;  Local variables: ***
;;;  alt-compile-command: "apxs -c mod_fdserv.c" ***
;;;  compile-command: "/var/apache20/bin/apxs -c mod_fdserv.c" ***
;;;  End: ***
*/

