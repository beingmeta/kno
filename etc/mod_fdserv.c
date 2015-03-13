/* C Mode */

/* mod_fdserv.c
   Copyright (C) 2002-2012 beingmeta, inc.  All Rights Reserved
   This is a Apache module supporting persistent FramerD servers

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
#include "unixd.h"
#include "mpm_common.h"
#include "netdb.h"

#if ((AP_SERVER_MAJORVERSION_NUMBER<=2)&&(AP_SERVER_MINORVERSION_NUMBER<4))
#define ap_unixd_config unixd_config
#endif

#ifndef DEBUG_ALL
#define DEBUG_ALL 0
#endif

#ifndef DEBUG_FDSERV
#define DEBUG_FDSERV DEBUG_ALL
#endif

#ifndef DEBUG_CGIDATA
#define DEBUG_CGIDATA DEBUG_ALL
#endif

#ifndef DEBUG_CONNECT
#define DEBUG_CONNECT DEBUG_ALL
#endif

#ifndef DEBUG_TRANSPORT
#define DEBUG_TRANSPORT DEBUG_ALL
#endif

#ifndef DEBUG_SOCKETS
#define DEBUG_SOCKETS DEBUG_ALL
#endif

#define LOGNOTICE APLOG_NOTICE
#define LOGINFO APLOG_INFO

#if DEBUG_FDSERV
#define LOGDEBUG APLOG_INFO
#else
#define LOGDEBUG APLOG_DEBUG
#endif

#if DEBUG_SOCKETS
#define LOGSOCKET APLOG_WARNING
#else
#define LOGSOCKET APLOG_NOTICE
#endif

static void log_config(cmd_parms *parms,const char *arg)
{
  if (parms->path)
    ap_log_error
      (APLOG_MARK,APLOG_DEBUG,OK,parms->server,
       "Config %s %s(%s)=%s",parms->server->server_hostname,
       parms->cmd->name,parms->path,arg);
  else ap_log_error
      (APLOG_MARK,APLOG_DEBUG,OK,parms->server,
       "Config %s %s=%s",parms->server->server_hostname,
       parms->cmd->name,arg);
}

static int debug_kv(void *data,const char *key,const char *value)
{
  request_rec *r=(request_rec *)data;
  ap_log_error(APLOG_MARK,APLOG_WARNING,OK,r->server,"debug_kv %s=%s",key,value);
  return 0;
}


#if DEBUG_CONFIG
#define LOG_CONFIG(p,arg) log_config(p,arg)
#else
#define LOG_CONFIG(p,arg) 
#endif

#define APACHE20 1
#define APACHE13 0

#include "mod_fdserv_fileinfo.h"

#if (APR_SIZEOF_VOIDP==8)
typedef unsigned long long INTPOINTER;
#else
typedef unsigned int INTPOINTER;
#endif

#ifndef DEFAULT_SERVLET_WAIT
#define DEFAULT_SERVLET_WAIT 60
#endif

#ifndef MAX_CONFIGS
#define MAX_CONFIGS 128
#endif

#ifndef DEFAULT_LOG_SYNC
#define DEFAULT_LOG_SYNC 0
#endif

#include "util_script.h"
#include "apr.h"
#include "apr_portable.h"
#include "apr_strings.h"

#include <sys/un.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <unistd.h>
#include <sys/ioctl.h>

#ifndef TRACK_EXECUTION_TIMES
#if HAVE_FTIME
#define TRACK_EXECUTION_TIMES 1
#else
#define TRACK_EXECUTION_TIMES 1
#endif
#endif

#define FDSERV_INIT_SERVLETS 32

#if TRACK_EXECUTION_TIMES
#include "sys/timeb.h"
#endif

#define FDSERV_MAGIC_TYPE "application/x-httpd-fdserv"

#ifndef DEFAULT_KEEP_SOCKS
#define DEFAULT_KEEP_SOCKS 2
#endif

#ifndef DEFAULT_MAX_SOCKS
#define DEFAULT_MAX_SOCKS 16
#endif

/* This is where cross-server consing for the entire module happens. */
static apr_pool_t *fdserv_pool;

typedef enum {filesock,aprsock,badsock=0} fdsocktype;

typedef struct FDSOCKET {
  fdsocktype socktype;
  const char *sockname;
  struct FDSERVLET *servlet;
  int socket_index, busy, closed;
  union { int fd; apr_socket_t *apr;}
    conn;}
  FDSOCKET;
typedef struct FDSOCKET *fdsocket;

typedef struct FDSERVLET {
  fdsocktype socktype;
  const char *sockname;
  apr_time_t spawning, spawned;
  int servlet_index;
  const struct server_rec *server;
  union {
    struct sockaddr_un path;
    struct apr_sockaddr_t *addr;}
    endpoint;
  apr_thread_mutex_t *lock; /* Lock to get/add sockets */
  /* How many sockets are currently kept open and live in the sockets array. */
  int n_socks;
  int max_socks; /* Total number of connections to keep open */
  int keep_socks; /* How many sockets to keep open */
  int n_busy; /* How many sockets are currently busy */
  int n_ephemeral;
  struct FDSOCKET *sockets;} FDSERVLET;
typedef struct FDSERVLET *fdservlet;

module AP_MODULE_DECLARE_DATA fdserv_module;

static struct FDSERVLET *servlets;
static int n_servlets=0, max_servlets=-1;
static apr_thread_mutex_t *servlets_lock;

#ifndef DEFAULT_ISASYNC
#define DEFAULT_ISASYNC 1
#endif

#ifndef _FILEINFO
#define _FILEINFO __FILE
#endif

#ifndef USEDTBLOCK
#define USEDTBLOCK 0
#endif

static int use_dtblock=USEDTBLOCK;

char *version_num="2.4.5";
char version_info[256];

static void init_version_info()
{
  sprintf(version_info,"mod_fdserv/%s",version_num);
}

/* Compatibility and utilities */

#define APLOG_HEAD APLOG_MARK,LOGDEBUG,OK
#define ap_can_exec(file) (1)
#define ap_send_http_header(r) ;
#define ap_reset_timeout(r) ;
#define apr_is_directory(s) (0)
#define isdirectoryp(p,s) (ap_is_directory(p,s))
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

static char *prealloc(apr_pool_t *p,char *ptr,int new_size,int old_size)
{
  char *newptr=apr_pcalloc(p,new_size);
  if (newptr != ptr) memmove(newptr,ptr,old_size);
  return newptr;
}

static char *makepath(apr_pool_t *p,const char *root,const char *name)
{
  int rootlen=strlen(root), endslash=((*root) && (root[rootlen-1]=='/'));
  char *path;
  if (endslash) path=apr_pstrcat(p,root,name,NULL);
  else if (isdirectoryp(p,root))
    path=apr_pstrcat(p,root,"/",name,NULL);
  else path=apr_pstrcat(p,root,name,NULL);
  return path;
}

/* Checking writability of files */

typedef apr_finfo_t fileinfo;
#define FINFO_FLAGS							\
   (APR_FINFO_USER|APR_FINFO_GROUP|APR_FINFO_PROT|APR_FINFO_TYPE)
static int get_file_info(apr_pool_t *p,const char *filename,fileinfo *info)
{
  return apr_stat(info,filename,FINFO_FLAGS,p);
}
static int stat_can_writep(apr_pool_t *p,server_rec *s,apr_finfo_t *finfo)
{
  apr_uid_t uid; apr_gid_t gid;
  apr_uid_current(&uid,&gid,p);
  ap_log_error
    (APLOG_MARK,LOGDEBUG,OK,s,
     "Checking writability of %s with uid=%d gid=%d",
     ((finfo->fname) ? (finfo->fname) : (finfo->name)),
     uid,gid);
  if (uid==0) return 1;
  if (uid == finfo->user)
    if (finfo->protection & APR_FPROT_UWRITE) return 1;
  if (uid == finfo->group)
    if (finfo->protection & APR_FPROT_GWRITE) return 1;
  if (finfo->protection & APR_FPROT_WWRITE) return 1;
  else return 0;
}
static int file_existsp(apr_pool_t *p,const char *filename)
{
  fileinfo finfo;
  int rv=apr_stat(&finfo,filename,FINFO_FLAGS,p);
  return (rv==OK);
}

static int file_writablep(apr_pool_t *p,server_rec *s,const char *filename)
{
  fileinfo finfo; int retval;
  ap_log_error(APLOG_MARK,LOGDEBUG,OK,s,
	       "Checking writability of file %s",filename);
  retval=get_file_info(p,filename,&finfo);
  if (retval) {
    ap_log_error(APLOG_MARK,LOGDEBUG,retval,s,
		 "stat failed for %s",filename);
    filename=ap_make_dirstr_parent(p,filename);
    retval=apr_stat(&finfo,filename,FINFO_FLAGS,p);
    if (retval) {
      ap_log_error(APLOG_MARK,APLOG_CRIT,retval,s,
		   "stat failed for %s",filename);
      return 0;}}
  if (stat_can_writep(p,s,&finfo)) {
    ap_log_error(APLOG_MARK,LOGDEBUG,retval,s,
		 "found file writable: %s",filename);
    return 1;}
  else return 0;
}

static int file_socket_existsp(apr_pool_t *p,server_rec *s,const char *filename)
{
  fileinfo finfo; int retval;
  ap_log_error(APLOG_MARK,LOGDEBUG,OK,s,
	       "Checking existence of file %s",filename);
  retval=get_file_info(p,filename,&finfo);
  if (retval) return 0;
  else if (stat_can_writep(p,s,&finfo)) {
    ap_log_error(APLOG_MARK,LOGDEBUG,retval,s,
		 "found file writable: %s",filename);
    return 1;}
  else return 0;
}

static char *convert_path(const char *spec,char *buf)
{
  /* Convert the spec (essentially the path to the location plus
     the server name) into a socket name by converting / to : */
  char *write=buf, *read=(char *)spec;
  while (*read)
    if ((*read == '/') || (*read == '\\')) {*write++=':'; read++;}
    else *write++=*read++;
  *write='\0';
  return buf;
}

/* Configuration handling */

/* This maps a URI representation (generated by getpathspec) into
   string socket identifiers */
static apr_table_t *socketname_table;

struct FDSERV_SERVER_CONFIG {
  const char *server_executable;
  const char **config_args;
  const char **req_params;
  const char **servlet_env;
  const char *socket_prefix;
  const char *socket_spec;
  const char *log_prefix;
  const char *log_file;
  int max_socks;
  int keep_socks;
  int servlet_spawn;
  int servlet_wait;
  int use_dtblock;
  int log_sync;
  /* We make these ints because apr_uid_t and even uid_t is sometimes
     unsigned, leaving no way to signal an empty value.	 Go figure. */
  int uid; int gid;};

struct FDSERV_DIR_CONFIG {
  const char *server_executable;
  const char **config_args;
  const char **req_params;
  const char **servlet_env;
  const char *socket_prefix;
  const char *socket_spec;
  const char *log_prefix;
  const char *log_file;
  int max_socks;
  int keep_socks;
  int servlet_spawn;
  int servlet_wait;
  int log_sync;};

static const char *get_sockname(request_rec *r)
{
  char locbuf[PATH_MAX], pathbuf[PATH_MAX];
  const char *cached, *location;
  sprintf(locbuf,"%s:%s",r->server->server_hostname,r->uri);
  location=locbuf;
  cached=apr_table_get(socketname_table,location);
  if (cached) return cached;
  else {
    struct FDSERV_SERVER_CONFIG *sconfig=
      ap_get_module_config(r->server->module_config,&fdserv_module);
    struct FDSERV_DIR_CONFIG *dconfig=
      ap_get_module_config(r->per_dir_config,&fdserv_module);
    const char *socket_location=
      (((dconfig->socket_spec)!=NULL) ? (dconfig->socket_spec) :
       (convert_path(location,pathbuf)));
    const char *socket_prefix=
      ((dconfig->socket_prefix) ? (dconfig->socket_prefix) :
       (sconfig->socket_prefix) ? (sconfig->socket_prefix) :
       "/var/run/fdserv/");

    if ((socket_location)&&
	((strchr(socket_location,'@'))||
	 (strchr(socket_location,':'))))
      return socket_location;

    /* This generates a socketname for a particular request URI.  It's
       not used when we explicitly set socket locations. */
    
    ap_log_rerror
      (APLOG_MARK,LOGDEBUG,OK,r,
       "FDSERV get_sockname socket_location='%s', socket_prefix='%s', isdir=%d",
       socket_location,socket_prefix,
       isdirectoryp(r->pool,socket_prefix));
    
    if ((socket_location)&&(socket_location[0]=='/')) {
      /* Absolute socket file name, just use it. */
      apr_table_set(socketname_table,location,socket_location);
      return apr_table_get(socketname_table,location);}
    else {
      /* Relative socket file name, just append it. */
      char *path=makepath(r->pool,socket_prefix,socket_location);
      apr_table_set(socketname_table,location,path);
      return apr_table_get(socketname_table,location);}}
}

static void *create_server_config(apr_pool_t *p,server_rec *s)
{
  struct FDSERV_SERVER_CONFIG *config=
    apr_palloc(p,sizeof(struct FDSERV_SERVER_CONFIG));
  config->server_executable=NULL;
  config->config_args=NULL;
  config->req_params=NULL;
  config->servlet_env=NULL;
  config->socket_prefix=NULL;
  config->socket_spec=NULL;
  config->log_prefix=NULL;
  config->log_file=NULL;
  config->log_sync=-1;
  config->max_socks=-1;
  config->keep_socks=-1;
  config->servlet_spawn=-1;
  config->servlet_wait=-1;
  config->use_dtblock=-1;
  config->uid=-1; config->gid=-1;
  return (void *) config;
}

static void *merge_server_config(apr_pool_t *p,void *base,void *new)
{
  struct FDSERV_SERVER_CONFIG *config=
    apr_palloc(p,sizeof(struct FDSERV_SERVER_CONFIG));
  struct FDSERV_SERVER_CONFIG *parent=base;
  struct FDSERV_SERVER_CONFIG *child=new;

  memset(config,0,sizeof(struct FDSERV_SERVER_CONFIG));

  if (child->uid <= 0) config->uid=parent->uid; else config->uid=child->uid;
  if (child->gid <= 0) config->gid=parent->gid; else config->gid=child->gid;

  if (child->max_socks <= 0)
    config->max_socks=parent->max_socks;
  else config->max_socks=child->max_socks;

  if (child->keep_socks <= 0)
    config->keep_socks=parent->keep_socks;
  else config->keep_socks=child->keep_socks;

  if (child->servlet_spawn < 0)
    config->servlet_spawn=parent->servlet_spawn;
  else config->servlet_spawn=child->servlet_spawn;

  if (child->servlet_wait < 0)
    config->servlet_wait=parent->servlet_wait;
  else config->servlet_wait=child->servlet_wait;

  if (child->log_sync < 0)
    config->log_sync=parent->log_sync;
  else config->log_sync=child->log_sync;

  if (child->use_dtblock < 0)
    config->use_dtblock=parent->use_dtblock;
  else config->use_dtblock=child->use_dtblock;

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
  
  if (child->req_params) {
    const char **scan=child->req_params;
    char **fresh, **write;
    int n_slots=0;
    while (*scan) {n_slots++; scan++;}
    fresh=apr_palloc(p,(n_slots+1)*sizeof(char *));
    scan=child->req_params; write=fresh;
    while (*scan) {
      *write=apr_pstrdup(p,*scan); write++; scan++;}
    *write=NULL;
    config->req_params=(const char **)fresh;}
  else config->req_params=NULL;
  
  if (child->servlet_env) {
    const char **scan=child->servlet_env;
    char **fresh, **write;
    int n_slots=0;
    while (*scan) {n_slots++; scan++;}
    fresh=apr_palloc(p,(n_slots+1)*sizeof(char *));
    scan=child->servlet_env; write=fresh;
    while (*scan) {
      *write=apr_pstrdup(p,*scan); write++; scan++;}
    *write=NULL;
    config->servlet_env=(const char **)fresh;}
  else config->servlet_env=NULL;

  if (child->socket_prefix)
    config->socket_prefix=apr_pstrdup(p,child->socket_prefix);
  else if (parent->socket_prefix)
    config->socket_prefix=apr_pstrdup(p,parent->socket_prefix);
  else config->socket_prefix=NULL;
  
  if (child->socket_spec)
    config->socket_spec=apr_pstrdup(p,child->socket_spec);
  else if (parent->socket_spec)
    config->socket_spec=apr_pstrdup(p,parent->socket_spec);
  else config->socket_spec=NULL;

  if (child->log_prefix)
    config->log_prefix=apr_pstrdup(p,child->log_prefix);
  else if (parent->log_prefix)
    config->log_prefix=apr_pstrdup(p,parent->log_prefix);
  else config->log_prefix=NULL;


  if (child->log_file)
    config->log_file=apr_pstrdup(p,child->log_file);
  else if (parent->log_file)
    config->log_file=apr_pstrdup(p,parent->log_file);
  else config->log_file=NULL;

  return (void *) config;
}

static void *create_dir_config(apr_pool_t *p,char *dir)
{
  struct FDSERV_DIR_CONFIG *config=
    apr_palloc(p,sizeof(struct FDSERV_DIR_CONFIG));
  config->server_executable=NULL;
  config->config_args=NULL;
  config->req_params=NULL;
  config->servlet_env=NULL;
  config->socket_prefix=NULL;
  config->socket_spec=NULL;
  config->log_prefix=NULL;
  config->log_file=NULL;
  config->max_socks=-1;
  config->keep_socks=-1;
  config->servlet_spawn=-1;
  config->servlet_wait=-1;
  config->log_sync=-1;
  return (void *) config;
}

static void *merge_dir_config(apr_pool_t *p,void *base,void *new)
{
  struct FDSERV_DIR_CONFIG *config
    =apr_palloc(p,sizeof(struct FDSERV_DIR_CONFIG));
  struct FDSERV_DIR_CONFIG *parent=base;
  struct FDSERV_DIR_CONFIG *child=new;

  memset(config,0,sizeof(struct FDSERV_DIR_CONFIG));

  if (child->max_socks <= 0)
    config->max_socks=parent->max_socks;
  else config->max_socks=child->max_socks;
  
  if (child->keep_socks < 0)
    config->keep_socks=parent->keep_socks;
  else config->keep_socks=child->keep_socks;

  if (child->servlet_spawn < 0)
    config->servlet_spawn=parent->servlet_spawn;
  else config->servlet_spawn=child->servlet_spawn;

  if (child->servlet_wait < 0)
    config->servlet_wait=parent->servlet_wait;
  else config->servlet_wait=child->servlet_wait;

  if (child->log_sync < 0)
    config->log_sync=parent->log_sync;
  else config->log_sync=child->log_sync;

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

  if (child->req_params) {
    const char **scan=child->req_params;
    char **fresh, **write;
    int n_slots=0;
    while (*scan) {n_slots++; scan++;}
    fresh=apr_palloc(p,(n_slots+1)*sizeof(char *));
    scan=child->req_params; write=fresh;
    while (*scan) {
      *write=apr_pstrdup(p,*scan); write++; scan++;}
    *write=NULL;
    config->req_params=(const char **)fresh;}
  else config->req_params=NULL;
  
  if (child->servlet_env) {
    const char **scan=child->servlet_env;
    char **fresh, **write;
    int n_slots=0;
    while (*scan) {n_slots++; scan++;}
    fresh=apr_palloc(p,(n_slots+1)*sizeof(char *));
    scan=child->servlet_env; write=fresh;
    while (*scan) {
      *write=apr_pstrdup(p,*scan); write++; scan++;}
    *write=NULL;
    config->servlet_env=(const char **)fresh;}
  else config->servlet_env=NULL;
  
  if (child->socket_prefix)
    config->socket_prefix=apr_pstrdup(p,child->socket_prefix);
  else if (parent->socket_prefix)
    config->socket_prefix=apr_pstrdup(p,parent->socket_prefix);
  else config->socket_prefix=NULL;

  if (child->socket_spec)
    config->socket_spec=apr_pstrdup(p,child->socket_spec);
  else if (parent->socket_spec)
    config->socket_spec=apr_pstrdup(p,parent->socket_spec);
  else config->socket_spec=NULL;

  if (child->log_prefix)
    config->log_prefix=apr_pstrdup(p,child->log_prefix);
  else if (parent->log_prefix)
    config->log_prefix=apr_pstrdup(p,parent->log_prefix);
  else config->log_prefix=NULL;

  if (child->log_file)
    config->log_file=apr_pstrdup(p,child->log_file);
  else if (parent->log_file)
    config->log_file=apr_pstrdup(p,parent->log_file);
  else config->log_file=NULL;

  return (void *) config;
}

/* Configuration directives */

/* The socket address or filename, relative to the socket prefix. */ 
static const char *socket_spec(cmd_parms *parms,void *mconfig,const char *arg)
{
  struct FDSERV_DIR_CONFIG *dconfig=mconfig;
  struct FDSERV_SERVER_CONFIG *sconfig=
    ap_get_module_config(parms->server->module_config,&fdserv_module);
  struct server_rec *srv=parms->server;
  const char *fullpath=NULL, *spec=NULL;

  if (arg[0]=='/') spec=fullpath=arg;
  else if ((strchr(arg,'@'))||(strchr(arg,':'))) spec=arg;
  else if (dconfig->socket_prefix)
    spec=fullpath=apr_pstrcat(parms->pool,dconfig->socket_prefix,arg,NULL);
  else if (sconfig->socket_prefix)
    spec=fullpath=apr_pstrcat(parms->pool,sconfig->socket_prefix,arg,NULL);
  else spec=arg;

  dconfig->socket_spec=spec;
  
  if (!(fullpath))
    ap_log_error(APLOG_MARK,APLOG_INFO,OK,parms->server,
		 "For host '%s' from %s, fdserv socket=%s",
		 srv->server_hostname,srv->defn_name,
		 spec);
  else if (!(file_writablep(parms->pool,parms->server,fullpath)))
    ap_log_error(APLOG_MARK,APLOG_CRIT,OK,parms->server,
		 "For host '%s' from %s, fdserv socket %s=%s is unwritable",
		 srv->server_hostname,srv->defn_name,
		 arg,fullpath);
  else ap_log_error(APLOG_MARK,APLOG_INFO,OK,parms->server,
		    "For host '%s' from %s, fdserv socket %s=%s",
		    srv->server_hostname,srv->defn_name,
		    arg,fullpath);
  return NULL;
}

/* Connection settings */

static const char *using_dtblock(cmd_parms *parms,void *mconfig,const char *arg)
{
  LOG_CONFIG(parms,arg);
  struct FDSERV_SERVER_CONFIG *sconfig=
    ap_get_module_config(parms->server->module_config,&fdserv_module);
  if (!(arg))
    sconfig->use_dtblock=0;
  else if (!(*arg))
    sconfig->use_dtblock=0;
  else if ((*arg=='1')||(*arg=='y')||(*arg=='y'))
    sconfig->use_dtblock=1;
  else sconfig->use_dtblock=0;
  return NULL;
}

static const char *servlet_keep(cmd_parms *parms,void *mconfig,const char *arg)
{
  LOG_CONFIG(parms,arg);
  if (parms->path) {
    struct FDSERV_DIR_CONFIG *dconfig=mconfig;
    int n_connections=atoi(arg);
    dconfig->keep_socks=n_connections;
    return NULL;}
  else {
    struct FDSERV_SERVER_CONFIG *sconfig=mconfig;
    int n_connections=atoi(arg);
    sconfig->keep_socks=n_connections;
    return NULL;}
}

static const char *servlet_maxconn(cmd_parms *parms,void *mconfig,const char *arg)
{
  LOG_CONFIG(parms,arg);
  if (parms->path) {
    struct FDSERV_DIR_CONFIG *dconfig=mconfig;
    int n_connections=atoi(arg);
    dconfig->max_socks=n_connections;
    return NULL;}
  else {
    struct FDSERV_SERVER_CONFIG *sconfig=mconfig;
    int n_connections=atoi(arg);
    sconfig->max_socks=n_connections;
    return NULL;}
}

/* How to run fdserv */

/* Setting the fdserv executable for spawning new processes */
static const char *servlet_executable
   (cmd_parms *parms,void *mconfig,const char *arg)
{
  struct FDSERV_SERVER_CONFIG *sconfig=
    ap_get_module_config(parms->server->module_config,&fdserv_module);
  struct FDSERV_DIR_CONFIG *dconfig=mconfig;
  LOG_CONFIG(parms,arg);
  if (executable_filep(parms->pool,arg))
    if (parms->path) {
      dconfig->server_executable=arg;
      return NULL;}
    else {
      sconfig->server_executable=arg;
      return NULL;}
  else return "server executable is not executable";
}

static const char *servlet_wait(cmd_parms *parms,void *mconfig,const char *arg)
{
  struct FDSERV_SERVER_CONFIG *sconfig=
    ap_get_module_config(parms->server->module_config,&fdserv_module);
  struct FDSERV_DIR_CONFIG *dconfig=mconfig;
  char *end=NULL; long wait_interval=-1;
  LOG_CONFIG(parms,arg);
  if (*arg=='\0') {
    ap_log_error(APLOG_MARK,APLOG_CRIT,OK,parms->server,"Bad wait interval '%s'",arg);
    return NULL;}
  else wait_interval=strtol(arg,&end,10);
  if (*end!='\0') {
    ap_log_error(APLOG_MARK,APLOG_CRIT,OK,parms->server,"Bad wait interval '%s'",arg);
    return NULL;}
  if (parms->path) {
    dconfig->servlet_wait=wait_interval;
    return NULL;}
  else {
    sconfig->servlet_wait=wait_interval;
    return NULL;}
}

static const char *servlet_spawn(cmd_parms *parms,void *mconfig,const char *arg)
{
  struct FDSERV_SERVER_CONFIG *sconfig=
    ap_get_module_config(parms->server->module_config,&fdserv_module);
  struct FDSERV_DIR_CONFIG *dconfig=mconfig;
  int spawn_wait; char *end;
  LOG_CONFIG(parms,arg);
  if (strcasecmp(arg,"on")==0) spawn_wait=10;
  else if (strcasecmp(arg,"off")==0) spawn_wait=0;
  else if (strcasecmp(arg,"auto")==0) spawn_wait=-1;
  else if (*arg=='\0') {
    ap_log_error(APLOG_MARK,APLOG_CRIT,OK,parms->server,
		 "Bad spawn wait '%s'",arg);
    return NULL;}
  else {
    spawn_wait=strtol(arg,&end,10);
    if ((end)&&(*end!='\0')) {
      ap_log_error(APLOG_MARK,APLOG_CRIT,OK,parms->server,
		   "Bad spawn wait '%s'",arg);
      return NULL;}}
  if (parms->path) {
    dconfig->servlet_spawn=spawn_wait;
    return NULL;}
  else {
    sconfig->servlet_spawn=spawn_wait;
    return NULL;}
}

static const char *log_sync(cmd_parms *parms,void *mconfig,const char *arg)
{
  int dosync=((*arg=='1')||(*arg=='y')||(*arg=='Y'));
  LOG_CONFIG(parms,arg);
  if (parms->path) {
    struct FDSERV_DIR_CONFIG *dconfig=(struct FDSERV_DIR_CONFIG *)mconfig;
    dconfig->log_sync=dosync;
    return NULL;}
  else {
    struct FDSERV_SERVER_CONFIG *sconfig=
      ap_get_module_config(parms->server->module_config,&fdserv_module);
    sconfig->log_sync=dosync;}
  return NULL;
}

static char **extend_config(apr_pool_t *p,char **config_args,const char *var,const char *val);

/* Adding config variables to be passed to fdserv */
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

/* Adding config variables to be passed to fdserv */
static const char *servlet_env
  (cmd_parms *parms,void *mconfig,const char *arg1,const char *arg2)
{
  struct FDSERV_DIR_CONFIG *dconfig=mconfig;
  struct FDSERV_SERVER_CONFIG *sconfig=
    ap_get_module_config(parms->server->module_config,&fdserv_module);
  apr_pool_t *p=parms->pool;
  if (parms->path) {
    dconfig->servlet_env=
      (const char **)extend_config(p,(char **)dconfig->servlet_env,arg1,arg2);
    return NULL;}
  else {
    sconfig->servlet_env=
      (const char **)extend_config(p,(char **)sconfig->servlet_env,arg1,arg2);
    return NULL;}
}

static char **extend_config(apr_pool_t *p,char **config_args,const char *var,const char *val)
{
  char *config_arg=apr_pstrcat(p,var,"=",val,NULL);
  char *config_prefix=apr_pstrcat(p,var,"=",NULL);
  int prefix_len=strlen(config_prefix);
  if (config_args==NULL) {
    char **vec=apr_palloc(p,2*sizeof(char *));
    vec[0]=config_arg; vec[1]=NULL;
    return vec;}
  else {
    char **scan=config_args, **grown; int n_configs=0;
    while (*scan) {
      if (strncmp(*scan,config_prefix,prefix_len)==0) {
	/* Override an earlier definition */
	*scan=config_arg;
	return config_args;}
      else {scan++; n_configs++;}}
    grown=(char **)prealloc(p,(char *)config_args,
			    (n_configs+2)*sizeof(char *),
			    (n_configs+1)*sizeof(char *));
    grown[n_configs]=config_arg; grown[n_configs+1]=NULL;
    return grown;}
}

static char **extend_params(apr_pool_t *p,char **req_params,
			    const char *var,const char *val);

/* Adding config variables to be passed to fdserv */
static const char *servlet_param
  (cmd_parms *parms,void *mconfig,const char *arg1,const char *arg2)
{
  struct FDSERV_DIR_CONFIG *dconfig=mconfig;
  struct FDSERV_SERVER_CONFIG *sconfig=
    ap_get_module_config(parms->server->module_config,&fdserv_module);
  apr_pool_t *p=parms->pool;
  if (parms->path) {
    dconfig->req_params=
      (const char **)extend_params(p,(char **)dconfig->req_params,arg1,arg2);
    return NULL;}
  else {
    sconfig->req_params=
      (const char **)extend_params(p,(char **)sconfig->req_params,arg1,arg2);
    return NULL;}
}

static char **extend_params(apr_pool_t *p,char **req_params,const
			    char *var,const char *val)
{
  if (req_params==NULL) {
    char **vec=apr_palloc(p,3*sizeof(char *));
    vec[0]=apr_pstrdup(p,var);
    vec[1]=apr_pstrdup(p,val);
    vec[2]=NULL;
    return vec;}
  else {
    char **scan=req_params, **grown; int n_params=0;
    while (*scan) {
      if (strcmp(var,scan[0])) {
	scan[1]=apr_pstrdup(p,val);
	return req_params;}
      scan++; n_params++;}
    grown=(char **)prealloc(p,(char *)req_params,
			    (n_params+3)*sizeof(char *),
			    (n_params+1)*sizeof(char *));
    grown[n_params]=apr_pstrdup(p,var);
    grown[n_params+1]=apr_pstrdup(p,val);
    grown[n_params+2]=NULL;
    return grown;}
}

/* What userid should run servlets */
static const char *servlet_user(cmd_parms *parms,void *mconfig,const char *arg)
{
  struct FDSERV_SERVER_CONFIG *sconfig=
    ap_get_module_config(parms->server->module_config,&fdserv_module);
  int uid=(int)ap_uname2id(arg);
  if (uid >= 0) {
    /* On some platforms (OS X), this condition can never happen,
       which seems like a bug.*/
    sconfig->uid=uid;
    return NULL;}
  else return "invalid user id";
}

/* What the default group should be for running servlets */
static const char *servlet_group(cmd_parms *parms,void *mconfig,const char *arg)
{
  struct FDSERV_SERVER_CONFIG *sconfig=
    ap_get_module_config(parms->server->module_config,&fdserv_module);
  int gid=(int)ap_gname2id(arg);
  if (gid >= 0) {
    /* On some platforms (OS X), this condition can never happen,
       which seems like a bug.*/
    sconfig->gid=gid;
    return NULL;}
  else return "invalid group id";
}

/* What prefix to use for servlet file sockets (also for .pid files)  */
static const char *socket_prefix(cmd_parms *parms,void *mconfig,const char *arg)
{
  struct FDSERV_DIR_CONFIG *dconfig=mconfig;
  struct FDSERV_SERVER_CONFIG *sconfig=
    ap_get_module_config(parms->server->module_config,&fdserv_module);
  const char *fullpath;
  if (arg[0]=='/') fullpath=arg;
  else fullpath=ap_server_root_relative(parms->pool,(char *)arg);
  if (parms->path) 
    dconfig->socket_prefix=fullpath;
  else 
    sconfig->socket_prefix=fullpath;
  if (parms->path)
    ap_log_error(APLOG_MARK,APLOG_INFO,OK,parms->server,
		 "Socket Prefix set to %s for path %s",
		 fullpath,parms->path);
  else ap_log_error(APLOG_MARK,APLOG_INFO,OK,parms->server,
		    "Socket Prefix set to %s for server %s",
		    fullpath,parms->server->server_hostname);
  
  if (!(file_writablep(parms->pool,parms->server,fullpath)))
    ap_log_error (APLOG_MARK,APLOG_CRIT,OK,parms->server,
		  "Socket Prefix %s (%s) is not writable",
		  arg,fullpath);

  return NULL;
}

/* What prefix to use for log files */
static const char *log_prefix(cmd_parms *parms,void *mconfig,const char *arg)
{
  struct FDSERV_DIR_CONFIG *dconfig=mconfig;
  struct FDSERV_SERVER_CONFIG *sconfig=
    ap_get_module_config(parms->server->module_config,&fdserv_module);
  const char *fullpath;
  if (arg[0]=='/') fullpath=arg;
  else fullpath=ap_server_root_relative(parms->pool,(char *)arg);
  if (parms->path) 
    dconfig->log_prefix=fullpath;
  else 
    sconfig->log_prefix=fullpath;

  if (parms->path)
    ap_log_error(APLOG_MARK,APLOG_INFO,OK,parms->server,
		 "Log Prefix set to %s for path %s",
		 fullpath,parms->path);
  else ap_log_error(APLOG_MARK,APLOG_INFO,OK,parms->server,
		    "Log Prefix set to %s for server %s",
		    fullpath,parms->server->server_hostname);

  if (!(file_writablep(parms->pool,parms->server,fullpath)))
    ap_log_error(APLOG_MARK,APLOG_CRIT,OK,parms->server,
		 "Log prefix %s (%s) is not writable",
		 arg,fullpath);
  return NULL;
}

/* The log file to use for launched fdserv processes */
static const char *log_file(cmd_parms *parms,void *mconfig,const char *arg)
{
  struct FDSERV_DIR_CONFIG *dconfig=mconfig;
  struct FDSERV_SERVER_CONFIG *sconfig=mconfig;
  struct FDSERV_SERVER_CONFIG *smodconfig=
    ap_get_module_config(parms->server->module_config,&fdserv_module);
  const char *log_prefix=NULL, *fullpath=NULL;
  if (parms->path)
    log_prefix=dconfig->log_prefix;
  else log_prefix=sconfig->log_prefix;
  if (log_prefix==NULL) log_prefix=sconfig->log_prefix;
  if (log_prefix==NULL) log_prefix=smodconfig->log_prefix;
  if (arg[0]=='/') fullpath=arg;
  else if (log_prefix)
    fullpath=apr_pstrcat(parms->pool,log_prefix,arg,NULL);
  else fullpath=ap_server_root_relative(parms->pool,(char *)arg);
  if (parms->path)
    dconfig->log_file=arg;
  else sconfig->log_file=arg;
  if (!(file_writablep(parms->pool,parms->server,fullpath)))
    ap_log_error(APLOG_MARK,APLOG_CRIT,OK,parms->server,
		 "Log file %s=%s+%s is unwritable",
		 fullpath,log_prefix,arg);
  else ap_log_error (APLOG_MARK,APLOG_INFO,OK,parms->server,
		     "Log file %s=%s+%s",fullpath,log_prefix,arg);
  return NULL;
}

static const command_rec fdserv_cmds[] =
{
  AP_INIT_TAKE1
  ("FDServletDTBlock", using_dtblock, NULL, OR_ALL,
   "whether to use the DTBlock DType representation to send requests"),
  AP_INIT_TAKE1("FDServletKeep", servlet_keep, NULL, OR_ALL,
		"how many connections to the servlet to keep open"),

  /* Everything below here is stuff about how to start a servlet */
  AP_INIT_TAKE1("FDServletSpawn", servlet_spawn, NULL, OR_ALL,
		"How long to wait for servlets to start (on=5, off=0)"),
  AP_INIT_TAKE1("FDServletExecutable", servlet_executable, NULL, OR_ALL,
		"the executable used to start a servlet"),
  AP_INIT_TAKE1("FDServletWait", servlet_wait, NULL, OR_ALL,
		"the number of seconds to wait for the servlet to startup"),
  AP_INIT_TAKE2("FDServletConfig", servlet_config, NULL, OR_ALL,
		"configuration parameters to the servlet"),
  AP_INIT_TAKE2("FDServletEnv", servlet_env, NULL, OR_ALL,
		"environment variables for servlet execution"),
  AP_INIT_TAKE2("FDServletParam", servlet_param, NULL, OR_ALL,
		"CGI parameters to pass with each request to the servlet"),
  AP_INIT_TAKE1("FDServletUser", servlet_user, NULL, RSRC_CONF,
	       "the user whom the fdservlet will run as"),
  AP_INIT_TAKE1("FDServletGroup", servlet_group, NULL, RSRC_CONF,
	       "the group whom the fdservlet will run as"),

  AP_INIT_TAKE1("FDServletPrefix", socket_prefix, NULL, OR_ALL,
	       "the prefix to be appended to socket names"),
  AP_INIT_TAKE1("FDServletSocket", socket_spec, NULL, OR_ALL,
	       "the socket file to be used for a particular script"),
  AP_INIT_TAKE1("FDServletLogPrefix", log_prefix, NULL, OR_ALL,
	       "the prefix for the logfile to be used for scripts"),
  AP_INIT_TAKE1("FDServletLog", log_file, NULL, OR_ALL,
	       "the logfile to be used for scripts"),
  AP_INIT_TAKE1("FDServletLogSync", log_sync, NULL, OR_ALL,
	       "whether to write to the log synchronously"),
  {NULL}
};

/* Launching fdservlet processes */

static int spawn_fdservlet (fdservlet s,request_rec *r,apr_pool_t *);

#define RUN_FDSERV_PERMISSIONS \
 (APR_FPROT_UREAD | APR_FPROT_UWRITE | APR_FPROT_UEXECUTE | \
  APR_FPROT_GREAD | APR_FPROT_GWRITE | APR_FPROT_GEXECUTE | \
  APR_FPROT_WREAD |APR_FPROT_WEXECUTE)

static int check_directory(apr_pool_t *p,const char *filename)
{
  char *dirname=ap_make_dirstr_parent(p,filename);
  int retval=apr_dir_make_recursive(dirname,RUN_FDSERV_PERMISSIONS,p);
  if (retval) return retval;
  else if (chown(dirname,ap_unixd_config.user_id,ap_unixd_config.group_id)<0)
    return 1;
  else return 0;
}

static const char *get_log_file(request_rec *r,const char *sockname) /* 2.0 */
{
  struct FDSERV_SERVER_CONFIG *sconfig=
    ap_get_module_config(r->server->module_config,&fdserv_module);
  struct FDSERV_DIR_CONFIG *dconfig=
    ap_get_module_config(r->per_dir_config,&fdserv_module);
  const char *log_file=
    (((dconfig->log_file)!=NULL) ? (dconfig->log_file) :
     ((sconfig->log_file)!=NULL) ? (sconfig->log_file) : (NULL));
  const char *log_prefix=
    ((dconfig->log_prefix) ? (dconfig->log_prefix) :
     (sconfig->log_prefix) ? (sconfig->log_prefix) :
     ap_server_root_relative(r->pool,"logs/fdserv/"));
  
  ap_log_rerror
    (APLOG_MARK,APLOG_INFO,OK,r,
     "spawn get_logfile log_file=%s, log_prefix=%s",
     log_file,log_prefix);
  
  if (log_file==NULL) {
    if (sockname==NULL) return NULL;
    else {
      char *dot=strrchr(sockname,'.'), *slash=strrchr(sockname,'/');
      char *copy, *buf;
      if ((dot) && (dot<slash)) dot=NULL;
      if ((slash==NULL) || (slash[1]=='\0')) slash=(char *)sockname;
      else slash++;
      copy=apr_pstrdup(r->pool,slash);
      if (dot) copy[dot-slash]='\0';
      buf=apr_pstrcat(r->pool,copy,".log",NULL);
      log_file=(const char *)buf;}}
  
  if ((log_file)&&(log_file[0]=='/')) {
    /* Absolute log file name, just use it. */
    return log_file;}
  else if (log_prefix)
    return makepath(r->pool,log_prefix,log_file);
  else return log_file;
}

static int spawn_wait(fdservlet s,request_rec *r) ;

static int spawn_fdservlet(fdservlet s,request_rec *r,apr_pool_t *p) 
{
  const char *sockname=s->sockname;
  /* This launches the fdservlet process.  It probably shouldn't be used
     when using TCP to connect, since the specified server might not be one
     we can lanuch a process on (i.e. not us) */
  apr_proc_t proc; apr_procattr_t *attr;
  /* Executable, socket name, NULL, LOG_FILE env, NULL */
  const char *argv[2+MAX_CONFIGS+1+1+1], **envp, **write_argv=argv;
  struct stat stat_data; int rv, n_configs=0, retval=0;
  const char *nospawn=apr_pstrcat(p,sockname,".nospawn",NULL);
  const char *lockname=apr_pstrcat(p,sockname,".spawn",NULL);
  apr_file_t *lockfile;
  int unlock=0;

  server_rec *server=r->server;
  struct FDSERV_SERVER_CONFIG *sconfig=
    ap_get_module_config(r->server->module_config,&fdserv_module);
  struct FDSERV_DIR_CONFIG *dconfig=
    ap_get_module_config(r->per_dir_config,&fdserv_module);
  const char *exename=((dconfig->server_executable) ?
		       (dconfig->server_executable) :
		       (sconfig->server_executable) ?
		       (sconfig->server_executable) :
		       "/usr/bin/fdservlet");
  const char **server_configs=(sconfig->config_args);
  const char **dir_configs=(dconfig->config_args);
  const char **server_env=(sconfig->servlet_env);
  const char **dir_env=(dconfig->servlet_env);
  const char *log_file=get_log_file(r,NULL);
  int servlet_wait=dconfig->servlet_spawn;
  int log_sync=dconfig->log_sync;
  uid_t uid; gid_t gid;

  if (log_sync<0) log_sync=sconfig->log_sync;
  if (log_sync<0) log_sync=DEFAULT_LOG_SYNC;

  if (servlet_wait<0) servlet_wait=sconfig->servlet_spawn;
  if (servlet_wait<0) servlet_wait=dconfig->servlet_spawn;
  if (servlet_wait<0) servlet_wait=dconfig->servlet_wait;
  if (servlet_wait<0) servlet_wait=sconfig->servlet_wait;
  if ((servlet_wait<0)&&
      ((strchr(sockname,'@')!=NULL)||(strchr(sockname,':')!=NULL)))
    servlet_wait=0;
  else if (servlet_wait<0) {
    if (!(file_existsp(p,nospawn)))
      servlet_wait=DEFAULT_SERVLET_WAIT;
    else servlet_wait=0;}
  else {}
  
  apr_uid_current(&uid,&gid,p);

  if (servlet_wait) {
    apr_status_t lock_status=
      apr_file_open(&lockfile,lockname,
		    APR_FOPEN_READ|APR_FOPEN_WRITE|APR_FOPEN_CREATE,
		    APR_FPROT_OS_DEFAULT,
		    p);
    if (lock_status!=OK) {
      char errbuf[512];
      ap_log_error
	(APLOG_MARK,APLOG_CRIT,lock_status,server,
	 "Failed to lock file %s with status %d (%s) for %s uid=%d gid=%d",
	 lockname,lock_status,apr_strerror(lock_status,errbuf,512),
	 r->unparsed_uri,uid,gid);}
    else {
      lock_status=apr_file_lock
	(lockfile,APR_FLOCK_EXCLUSIVE|APR_FLOCK_NONBLOCK);
      if (lock_status!=OK) {
	char errbuf[512];
	ap_log_error
	  (APLOG_MARK,APLOG_CRIT,lock_status,server,
	   "Failed spawn lock %s with status %d (%s) for %s uid=%d gid=%d",
	   lockname,lock_status,apr_strerror(lock_status,errbuf,512),
	   r->unparsed_uri,uid,gid);
	s->spawning=apr_time_now();}
      else {
	unlock=1;
	ap_log_error
	  (APLOG_MARK,APLOG_WARNING,lock_status,server,
	   "Got spawn lock %s for %s uid=%d gid=%d",
	   lockname,r->unparsed_uri,uid,gid);}}}
  else {
    ap_log_error(APLOG_MARK,APLOG_CRIT,OK,server,
		 "Waiting on external socket %s for %s, uid=%d, gid=%d",
		 sockname,r->unparsed_uri,uid,gid);
    return 0;}
  
  if (log_file==NULL) log_file=get_log_file(r,sockname);
  
  if (s->spawning) {
    ap_log_error(APLOG_MARK,APLOG_CRIT,OK,server,
		 "Waiting on spawned socket %s for %s, uid=%d, gid=%d",
		 sockname,r->unparsed_uri,uid,gid);
    return spawn_wait(s,r);}
  else {
    ap_log_error(APLOG_MARK,APLOG_INFO,OK,server,
		 "Spawning %s %s for %s, uid=%d, gid=%d",
		 exename,sockname,r->unparsed_uri,uid,gid);
    s->spawning=apr_time_now();
    s->spawned=0;}
    
  if (!(file_writablep(p,server,sockname))) {
    ap_log_error(APLOG_MARK,APLOG_CRIT,apr_get_os_error(),server,
		 "Can't write socket file '%s' (%s) for %s, uid=%d, gid=%d",
		 sockname,exename,r->unparsed_uri,uid,gid);
    if (unlock) apr_file_unlock(lockfile);
    return -1;}
  if ((log_file) && (!(file_writablep(p,server,log_file)))) {
    ap_log_error(APLOG_MARK,APLOG_CRIT,apr_get_os_error(),server,
		 "Logfile %s isn't writable for processing %s",
		 log_file,r->unparsed_uri);
    if (unlock) apr_file_unlock(lockfile);
    return -1;}
    
  if (log_file)
    ap_log_error(APLOG_MARK,APLOG_NOTICE,OK,server,
		 "Spawning fdservlet %s @%s>%s for %s, uid=%d, gid=%d",
		 exename,sockname,log_file,r->unparsed_uri,uid,gid);
  else ap_log_error(APLOG_MARK,APLOG_NOTICE,OK,server,
		    "Spawning fdservlet %s @%s for %s, uid=%d, gid=%d",
		    exename,sockname,r->unparsed_uri,uid,gid);

  *write_argv++=(char *)exename;
  *write_argv++=(char *)sockname;
    
  if (gid>0) {
    char *gidconfig=apr_psprintf(p,"RUNGROUP=%d",gid);
    *write_argv++=gidconfig;}
    
  if (server_configs) {
    const char **scan_config=server_configs;
    while (*scan_config) {
      if (n_configs>MAX_CONFIGS) {
	ap_log_error(APLOG_MARK,APLOG_CRIT,OK,server,
		     "Stopped after %d configs!	 Not passing FramerD server config %s",
		     n_configs,*scan_config);}
      else {
	ap_log_error(APLOG_MARK,APLOG_INFO,OK,server,
		     "Passing FramerD server config %s to %s for %s",
		     *scan_config,exename,sockname);
	*write_argv++=(char *)(*scan_config);}
      scan_config++;
      n_configs++;}}
  if (dir_configs) {
    const char **scan_config=dir_configs;
    while (*scan_config) {
      if (n_configs>MAX_CONFIGS) {
	ap_log_error(APLOG_MARK,APLOG_CRIT,OK,server,
		     "Stopped after %d configs!	 Not passing FramerD path config %s",
		     n_configs,*scan_config);}
      else {
	ap_log_error(APLOG_MARK,APLOG_INFO,OK,server,
		     "Passing FramerD path config %s to %s for %s",
		     *scan_config,exename,sockname);
	*write_argv++=(char *)(*scan_config);}
      scan_config++;
      n_configs++;}}
    
  *write_argv++=NULL; n_configs++;
    
  envp=NULL;
  /* Pass the logfile in through the environment, so we can
     use it to record processing of config variables */
  if (log_file) {
    if (n_configs>MAX_CONFIGS) {
      ap_log_error(APLOG_MARK,APLOG_CRIT,OK,server,
		   "Stopped after %d configs!  Not passing LOGFILE=%s",
		   n_configs,log_file);}
    else {
      char *env_entry=apr_psprintf(p,"LOGFILE=%s",log_file);
      envp=write_argv;
      *write_argv++=(char *)env_entry;
      n_configs++;}}
  if (log_sync) {
    if (n_configs>MAX_CONFIGS) {
      ap_log_error(APLOG_MARK,APLOG_CRIT,OK,server,
		   "Stopped after %d configs!  Not passing LOGFILE=%s",
		   n_configs,log_file);}
    else {
      if (n_configs>MAX_CONFIGS) {
	ap_log_error(APLOG_MARK,APLOG_CRIT,OK,server,
		     "Stopped after %d configs! Not passing LOGSYNC",n_configs);}
      else {
	char *env_entry=apr_pstrdup(p,"LOGSYNC=yes");
	if (!(envp)) envp=write_argv;
	*write_argv++=(char *)env_entry;}}}
    
  if (server_env) {
    const char **scan_env=server_env;
    while (*scan_env) {
      if (n_configs>MAX_CONFIGS) {
	ap_log_error(APLOG_MARK,APLOG_CRIT,OK,server,
		     "Stopped after %d configs, not passing server env %s!",
		     n_configs,*scan_env);}
      else {
	ap_log_error(APLOG_MARK,APLOG_INFO,OK,server,
		     "Passing server environment %s to %s for %s",
		     *scan_env,exename,sockname);
	if (!(envp)) envp=write_argv;
	*write_argv++=(char *)(*scan_env);}
      scan_env++;
      n_configs++;}}
    
  if (dir_env) {
    const char **scan_env=dir_configs;
    while (*scan_env) {
      if (n_configs>MAX_CONFIGS) {
	ap_log_error(APLOG_MARK,APLOG_CRIT,OK,server,
		     "Stopped after %d configs! Not passing path ENV %s",
		     n_configs,*scan_env);}
      else {
	ap_log_error(APLOG_MARK,APLOG_INFO,OK,server,
		     "Passing path ENV %s to %s for %s",
		     *scan_env,exename,sockname);
	if (!(envp)) envp=write_argv;
	*write_argv++=(char *)(*scan_env);}
      scan_env++;
      n_configs++;}}
    
  *write_argv++=NULL;
    
  /* Make sure the socket is writable, creating directories
     if needed. */
  check_directory(p,sockname);
    
  if (((rv=apr_procattr_create(&attr,p)) != APR_SUCCESS) ||
      ((rv=apr_procattr_cmdtype_set(attr,APR_PROGRAM)) != APR_SUCCESS) ||
      ((rv=apr_procattr_detach_set(attr,1)) != APR_SUCCESS) ||
      ((rv=apr_procattr_dir_set(attr,ap_make_dirstr_parent(p,r->filename)))
       != APR_SUCCESS))
    ap_log_rerror
      (APLOG_MARK, APLOG_ERR, rv, r,
       "couldn't set child process attributes: %s", r->filename);
  else ap_log_rerror
	 (APLOG_MARK, LOGDEBUG, rv, r,
	  "Successfully set child process attributes: %s", r->filename);
    
  {
    const char **scanner=argv; while (scanner<write_argv) {
      if ((envp) && (scanner>=envp))
	ap_log_rerror(APLOG_MARK, LOGDEBUG, OK, r,
		      "%s ENV[%ld]='%s'",
		      exename,((long int)(scanner-argv)),*scanner);
      else ap_log_rerror(APLOG_MARK, LOGDEBUG, OK, r,
			 "%s ARG[%ld]='%s'",
			 exename,((long int)(scanner-argv)),*scanner);
      scanner++;}}
    
  if ((stat(sockname,&stat_data) == 0)&&
      (((time(NULL))-stat_data.st_mtime)>15)) {
    if (remove(sockname) == 0)
      ap_log_error(APLOG_MARK,APLOG_NOTICE,OK,server,
		   "Removed leftover socket file %s",sockname);
    else {
      ap_log_error(APLOG_MARK,APLOG_CRIT,apr_get_os_error(),server,
		   "Could not remove socket file %s",sockname);
      apr_file_remove(lockname,p);
      return -1;}}
    
  errno=0;
  rv=apr_proc_create(&proc,exename,(const char **)argv,envp,
		     attr,p);
  if (rv!=APR_SUCCESS) {
    ap_log_rerror(APLOG_MARK,APLOG_EMERG, rv, r,
		  "Couldn't spawn %s @%s for %s [rv=%d,pid=%d,uid=%d,gid=%d]",
		  exename,sockname,r->unparsed_uri,rv,proc.pid,uid,gid);
    /* We don't exit right away because there might be a race condition
       so another process is creating the socket.  So we still wait. */
    retval=-1;}
  else if (log_file)
    ap_log_error
      (APLOG_MARK,APLOG_NOTICE,rv,server,
       "Spawned %s @%s (logfile=%s) for %s [rv=%d,pid=%d,uid=%d,uid=%d]",
       exename,sockname,log_file,r->unparsed_uri,rv,
       proc.pid,uid,gid);
  else ap_log_error(APLOG_MARK,APLOG_NOTICE,rv,server,
		    "Spawned %s @%s (nolog) for %s [rv=%d,pid=%d,uid=%d,gid=%d]",
		    exename,sockname,r->unparsed_uri,rv,proc.pid,uid,gid);
    
  /* Now wait for the socket file to exist */
  {
    int sleep_count=1;
    sleep(1); while ((rv=stat(sockname,&stat_data)) < 0) {
      if (sleep_count>servlet_wait) {
	ap_log_rerror(APLOG_MARK,APLOG_EMERG,apr_get_os_error(),r,
		      "Failed to spawn socket file %s (i=%d/wait=%d) (%d:%s)",
		      sockname,sleep_count,servlet_wait,
		      errno,strerror(errno));
	errno=0;
	if (unlock) apr_file_unlock(lockfile);
	apr_file_remove(lockname,p);
	return -1;}
      if (((sleep_count+1)%4)==0) {
	ap_log_rerror
	  (APLOG_MARK,APLOG_NOTICE,OK,r,
	   "Still waiting for %s to exist (i=%d/wait=%d) (errno=%d:%s)",
	   sockname,sleep_count,servlet_wait,errno,strerror(errno));
	errno=0; sleep(2);}
      else sleep(1);
      sleep_count++;}
    s->spawned=apr_time_now();
    s->spawning=0;}

  if (unlock) apr_file_unlock(lockfile);
  apr_file_remove(lockname,p);

  if (rv>=0) return 1;
  else return retval;
}

static int spawn_wait(fdservlet s,request_rec *r) 
{
  apr_pool_t *p=((r==NULL)?(fdserv_pool):(r->pool));
  const char *sockname=s->sockname;
  struct FDSERV_SERVER_CONFIG *sconfig=
    ap_get_module_config(r->server->module_config,&fdserv_module);
  struct FDSERV_DIR_CONFIG *dconfig=
    ap_get_module_config(r->per_dir_config,&fdserv_module);
  struct stat stat_data; int rv;
  int servlet_wait=dconfig->servlet_wait;
  int sleep_count=1;
  sleep(1); while ((rv=stat(sockname,&stat_data)) < 0) {
      if (sleep_count>servlet_wait) {
	ap_log_rerror(APLOG_MARK,APLOG_EMERG,apr_get_os_error(),r,
		      "Failed to spawn socket file %s (i=%d/wait=%d) (%d:%s)",
		      sockname,sleep_count,servlet_wait,
		      errno,strerror(errno));
	errno=0;
	return -1;}
      if (((sleep_count+1)%4)==0) {
	ap_log_rerror
	  (APLOG_MARK,APLOG_NOTICE,OK,r,
	   "Still waiting for %s to exist (i=%d/wait=%d) (errno=%d:%s)",
	   sockname,sleep_count,servlet_wait,errno,strerror(errno));
	errno=0; sleep(2);}
      else sleep(1);
      sleep_count++;}
  if (rv>=0) return 0;
  else return rv;
}

/* Maintaining the servlet table */

static fdservlet get_servlet(const char *sockname)
{
  int i=0; int lim=n_servlets;
  while (i<lim) {
    if (strcmp(sockname,servlets[i].sockname)==0) return &(servlets[i]);
    else i++;}
  return NULL;
}

/* TODO: This should also handle growing down */
static fdservlet servlet_set_keep_socks(fdservlet s,int keep_socks)
{
  if (s->keep_socks>=keep_socks) return s;
  else {
    apr_thread_mutex_lock(s->lock);
    if (s->keep_socks>=keep_socks) {
      apr_thread_mutex_unlock(s->lock);
      return s;}
    else {
      struct FDSOCKET *fresh=
	apr_pcalloc(fdserv_pool,sizeof(struct FDSOCKET)*keep_socks);
      ap_log_error(APLOG_MARK,APLOG_CRIT,OK,s->server,
		   "Growing socket pool for %s from %d/%d to %d/%d",
		   s->sockname,s->n_socks,s->keep_socks,s->n_socks,keep_socks);
      if (s->n_socks) {
	memcpy(fresh,s->sockets,(sizeof(struct FDSOCKET))*(s->n_socks));}
      if (fresh) {
	memset(fresh+((sizeof(struct FDSOCKET))*(s->keep_socks)),0,
	       ((sizeof(struct FDSOCKET))*(keep_socks-s->n_socks)));
	s->sockets=fresh; s->keep_socks=keep_socks;
	apr_thread_mutex_unlock(s->lock);
	return s;}
      else {
	ap_log_error(APLOG_MARK,APLOG_CRIT,OK,s->server,
		     "Unable to grow socket pool for %s",s->sockname);
	apr_thread_mutex_unlock(s->lock);
	return NULL;}}}
}

/* Adds a servlet for sockname, setup for */
static fdservlet add_servlet(struct request_rec *r,const char *sockname,
			     int keep_socks,int max_socks)
{
  int i=0; int lim=n_servlets;
  apr_thread_mutex_lock(servlets_lock);
  while (i<lim) {
    /* First check (probably again) if it's already there */
    /* If the number of servlets gets big, this could be made into a
       binary search in a custom table. */
    if (strcmp(sockname,servlets[i].sockname)==0) {
      if (servlets[i].keep_socks<keep_socks)
	servlet_set_keep_socks(&(servlets[i]),keep_socks);
      servlets[i].max_socks=max_socks;
      apr_thread_mutex_unlock(servlets_lock);
      return &(servlets[i]);}
    else i++;}
  if (i>=max_servlets) {
    /* Grow the servlets table if needed */
    int old_max=max_servlets;
    int new_max=max_servlets+FDSERV_INIT_SERVLETS;
    struct FDSERVLET *newvec=(struct FDSERVLET *)
      prealloc(fdserv_pool,(char *)servlets,
	       sizeof(struct FDSERVLET)*new_max,
	       sizeof(struct FDSERVLET)*old_max);
    if (newvec) {
      servlets=newvec; max_servlets=new_max;}
    else {
      ap_log_error(APLOG_MARK,APLOG_CRIT,OK,r->server,
		   "Can't grow max servlets (%d) to add %s",
		   max_servlets,sockname);
      apr_thread_mutex_unlock(servlets_lock);
      return NULL;}}
  {
    /* Now, we create the servlet entry itself */
    int isfilesock=((sockname)&&
		    ((strchr(sockname,'@'))==NULL)&&
		    ((strchr(sockname,'/')!=NULL)||
		     ((strchr(sockname,':'))==NULL)));
    fdservlet servlet=&(servlets[i]);
    ap_log_error(APLOG_MARK,APLOG_NOTICE,OK,r->server,
		 "Adding new servlet for %s at #%d, keep=%d, max=%d",
		 sockname,i,keep_socks,max_socks);
    memset(servlet,0,sizeof(struct FDSERVLET));
    servlet->sockname=apr_pstrdup(fdserv_pool,sockname);
    servlet->server=r->server; servlet->servlet_index=i;
    servlet->spawning=0; servlet->spawned=0;
    /* Initialize type and address/endpoint fields */
    if (isfilesock) {
      servlet->socktype=filesock;
      servlet->endpoint.path.sun_family=AF_LOCAL;
      strcpy(servlet->endpoint.path.sun_path,sockname);}
    else {
      apr_status_t retval;
      apr_sockaddr_t *addr;
      char *rname;
      char hostname[128]; int portno=-1;
      char *split=strchr(sockname,'@');
      servlet->socktype=aprsock;
      if (split) {
	char portbuf[32];
	strcpy(hostname,split+1);
	strncpy(portbuf,sockname,split-sockname);
	portbuf[split-sockname]='\0';
	portno=atoi(portbuf);}
      else if ((split=strchr(sockname,':'))) {
	strncpy(hostname,sockname,split-sockname);
	hostname[split-sockname]='\0';
	portno=atoi(split+1);}
      else {}
      if (portno<0) {
	ap_log_rerror
	  (APLOG_MARK,APLOG_WARNING,OK,r,
	   "Unable to determine port from %s",sockname);
	apr_thread_mutex_unlock(servlets_lock);
	return NULL;}
      retval=apr_sockaddr_info_get
	(&(servlet->endpoint.addr),hostname,
	 APR_UNSPEC,(short)portno,0,fdserv_pool);
      if (retval!=APR_SUCCESS) {
	ap_log_rerror
	  (APLOG_MARK,APLOG_WARNING,OK,r,
	   "Unable to resolve connection info for port %d at %s",
	   portno,sockname);
	apr_thread_mutex_unlock(servlets_lock);
	return NULL;}
      else {
	apr_sockaddr_ip_get(&rname,addr);
	ap_log_rerror
	  (APLOG_MARK,APLOG_INFO,OK,r,
	   "Got info for port %d at %s, addr=%s, port=%d",
	   portno,hostname,rname,addr->port);}}
    apr_thread_mutex_create(&(servlet->lock),
			    APR_THREAD_MUTEX_DEFAULT,fdserv_pool);
    servlet->max_socks=max_socks;
    if (keep_socks>=0) {
      servlet_set_keep_socks(servlet,keep_socks);}
    else {servlet->sockets=NULL; servlet->keep_socks=-1;}
    servlet->n_socks=0;
    servlet->n_busy=0;
    servlet->n_ephemeral=0;
    n_servlets++;
    apr_thread_mutex_unlock(servlets_lock);
    return servlet;}
}

/* Getting (and opening) sockets */

static char *fdsocketinfo(fdsocket s,char *buf)
{
  char sockid[256];
  if (!(s)) return "nullsocketarg";
  else if (!(buf)) return "fdsocketinfo:nullbufarg";
  if (s->socket_index>=0)
    sprintf(sockid,"%s(#%d/%d)",
	    s->sockname,s->socket_index,s->servlet->n_socks);
  else sprintf(sockid,"%s(@%lx)",s->sockname,((unsigned long)(s)));
  if (s->socktype==filesock)
    sprintf(buf,"%s file socket %s (fd=%d)",
	    ((s->socket_index>=0)?("cached"):("ephemeral")),
	    sockid,s->conn.fd);
  else sprintf(buf,"%s APR socket %s",
	       ((s->socket_index>=0)?("cached"):("ephemeral")),
	       sockid);
  return buf;
}

static int get_servlet_wait(request_rec *r);

/* Opens a servlet socket, either a cached one (if given != NULL) or a
   new one (malloc) */
static fdsocket servlet_open(fdservlet s,struct FDSOCKET *given,request_rec *r)
{
  struct FDSOCKET *result; apr_pool_t *pool; int one=1, dospawn=-1, wait=-1;
  if (given) pool=fdserv_pool; else pool=r->pool;
  if (given) result=given;
  else {
    result=apr_pcalloc(pool,sizeof(struct FDSOCKET));
    memset(result,0,sizeof(struct FDSOCKET));}

#if DEBUG_CONNECT
  ap_log_rerror
    (APLOG_MARK,LOGDEBUG,OK,r,"Opening new %s socket to %s",
     ((given==NULL)?("ephemeral"):("cached")),
     s->sockname);
#endif
  if (s->socktype==filesock) {
    const char *sockname=s->sockname;
    int unix_sock=socket(PF_LOCAL,SOCK_STREAM,0), connval=-1, rv=-1, intval=1;
    if (unix_sock<0) {
      ap_log_rerror(APLOG_MARK,APLOG_CRIT,apr_get_os_error(),r,
		    "Couldn't open socket for %s (errno=%d:%s)",
		    sockname,errno,strerror(errno));
      return NULL;}
    else ap_log_rerror
	   (APLOG_MARK,LOGDEBUG,OK,r,
	    "Opened socket %d to connect to %s",unix_sock,sockname);
    connval=connect(unix_sock,(struct sockaddr *)&(s->endpoint.path),
		    SUN_LEN(&(s->endpoint.path)));
    if ((connval<0)&&(file_socket_existsp(pool,r->server,sockname))) {
      int wait=get_servlet_wait(r), count=0;
      while ((count<wait)&&(connval<0)) {
	sleep(1); count++;
	connval=connect(unix_sock,(struct sockaddr *)&(s->endpoint.path),
			SUN_LEN(&(s->endpoint.path)));}}
    if (connval<0) {
      ap_log_rerror
	(APLOG_MARK,APLOG_CRIT,apr_get_os_error(),r,
	 "Couldn't connect socket @ %s, spawning fdservlet",sockname);
      errno=0;
      rv=spawn_fdservlet(s,r,fdserv_pool);
      if (rv<0) {
	ap_log_rerror
	  (APLOG_MARK,APLOG_EMERG,apr_get_os_error(),r,
	   "Couldn't spawn fdservlet @ %s",sockname);
	close(unix_sock);
	return NULL;}
      else if (rv)
	ap_log_rerror(APLOG_MARK,APLOG_INFO,OK,r,
		      "Spawn succeeded, waiting to connect to %s",sockname);
      else ap_log_rerror(APLOG_MARK,APLOG_INFO,OK,r,
			 "Waiting to connect to %s",sockname);}
    if ((connval<0)&&(file_socket_existsp(pool,r->server,sockname))) {
      int wait=get_servlet_wait(r), count=0;
      while ((count<wait)&&(connval<0)) {
	sleep(1); count++;
	connval=connect(unix_sock,(struct sockaddr *)&(s->endpoint.path),
			SUN_LEN(&(s->endpoint.path)));}}
    if (connval<0) {
      ap_log_rerror
	(APLOG_MARK,APLOG_CRIT,apr_get_os_error(),r,
	 "Couldn't connect to %s (errno=%d:%s)",
	 sockname,errno,strerror(errno));
      errno=0;
      close(unix_sock);
      return NULL;}
#if DEBUG_CONNECT
    ap_log_rerror
      (APLOG_MARK,LOGDEBUG,OK,r,"Opened new %s file socket (fd=%d) to %s",
       ((given==NULL)?("ephemeral"):("cached")),
       unix_sock,s->sockname);
#endif
    setsockopt(unix_sock,IPPROTO_TCP,TCP_NODELAY,&one,sizeof(int));
    result->servlet=s;
    result->socktype=filesock;
    result->sockname=sockname;
    result->conn.fd=unix_sock;
    result->busy=1; result->closed=0;
    if (!(given)) result->socket_index=-1;
    return result;}
  else if (s->socktype==aprsock) {
    apr_status_t retval;
    apr_socket_t *sock;
    retval=apr_socket_create
      (&sock,APR_INET,SOCK_STREAM,APR_PROTO_TCP,pool);
    if (retval!=APR_SUCCESS) {
      ap_log_rerror
	(APLOG_MARK,APLOG_WARNING,OK,r,
	 "Unable to allocate socket to connect with %s",
	 s->sockname);
      return NULL;}
    retval=apr_socket_connect(sock,s->endpoint.addr);
    if (retval!=APR_SUCCESS) {
      int count=0, wait=get_servlet_wait(r);
      ap_log_rerror(APLOG_MARK,APLOG_WARNING,OK,r,
		    "Unable to make connection to %s",s->sockname);
      while ((count<wait)&&(retval!=APR_SUCCESS)) {
	sleep(1); count++; retval=apr_socket_connect(sock,s->endpoint.addr);}
      if (retval!=APR_SUCCESS) {
	ap_log_rerror(APLOG_MARK,APLOG_WARNING,OK,r,
		      "Timeout making connection to %s",s->sockname);
	apr_socket_close(sock);
	return NULL;}}
    ap_log_rerror
      (APLOG_MARK,LOGDEBUG,OK,r,"Opening new %s APR socket to %s",
       ((given==NULL)?("ephemeral"):("cached")),
       s->sockname);
    result->servlet=s;
    result->socktype=aprsock;
    result->sockname=s->sockname;
    result->conn.apr=sock;
    result->busy=1; result->closed=0;
    /* -1 as a socket index indicates a malloc'd socket */
    if (!(given)) result->socket_index=-1;
    /* If it's an ephemeral socket, record the request */
    return result;}
  else {
    ap_log_rerror(APLOG_MARK,APLOG_CRIT,OK,r,"Bad servlet arg");}
  return NULL;
}

static int get_servlet_wait(request_rec *r)
{
  struct FDSERV_SERVER_CONFIG *sconfig=
    ap_get_module_config(r->server->module_config,&fdserv_module);
  struct FDSERV_DIR_CONFIG *dconfig=
    ap_get_module_config(r->per_dir_config,&fdserv_module);
  if (dconfig->servlet_wait>=0) return dconfig->servlet_wait;
  else if (sconfig->servlet_wait>=0) return sconfig->servlet_wait;
  else return DEFAULT_SERVLET_WAIT;
}

static fdsocket servlet_connect(fdservlet s,request_rec *r)
{
  char infobuf[512];
  apr_thread_mutex_lock(s->lock); {
    int i=0; int lim=s->n_socks; int closed=-1;
    if (s->n_socks>s->n_busy) {
      /* There should be a free open socket to reuse, so we scan */
      struct FDSOCKET *sockets=s->sockets;
      while (i<lim) {
	ap_log_error(APLOG_MARK,LOGDEBUG,OK,s->server,
		     "Checking %s #%d/%d busy=%d closed=%d",
		     s->sockname,i,lim,sockets[i].busy,sockets[i].closed);
	if (sockets[i].busy>0) i++;
	else if (sockets[i].closed) {
	  if (closed<0) closed=i++;
	  else i++;}
	else {
	  sockets[i].busy=1;
	  s->n_busy++;
	  apr_thread_mutex_unlock(s->lock);
#if DEBUG_SOCKETS
	  ap_log_rerror(APLOG_MARK,APLOG_WARNING,OK,r,"Using cached %s",
			fdsocketinfo(&(sockets[i]),infobuf));
#endif
	  return &(sockets[i]);}}}
    /* All allocated sockets are busy, so we just go to the end of the
       opened sockets vector to (probably) use a new one. */
    else i=s->n_socks;
    if (closed>=0) {
      /* If there's a closed socket < i, try to use that. */
      struct FDSOCKET *sockets=s->sockets; fdsocket sock;
      ap_log_rerror(APLOG_MARK,LOGSOCKET,OK,r,"Reopening %s",
		    fdsocketinfo(&(sockets[closed]),infobuf));
      sock=servlet_open(s,&(sockets[closed]),r); 
      if (sock) s->n_busy++;
      apr_thread_mutex_unlock(s->lock);
      return sock;}
    if (i>=s->keep_socks) {
      /* You're past the number of sockets to keep open, so either
	 open an ephemeral one or give up */
      if ((s->max_socks>0)&&(s->n_busy>=s->max_socks)) {
	ap_log_error(APLOG_MARK,APLOG_CRIT,OK,s->server,
		     "Reached max sockets on %s busy=%d>%d=max",
		     s->sockname,s->n_busy,s->max_socks);
	/* Give up if you're over max_socks */
	apr_thread_mutex_unlock(s->lock);
	return NULL;}
      else {
	/* Open an ephemeral socket */
	fdsocket sock=servlet_open(s,NULL,r);
	if (sock) {s->n_ephemeral++; s->n_busy++;}
#if DEBUG_SOCKETS
	ap_log_rerror
	  (APLOG_MARK,LOGNOTICE,OK,r,
	   "Using ephemeral (#%d) %s because all %d cached sockets are busy",
	   s->n_ephemeral,
	   fdsocketinfo(sock,infobuf),
	   s->keep_socks);
#endif
	apr_thread_mutex_unlock(s->lock);
	return sock;}}
    else {
      /* i should be the same as n_socks, so we try to open that socket. */
      struct FDSOCKET *sockets=s->sockets;
      fdsocket sock=servlet_open(s,&(sockets[i]),r);
      if (sock) {
	s->n_socks++; s->n_busy++; sock->socket_index=i;
	apr_thread_mutex_unlock(s->lock);
#if DEBUG_SOCKETS
	ap_log_rerror(APLOG_MARK,APLOG_WARNING,OK,r,
		      "New %s",fdsocketinfo(sock,infobuf));
#else
	ap_log_rerror(APLOG_MARK,LOGINFO,OK,r,
		      "New %s",fdsocketinfo(sock,infobuf));
#endif
	return sock;}
      else {
	/* Failed for some reason, open an excess socket */
	ap_log_rerror(APLOG_MARK,APLOG_WARNING,OK,r,
		      "Error opening %s %s, trying ephemeral",
		      ((s->socktype==filesock)?("servlet file socket"):
		       (s->socktype==aprsock)?("servlet APR socket"):
		       ("servlet bad socket")),
		      s->sockname);
	s->n_ephemeral++; s->n_busy++;
	apr_thread_mutex_unlock(s->lock);
	sock=servlet_open(s,NULL,r);
	if (sock==NULL) {
	  /* If the socket open failed, relock the servlet structure
	     and decrement the busy/emphemeral counters */
	  apr_thread_mutex_lock(s->lock);
	  s->n_ephemeral--; s->n_busy--;
	  apr_thread_mutex_unlock(s->lock);
	  ap_log_rerror(APLOG_MARK,APLOG_WARNING,OK,r,
			"Ephemeral open failed (n=%d) for %s",
			s->n_ephemeral,r->uri);}
	else ap_log_rerror(APLOG_MARK,LOGSOCKET,OK,r,
			   "Ephemeral (#%d) %s opened for %s",
			   s->n_ephemeral,
			   fdsocketinfo(sock,infobuf),
			   r->uri);
	return sock;}}}
}

static int servlet_recycle_socket(fdservlet servlet,fdsocket sock)
{
  char infobuf[256];
  if (!(sock->servlet)) {
    ap_log_error
      (APLOG_MARK,APLOG_ERR,OK,servlet->server,
       "Internal error, recycling socket (for %s) without a servlet",
       sock->sockname);
    return -1;}
  else if (sock->servlet!=servlet) {
    ap_log_error
      (APLOG_MARK,APLOG_ERR,OK,servlet->server,
       "Internal error, recycling socket (for %s:%s) to wrong servlet (%s)",
       sock->sockname,sock->servlet->sockname,servlet->sockname);
    return -1;}
  else if (sock->socket_index<0) {
    int busy=0, ephemeral=0;
    apr_thread_mutex_lock(servlet->lock);
    if (sock->busy) {
      sock->busy=0; servlet->n_busy--;}
    servlet->n_ephemeral--;
    busy=servlet->n_busy; ephemeral=servlet->n_ephemeral;
    apr_thread_mutex_unlock(servlet->lock);
#if DEBUG_SOCKETS
    ap_log_error(APLOG_MARK,LOGDEBUG,OK,servlet->server,
		 "Closing %s, %d ephemeral, %d busy",
		 fdsocketinfo(sock,infobuf),
		 ephemeral,busy);
#endif
    if (sock->socktype==aprsock) apr_socket_close(sock->conn.apr);
    else if (sock->socktype==filesock) close(sock->conn.fd);
    else {}
    memset(sock,0,sizeof(struct FDSOCKET));
    return 0;}
  else {
    int i=sock->socket_index;
    if (sock!=(&(servlet->sockets[i]))) {
      /* The sockets array was reallocated, so sock is different than
	 it was */
      sock=&(servlet->sockets[i]);}
    apr_thread_mutex_lock(servlet->lock);
    if (sock->busy) {
      sock->busy=0; servlet->n_busy--;}
    apr_thread_mutex_unlock(servlet->lock);
#if DEBUG_SOCKETS
    if (sock->socktype==filesock)
      ap_log_error(APLOG_MARK,LOGDEBUG,OK,servlet->server,
		   "Recycling cached file socket #%d (fd=%d) for use with %s",
		   sock->socket_index,sock->conn.fd,servlet->sockname);
    else ap_log_error(APLOG_MARK,LOGDEBUG,OK,servlet->server,
		      "Recycling cached socket #%d for use with %s",
		      sock->socket_index,servlet->sockname);
#endif
    return 1;}
}

static int servlet_close_socket(fdservlet servlet,fdsocket sock)
{
  char infobuf[256];
  if (sock->servlet!=servlet) {
    ap_log_error
      (APLOG_MARK,APLOG_ERR,OK,servlet->server,
       "Internal error, closed socket (for %s) recycled to wrong servlet (%s)",
       sock->sockname,servlet->sockname);
    return -1;}
  apr_thread_mutex_lock(servlet->lock);
#if DEBUG_SOCKETS
  ap_log_error(APLOG_MARK,LOGDEBUG,OK,servlet->server,
	       "Closing %s",fdsocketinfo(sock,infobuf));
#endif
  if ((sock->socket_index>=0)&&
      (sock!=(&(servlet->sockets[sock->socket_index])))) {
    int i=sock->socket_index;
    /* The sockets array was reallocated, so sock is different than it was */
    sock=&(servlet->sockets[i]);}
  if (sock->busy) servlet->n_busy--;
  sock->busy=0;
  if (sock->socktype==filesock) {
    int rv=close(sock->conn.fd);
    if (rv<0)
      ap_log_error(APLOG_MARK,LOGDEBUG,rv,servlet->server,
		   "Error (%s) closing %s",strerror(errno),
		   fdsocketinfo(sock,infobuf));
    sock->conn.fd=-1; errno=0;}
  else if (sock->socktype==aprsock) {
    apr_status_t rv=apr_socket_close(sock->conn.apr);
    if (rv!=OK) 
      ap_log_error(APLOG_MARK,LOGDEBUG,rv,servlet->server,
		   "Error (%s) closing %s",strerror(errno),
		   fdsocketinfo(sock,infobuf));
    sock->conn.apr=NULL;}
  else {}
  sock->closed=1;
  if (sock->socket_index<0) servlet->n_ephemeral--;
  apr_thread_mutex_unlock(servlet->lock);
  return 1;
}

/* Connecting to the servlet */

static fdservlet request_servlet(request_rec *r)
{
  const char *sockname=get_sockname(r);
  struct FDSERV_SERVER_CONFIG *sconfig=
    ap_get_module_config(r->server->module_config,&fdserv_module);
  struct FDSERV_DIR_CONFIG *dconfig=
    ap_get_module_config(r->per_dir_config,&fdserv_module);
  fdservlet servlet;
  int keep_socks=sconfig->keep_socks, max_socks=sconfig->max_socks;
  ap_log_rerror(APLOG_MARK,APLOG_INFO,OK,r,
		"Resolving %s using servlet %s",r->unparsed_uri,sockname);
  servlet=get_servlet(sockname);
  if ((dconfig)&&(dconfig->keep_socks>keep_socks))
    keep_socks=dconfig->keep_socks;
  if ((dconfig)&&(dconfig->max_socks>max_socks))
    max_socks=dconfig->max_socks;
  /* Get valid values by using defaults.  Note that zero is a valid
     value for keep socks (no socket cache), but not for max socks.  */
  if (keep_socks<0) keep_socks=DEFAULT_KEEP_SOCKS;
  if (max_socks<=0) max_socks=DEFAULT_MAX_SOCKS;
  if (servlet) {
#if DEBUG_FDSERV
    ap_log_rerror(APLOG_MARK,LOGDEBUG,OK,r,
		  "Found existing servlet @#%d for use with %s",
		  servlet->servlet_index,servlet->sockname);
#endif
    servlet_set_keep_socks(servlet,keep_socks);
    servlet->max_socks=max_socks;
    return servlet;}
  else {
    servlet=add_servlet(r,sockname,keep_socks,max_socks);
    return servlet;}
}

/* Cleaning up servlets */

static apr_status_t close_servlets(void *data)
{
  apr_pool_t *p=(apr_pool_t *)data;
  int i=0; int lim; int sock_count=0;
  apr_thread_mutex_lock(servlets_lock);
  lim=n_servlets;
  ap_log_perror(APLOG_MARK,APLOG_INFO,OK,p,
		"mod_fdserv closing %d open servlets",lim);
  while (i<lim) {
    fdservlet s=&(servlets[i++]);
    int j=0, n_socks;
    struct FDSOCKET *sockets;
    apr_thread_mutex_lock(s->lock);
    sockets=s->sockets; n_socks=s->n_socks;
    while (j<n_socks) {
      fdsocket sock=&(sockets[j++]);
      if (sock->closed) continue;
      sock_count++;
      if (sock->socktype==filesock) close(sock->conn.fd);
      else if (sock->socktype==aprsock)
	apr_socket_close(sock->conn.apr);
      else {}
      sock->closed=1; sock->busy=1;}
    apr_thread_mutex_unlock(s->lock);}
  ap_log_perror(APLOG_MARK,APLOG_INFO,OK,p,
		"mod_fdserv closed %d open sockets across %d servlets",
		sock_count,lim);
  apr_thread_mutex_unlock(servlets_lock);
  return OK;
}

/* Writing DTypes to BUFFs */

#define ARBITRARY_BUFLIM (8*65536)

/* In Apache 2.0, BUFFs seem to be replaced by buckets in brigades,
    but that seems a little overhead heavy for the output buffers used
    here, which are just used to write slotmaps to servlets.  So a
    simple reimplementation is done here.  */
typedef struct BUFF {
  apr_pool_t *p; unsigned char *buf, *ptr, *lim;} BUFF;
static int ap_bneeds(BUFF *b,size_t n)
{
  if ((b->ptr+n)>=b->lim) {
    size_t old_size=b->lim-b->buf, off=b->ptr-b->buf;
    size_t need_off=off+n, new_size=old_size;
    unsigned char *nbuf;
    while (need_off>new_size) {
      if (new_size>=ARBITRARY_BUFLIM) {
	new_size=ARBITRARY_BUFLIM*(2+(need_off/ARBITRARY_BUFLIM));
	break;}
      else new_size=new_size*2;}
    nbuf=apr_pcalloc(b->p,new_size);
    if (!(nbuf)) return -1;
    memcpy(nbuf,b->buf,off);
    b->buf=nbuf; b->ptr=nbuf+off; b->lim=nbuf+new_size;
    return new_size;}
  else return b->lim-b->buf;
}

static int ap_bputc(unsigned char c,BUFF *b)
{
  if (ap_bneeds(b,1)<0) return -1; 
  *(b->ptr++)=c;
  return 1;
}
static int ap_bputs(char *string,BUFF *b)
{
  int len=strlen(string);
  if (ap_bneeds(b,len+1)<0) return -1; 
  memcpy((char *)b->ptr,string,len); b->ptr=b->ptr+len;
  return len;
}
static int ap_bwrite(BUFF *b,char *string,int len)
{
  if (ap_bneeds(b,len+1)<0) return -1; 
  memcpy((char *)b->ptr,string,len); b->ptr=b->ptr+len;
  return len;
}
static BUFF *ap_bcreate(apr_pool_t *p,int ignore_flag)
{
  struct BUFF *b=apr_palloc(p,sizeof(struct BUFF));
  if (!(b)) return b;
  b->p=p; b->ptr=b->buf=apr_palloc(p,4096); b->lim=b->buf+4096;
  return b;
}

static int buf_write_4bytes(unsigned int i,BUFF *b)
{
  if (ap_bneeds(b,4)<0) return -1; 
  ap_bputc((i>>24),b);	     ap_bputc(((i>>16)&0xFF),b);
  ap_bputc(((i>>8)&0xFF),b); ap_bputc((i&0xFF),b);
  return 4;
}

static int buf_write_string(char *string,BUFF *b)
{
  int len=strlen(string);
  if (ap_bneeds(b,len+5)<0) return -1; 
  ap_bputc(0x06,b); buf_write_4bytes(len,b);
  ap_bwrite(b,string,len);
  return len+5;
}

static int buf_write_symbol(char *string,BUFF *b)
{
  int len=strlen(string);
  if (ap_bneeds(b,len+5)<0) return -1; 
  ap_bputc(0x07,b); buf_write_4bytes(len,b);
  ap_bwrite(b,string,len);
  return len+5;
}

static int write_cgidata
  (request_rec *r,BUFF *b,
   apr_table_t *t,const char **sparams,const char **dparams,
   int post_size,char *post_data)
{
  const apr_array_header_t *ah=apr_table_elts(t);
  int n_elts=ah->nelts, n_params=0, n_sparams=0, n_dparams=0;
  apr_table_entry_t *scan=(apr_table_entry_t *) ah->elts, *limit=scan+n_elts;
  ssize_t n_bytes=6, delta=0;
  if (sparams) {
    const char **pscan=sparams; while (*pscan) {
      pscan=pscan+2; n_sparams++; n_params++;};}
  if (dparams) {
    const char **pscan=dparams;
    while (*pscan) {
	pscan=pscan+2; n_dparams++; n_params++;};}
#if DEBUG_CGIDATA
  if (post_size)
    ap_log_error
      (APLOG_MARK,LOGDEBUG,OK,
       r->server,"CGIDATA: %d slots=%d HTTP+%d config; %d post bytes",
       n_params+n_elts+1,n_elts,n_params,post_size);
  else ap_log_error
	 (APLOG_MARK,LOGDEBUG,OK,
	  r->server,"CGIDATA: - %d slots= %d HTTP+%d config",
	  n_params+n_elts+1,n_elts,n_params);
#endif
  if (ap_bneeds(b,6)<0) return -1; 
  ap_bputc(0x42,b); ap_bputc(0xC1,b);
  if (post_size) buf_write_4bytes((n_elts*2)+(n_params*2)+2,b);
  else buf_write_4bytes((n_elts*2)+(n_params*2),b);
  while (scan < limit) {
#if DEBUG_CGIDATA
    ap_log_error
      (APLOG_MARK,LOGDEBUG,OK,r->server,"CGIDATA %s=%s",scan->key,scan->val);
#endif
    if ((delta=buf_write_symbol(scan->key,b))<0) return -1;
    n_bytes=n_bytes+delta;
    if ((delta=buf_write_string(scan->val,b))<0) return -1;
    n_bytes=n_bytes+delta;
#if DEBUG_FDSERV
    ap_log_error
      (APLOG_MARK,LOGDEBUG,OK,
       r->server,"Buffered HTTP request data %s=%s",
       scan->key,scan->val);
#endif
    scan++;}
  if (sparams) {
    const char **pscan=sparams, **plimit=pscan+n_sparams;
    while (pscan<plimit) {
      if ((delta=buf_write_symbol((char *)(pscan[0]),b))<0) return -1;
      n_bytes=n_bytes+delta;
      if ((delta=buf_write_string((char *)(pscan[1]),b))<0) return -1;
      n_bytes=n_bytes+delta;
#if DEBUG_FDSERV
      ap_log_error(APLOG_MARK,LOGDEBUG,OK,r->server,
		   "Buffered servlet parameter %s=%s",
		   pscan[0],pscan[1]);
#endif
      pscan=pscan+2;}}
  if (dparams) {
    const char **pscan=dparams, **plimit=pscan+n_dparams;
    while (pscan<plimit) {
      if ((delta=buf_write_symbol((char *)(pscan[0]),b))<0) return -1;
      n_bytes=n_bytes+delta;
      if ((delta=buf_write_string((char *)(pscan[1]),b))<0) return -1;
      n_bytes=n_bytes+delta;
#if DEBUG_FDSERV
      ap_log_error
	(APLOG_MARK,LOGDEBUG,OK,
	 r->server,"Buffered servlet parameter %s=%s",
	 pscan[0],pscan[1]);
#endif
      pscan=pscan+2;}}
  if (post_size) {
    if ((delta=buf_write_symbol("POST_DATA",b))<0) return -1;
    n_bytes=n_bytes+delta;
    if (ap_bneeds(b,post_size+5)<0) return -1; 
    ap_bputc(0x05,b); buf_write_4bytes(post_size,b);
    ap_bwrite(b,post_data,post_size);
    n_bytes=n_bytes+post_size+5;}
#if DEBUG_CGIDATA
  if (post_size)
    ap_log_error
      (APLOG_MARK,LOGDEBUG,OK,
       r->server,"CGIDATA: %lu bytes/%lu posted/%d slots= %d HTTP/%d config",
       (long)n_bytes,(long)post_size,n_params+n_elts+1,n_elts,n_params);
  else ap_log_error
      (APLOG_MARK,LOGDEBUG,OK,
       r->server,"CGIDATA: %lu bytes/%d slots= %d HTTP/%d config",
       (long)n_bytes,n_params+n_elts+1,n_elts,n_params);
#endif
  return n_bytes;
}

/* Handling fdserv requests */

struct HEAD_SCANNER {
  fdsocket sock;
  request_rec *req;};

static int scan_fgets(char *buf,int n_bytes,void *stream)
{
  struct HEAD_SCANNER *scan=(struct HEAD_SCANNER *)stream;
  if (scan->sock->socktype==aprsock) {
    apr_socket_t *sock=scan->sock->conn.apr;
    request_rec *r=scan->req;
    /* This should use bigger chunks */
    char bytes[1], *write=buf, *limit=buf+n_bytes; 
    size_t bytes_read=1;
    apr_status_t rv=apr_socket_recv(sock,bytes,&bytes_read);
    while ((bytes_read>0)&&(rv==OK)) {
      if (write>=limit) {}
      else *write++=bytes[0];
      if (bytes[0] == '\n') break;
      rv=apr_socket_recv(sock,bytes,&bytes_read);}
    *write='\0';
#if DEBUG_FDSERV
    ap_log_error
      (APLOG_MARK,LOGDEBUG,rv,r->server,
       "mod_fdserv/scan_fgets: Read header string %s from APR socket",buf);
#endif
    if (write>=limit) return write-buf;
    else return write-buf;}
  else if (scan->sock->socktype==filesock) {
    int sock=scan->sock->conn.fd, bytes_read=0;
    request_rec *r=scan->req;
    char bytes[1], *write=buf, *limit=buf+n_bytes;
    while ((write<limit)&&((read(sock,bytes,1))>0)) { 
      if (write>=limit) {}
      else *write++=bytes[0];
      if (bytes[0]=='\n') break;}
    *write='\0';
    ap_log_error
      (APLOG_MARK,APLOG_WARNING,OK,r->server,
       "mod_fdserv/scan_fgets: Read header string %s from %d",buf,sock);
    if (write>=limit) return write-buf;
    else return write-buf;}
  else {
    return -1;}
}

static int sock_write(request_rec *r,
		      const unsigned char *buf,
		      long int n_bytes,
		      fdsocket sockval)
{
  char infobuf[256];
#if DEBUG_TRANSPORT
  ap_log_rerror
    (APLOG_MARK,LOGDEBUG,OK,r,
     "Writing %ld bytes to %s for %s (%s)",
     n_bytes,fdsocketinfo(sockval,infobuf),
     r->unparsed_uri,r->filename);
#endif

  if (sockval->socktype==aprsock) {
    apr_socket_t *sock=sockval->conn.apr;
    apr_size_t bytes_to_write=n_bytes, block_size=n_bytes;
    apr_ssize_t bytes_written=0;
    while (bytes_written < n_bytes) {
      /* Since we haven't called apr_socket_timeout_set, this call
	 will block, which is ok here. */
      apr_status_t rv=apr_socket_send
	(sock,(char *)(buf+bytes_written),&block_size);
      if (rv!=APR_SUCCESS) {
	char errbuf[256]="unknown", *err;
	err=apr_strerror(rv,errbuf,256);
	ap_log_rerror
	  (APLOG_MARK,LOGDEBUG,OK,r,
	   "Error (%s) from %s after %ld=%ld-%ld bytes for %s (%s)",
	   errbuf,fdsocketinfo(sockval,infobuf),
	   (long int)bytes_written,
	   (long int)n_bytes,
	   (long int)bytes_to_write,
	   r->unparsed_uri,r->filename);
	if (bytes_written<n_bytes) return -1;
	else break;}
      else if (block_size == 0) {
	ap_log_rerror
	  (APLOG_MARK,LOGDEBUG,OK,r,
	   "Zero blocks after %ld/%ld on %s for %s (%s)",
	   ((long int)bytes_written),((long int)n_bytes),
	   fdsocketinfo(sockval,infobuf),
	   r->unparsed_uri,r->filename);
	if (bytes_written<n_bytes) return -1;
	else break;}
      else {
	bytes_written=bytes_written+block_size;
	bytes_to_write=bytes_to_write-bytes_written;
#if DEBUG_TRANSPORT
	ap_log_error
	  (APLOG_MARK,LOGDEBUG,OK,
	   r->server,"Wrote %ld more bytes (%ld/%ld) to %s for %s (%s)",
	   block_size,bytes_written,n_bytes,
	   fdsocketinfo(sockval,infobuf),
	   r->unparsed_uri,r->filename);
#endif
	block_size=bytes_to_write;}}
    return bytes_written;}
  else if (sockval->socktype==filesock) {
    int sock=sockval->conn.fd;
    long int bytes_written=0, bytes_to_write=n_bytes;
#if DEBUG_TRANSPORT
    ap_log_error
      (APLOG_MARK,LOGDEBUG,OK,r->server,
       "Writing %ld bytes to %s for %s (%s)",
       bytes_to_write,fdsocketinfo(sockval,infobuf),
       r->unparsed_uri,r->filename);
#endif
    while (bytes_written < n_bytes) {
      int block_size=write(sock,buf+bytes_written,n_bytes-bytes_written);
      if (block_size<0) {
	/* Need to get this to work */
	if (errno==EPIPE) {
	  int olderr=errno;
	  fdsocket val;
	  errno=0; val=servlet_open(sockval->servlet,sockval,r);
	  if (val) {
	    if (val->conn.fd==sock) 
	      ap_log_rerror
		(APLOG_MARK,APLOG_WARNING,OK,r,
		 "Reopened %s, continuing output (%ld/%ld so far) for %s (%s)",
		 fdsocketinfo(sockval,infobuf),
		 bytes_written,n_bytes,
		 r->unparsed_uri,r->filename);
	    else {
	      int new_sock=val->conn.fd;
	      bytes_written=0; 
	      ap_log_rerror
		(APLOG_MARK,APLOG_WARNING,OK,r,
		 "Reopened %s, resetting output of %ld bytes for %s (%s)",
		 fdsocketinfo(sockval,infobuf),
		 n_bytes,r->unparsed_uri,r->filename);
	      sock=new_sock;}
	    continue;}
	  else {
	    ap_log_rerror
	      (APLOG_MARK,APLOG_ERR,OK,r,
	       "Couldn't reopen %s: (%d:%s) (%d:%s) for %s (%s)",
	       fdsocketinfo(sockval,infobuf),
	       olderr,strerror(olderr),errno,strerror(errno),
	       r->unparsed_uri,r->filename);
	    bytes_written=0;}
	  errno=0;}
	else {
	  ap_log_rerror
	    (APLOG_MARK,LOGDEBUG,OK,r,
	     "Error %d (%s) on %s after %ld/%ld bytes for %s (%s)",
	     errno,strerror(errno),
	     fdsocketinfo(sockval,infobuf),
	     bytes_written,n_bytes,
	     r->unparsed_uri,r->filename);
	  if (bytes_written<n_bytes) return -1;
	  else break;}}
      else if (block_size == 0) {
	ap_log_rerror
	  (APLOG_MARK,LOGDEBUG,OK,r,
	   "Zero blocks written to %s after %ld/%ld for %s (%s)",
	   fdsocketinfo(sockval,infobuf),
	   (long int)bytes_written,n_bytes,
	   r->unparsed_uri,r->filename);
	if (bytes_written<n_bytes) return -(bytes_written+1);
	else break;}
      else {
#if DEBUG_TRANSPORT
	ap_log_rerror
	  (APLOG_MARK,LOGDEBUG,OK,r,"Wrote %ld bytes to %s for %s (%s)",
	   (long int)block_size,fdsocketinfo(sockval,infobuf),
	   r->unparsed_uri,r->filename);
#endif
	bytes_written=bytes_written+block_size;}}
    if (bytes_written!=n_bytes) 
      ap_log_rerror(APLOG_MARK,(APLOG_CRIT),
		    OK,r,"Wrote %ld/%ld bytes to %s for %s (%s)",
		    ((long int)bytes_written),n_bytes,
		    fdsocketinfo(sockval,infobuf),
		    r->unparsed_uri,r->filename);
#if DEBUG_TRANSPORT
    ap_log_rerror(APLOG_MARK,APLOG_DEBUG,
		  OK,r,"Wrote %ld/%ld bytes to %s for %s (%s)",
		  ((long int)bytes_written),n_bytes,
		  fdsocketinfo(sockval,infobuf),
		  r->unparsed_uri,r->filename);
#endif
    return bytes_written;}
  else {
    ap_log_rerror
      (APLOG_MARK,APLOG_CRIT,apr_get_os_error(),r,"Bad fdsocket passed");
    return -1;}
}

static int copy_servlet_output(fdsocket sockval,request_rec *r)
{
  char infobuf[256];
  char buf[4096]; apr_size_t bytes_read=0, bytes_written=0; int error=0, rv=0;
  apr_table_t *headers=r->headers_out;
  const char *clength_string=apr_table_get(headers,"Content-Length");
  long int content_length=((clength_string)?(atoi(clength_string)):(-1));
  if (content_length>=0) ap_set_content_length(r,content_length);
  if (content_length<0)
    ap_log_rerror
      (APLOG_MARK,LOGDEBUG,OK,r,
       "Returning some number of content bytes from %s for %s (%s)",
       fdsocketinfo(sockval,infobuf),
       r->unparsed_uri,r->filename);
#if DEBUG_TRANSPORT
  else ap_log_rerror
	 (APLOG_MARK,LOGDEBUG,OK,r,
	  "Returning %ld content bytes from %s for %s (%s)",
	  content_length,fdsocketinfo(sockval,infobuf),
	  r->unparsed_uri,r->filename);
#endif
  if (sockval->socktype==aprsock) {
    apr_socket_t *sock=sockval->conn.apr;
    while ((content_length<0)||(bytes_read<content_length)) {
      apr_size_t delta=4096, written=0;
      apr_status_t rv;
      if (r->connection->aborted) {
	ap_log_rerror(APLOG_MARK,APLOG_WARNING,OK,r,
		      "Connection aborted for %s",r->uri);
	return -1;}
      else rv=apr_socket_recv(sock,buf,&delta);
      if (rv!=OK) {
	ap_log_rerror
	  (APLOG_MARK,LOGDEBUG,rv,r,
	   "Stopped after reading %ld/%ld bytes from %s for %s (%s)",
	   (long int)bytes_read,content_length,
	   fdsocketinfo(sockval,infobuf),
	   r->unparsed_uri,r->filename);
	error=1;
	break;}
      else if (delta>0) {
	int chunk=ap_rwrite(buf,delta,r); written=0;
	bytes_read=bytes_read+delta;
	while (written<delta) {
	  if (chunk<0) break;
	  written=written+chunk;
	  chunk=ap_rwrite(buf+written,delta-written,r);}
	if (chunk<0) {error=1; break;}}
      else {error=1; break;}
      if (written<=0) {
	ap_log_rerror
	  (APLOG_MARK,APLOG_ERR,OK,r,
	   "Write error after %ld=>%ld/%ld bytes from %s for %s (%s)",
	   (long int)bytes_read,(long int)bytes_written,content_length,
	   fdsocketinfo(sockval,infobuf),
	   r->unparsed_uri,r->filename);
	errno=0; error=1; break;}}
    if (error) return -(bytes_read+1);
    rv=ap_rflush(r);
    if (r->connection->aborted) {
      ap_log_rerror(APLOG_MARK,APLOG_WARNING,OK,r,
		    "Connection aborted for %s",r->uri);
      return -1;}
    if (rv!=OK) {
      ap_log_rerror
	(APLOG_MARK,APLOG_ERR,rv,r,
	 "Flush error after %ld/%ld/%ld content bytes from %s for %s (%s)",
	 (long int)bytes_read,(long int)bytes_written,content_length,
	 fdsocketinfo(sockval,infobuf),
	 r->unparsed_uri,r->filename);
      return -(bytes_read+1);}
#if DEBUG_TRANSPORT
    ap_log_rerror
      (APLOG_MARK,APLOG_INFO,OK,r,
       "Transferred %ld/%ld content bytes from %s for %s (%s)",
       (long int)bytes_read,content_length,
       fdsocketinfo(sockval,infobuf),
       r->unparsed_uri,r->filename);
#endif
    return bytes_read;}
  else if (sockval->socktype==filesock) {
    int sock=sockval->conn.fd; int error=0, rv=-1;
    while ((content_length<0)||(bytes_read<content_length)) {
      ssize_t delta=read(sock,buf,4096), written=-1;
      if (r->connection->aborted) {
	ap_log_rerror(APLOG_MARK,APLOG_WARNING,OK,r,
		      "Connection aborted for %s",r->uri);
	return -1;}
      if (delta>0) {
	int chunk=ap_rwrite(buf,delta,r); written=0;
	bytes_read=bytes_read+delta;
	while (written<delta) {
	  if (chunk<0) break;
	  written=written+chunk;
	  chunk=ap_rwrite(buf+written,delta-written,r);}
	if (chunk<0) {error=1; break;}}
      else if (delta==0) {
	if (bytes_read<content_length) error=1;
	break;}
      else if (errno==EAGAIN) {errno=0; continue;}
      else {
	ap_log_rerror
	  (APLOG_MARK,APLOG_ERR,OK,r,
	   "Read error (%d:%s) after %ld=>%ld/%ld bytes from %s for %s (%s)",
	   errno,strerror(errno),
	   (long int)bytes_read,(long int)bytes_written,content_length,
	   fdsocketinfo(sockval,infobuf),
	   r->unparsed_uri,r->filename);
	errno=0; error=1; break;}
      if (written<0) {
	ap_log_rerror
	  (APLOG_MARK,APLOG_ERR,OK,r,
	   "Write error after %ld=>%ld/%ld bytes from %s for %s (%s)",
	   (long int)bytes_read,(long int)bytes_written,content_length,
	   fdsocketinfo(sockval,infobuf),
	   r->unparsed_uri,r->filename);
	errno=0; error=1; break;}
      else bytes_written=bytes_written+written;}
    rv=ap_rflush(r);
    if (r->connection->aborted) {
      ap_log_rerror(APLOG_MARK,APLOG_WARNING,OK,r,
		    "Connection aborted for %s",r->uri);
      return -1;}
    if ((error)||(rv!=OK)) {
      ap_log_rerror
	(APLOG_MARK,APLOG_ERR,rv,r,
	 "Error after transferring %ld/%ld bytes from %s for %s (%s)",
	 (long int)bytes_read,content_length,
	 fdsocketinfo(sockval,infobuf),
	 r->unparsed_uri,r->filename);
      return -(bytes_read+1);}
    else return bytes_read;}
  else {
    ap_log_error
      (APLOG_MARK,APLOG_CRIT,apr_get_os_error(),r->server,
       "Bad fdsocket passed");
    return -1;}
}

static int checkabort(request_rec *r,fdservlet servlet,fdsocket sock,
		      int started)
{
  char infobuf[512];
  if (r->connection->aborted) {
    if ((servlet)&&(sock)) {    
      ap_log_rerror(APLOG_MARK,APLOG_WARNING,OK,r,
		    "Connection aborted for %s, %s %s",
		    r->uri,((started)?("closing in-use"):("recycling")),
		    fdsocketinfo(sock,infobuf));
      if (started) servlet_close_socket(servlet,sock);
      else servlet_recycle_socket(servlet,sock);}
    else ap_log_rerror(APLOG_MARK,APLOG_WARNING,OK,r,
		       "Connection aborted for %s",r->uri);
    return 1;}
  else return 0;
}

/* The main event handler */

static int fdserv_handler(request_rec *r)
{
  apr_time_t started=apr_time_now(), connected, requested, computed, responded;
  BUFF *reqdata;
  fdservlet servlet=NULL; fdsocket sock=NULL;
  char *post_data, errbuf[512], infobuf[512];
  int post_size, bytes_written=0, bytes_transferred=-1;
  char *new_error=NULL, *error=NULL;
  const char *xredirect=NULL;
  int rv;
  struct HEAD_SCANNER scanner;
  struct FDSERV_SERVER_CONFIG *sconfig=
    ap_get_module_config(r->server->module_config,&fdserv_module);
  struct FDSERV_DIR_CONFIG *dconfig=
    ap_get_module_config(r->per_dir_config,&fdserv_module);
  int using_dtblock=((sconfig->use_dtblock<0)?(use_dtblock):
		     ((sconfig->use_dtblock)?(1):(0)));
  const char **sreq_params=sconfig->req_params;
  const char **dreq_params=dconfig->req_params;
#if TRACK_EXECUTION_TIMES
  struct timeb start, end; 
#endif
  if(strcmp(r->handler, FDSERV_MAGIC_TYPE) && strcmp(r->handler, "fdservlet"))
    return DECLINED;
  else if (!((r->method_number == M_GET) ||
	     (r->method_number == M_PUT) ||
	     (r->method_number == M_POST) ||
	     (r->method_number == M_DELETE) ||
	     (r->method_number == M_OPTIONS)))
	     return DECLINED;
#if DEBUG_FDSERV
  ap_log_rerror(APLOG_MARK,LOGDEBUG,OK,r,
		"Entered fdserv_handler for %s from %s",
		r->filename,r->unparsed_uri);
#endif
  if (r->connection->aborted) return OK;
  servlet=request_servlet(r);
  if (!(servlet)) {
    ap_log_rerror(APLOG_MARK,APLOG_ERR,OK,r,
		  "Couldn't get servlet %s to resolve %s",
		  r->filename,r->unparsed_uri);
    if (errno) error=strerror(errno); else error="no servlet";
    errno=0;}
  else sock=servlet_connect(servlet,r);

  if (checkabort(r,servlet,sock,0)) return OK;

  if (sock==NULL) {
    ap_log_rerror(APLOG_MARK,APLOG_ERR,HTTP_SERVICE_UNAVAILABLE,r,
		  "Servlet connect failed to %s for %s",
		  r->filename,r->unparsed_uri);
    /* This should be better */
    r->content_type="text/html";
    ap_send_http_header(r);
    ap_rvputs(r,"<HTML>\n<HEAD>\n<TITLE>",NULL);
    ap_rprintf(r,"Cannot start fdservlet for %s",r->filename);
    ap_rvputs(r,"</HEAD>\n<BODY>\n<H1>",NULL);
    ap_rprintf(r,"Cannot start fdservlet for %s",r->filename);
    ap_rvputs(r,"</BODY>\n</HTML>",NULL);
    errno=0;
    return HTTP_SERVICE_UNAVAILABLE;}
  else {
    connected=apr_time_now();
    ap_log_rerror(APLOG_MARK,APLOG_INFO,OK,r,
		  "Handling %s with %s through %s, %d busy",
		  r->unparsed_uri,r->filename,
		  fdsocketinfo(sock,infobuf),
		  servlet->n_busy);}
  reqdata=ap_bcreate(r->pool,0);
  if (!(reqdata)) {
    ap_log_rerror(APLOG_MARK,APLOG_CRIT,HTTP_INTERNAL_SERVER_ERROR,r,
		  "Couldn't allocate bufstream for processing %s (%s)",
		  r->unparsed_uri,r->filename);
    errno=0;
    servlet_recycle_socket(servlet,sock);
    return HTTP_INTERNAL_SERVER_ERROR;}
  else {}
  ap_add_common_vars(r); ap_add_cgi_vars(r);
  if (r->method_number == M_POST) {
    char bigbuf[4096];
    rv=ap_setup_client_block(r,REQUEST_CHUNKED_ERROR);
    if (rv!=OK) {}
    else if (ap_should_client_block(r)) {
      int bytes_read, size=0, limit=16384;
      char *data=apr_palloc(r->pool,16384);
      while ((bytes_read=ap_get_client_block(r,bigbuf,4096)) > 0) {
	if (bytes_read<0) {
	  ap_log_rerror(APLOG_MARK,APLOG_ERR,OK,r,
			"Error reading POST data from client");
	  if (errno) error=strerror(errno); else error="unknown";
	  break;}
	else ap_log_rerror(APLOG_MARK,LOGDEBUG,OK,r,
			   "Read %d bytes of POST data from client",
			   bytes_read);
	ap_reset_timeout(r);
	if (checkabort(r,servlet,sock,0)) return OK;
	if (size+bytes_read > limit) {
	  char *newbuf=apr_palloc(r->pool,limit*2);
	  if (!(newbuf)) {error="malloc failed"; break;}
	  memcpy(newbuf,data,size);
	  data=newbuf; limit=limit*2;}
	memcpy(data+size,bigbuf,bytes_read); size=size+bytes_read;}
      post_data=data; post_size=size;}
    else {post_data=NULL; post_size=0;}}
  else {post_data=NULL; post_size=0;}

  if (checkabort(r,servlet,sock,0)) return OK;
  else if (error) {
    ap_log_rerror(APLOG_MARK,APLOG_WARNING,HTTP_INTERNAL_SERVER_ERROR,r,
		  "Error (%s) reading post data",error);
    servlet_recycle_socket(servlet,sock);
    return HTTP_INTERNAL_SERVER_ERROR;}
#if DEBUG_CGIDATA
  else ap_log_rerror(APLOG_MARK,LOGDEBUG,OK,r,
		     "Composing request data as a slotmap for %s (%s)",
		     r->unparsed_uri,r->filename);
#endif
  
  /* Write the slotmap into buf, the write the buf to the servlet socket */
  if (write_cgidata(r,reqdata,r->subprocess_env,
		    sreq_params,dreq_params,
		    post_size,post_data)<0) {
    ap_log_rerror(APLOG_MARK,LOGDEBUG,OK,r,
		  "Error composing request data as a slotmap");
    servlet_recycle_socket(servlet,sock);
    return HTTP_INTERNAL_SERVER_ERROR;}

  /* We don't really need the socket until here, but we want to fail earlier. */
#if ((DEBUG_CGIDATA)||(DEBUG_TRANSPORT))
  ap_log_rerror(APLOG_MARK,LOGDEBUG,OK,r,
		"Writing %ld bytes of request to %s for %s (%s)",
		(long int)(reqdata->ptr-reqdata->buf),
		fdsocketinfo(sock,infobuf),
		r->unparsed_uri,r->filename);
#endif
  
  if (using_dtblock) {
    unsigned char buf[8];
    unsigned int nbytes=reqdata->ptr-reqdata->buf;
    buf[0]=0x14;
    buf[1]=((nbytes>>24)&0xFF);
    buf[2]=((nbytes>>16)&0xFF);
    buf[3]=((nbytes>>8)&0xFF);
    buf[4]=((nbytes>>0)&0xFF);
    bytes_written=sock_write(r,buf,5,sock);}
  if (checkabort(r,servlet,sock,1)) return OK;
  if ((using_dtblock)&&(bytes_written<5)) {
    ap_log_rerror(APLOG_MARK,APLOG_CRIT,OK,r,
		  "Only wrote %ld bytes of block to %s for %s (%s)",
		  (long int)bytes_written,
		  fdsocketinfo(sock,infobuf),
		  r->unparsed_uri,r->filename);
    servlet_close_socket(servlet,sock);
    if (bytes_written==0)
      return HTTP_SERVICE_UNAVAILABLE;
    else return HTTP_INTERNAL_SERVER_ERROR;}
  else {
    bytes_written=sock_write(r,reqdata->buf,reqdata->ptr-reqdata->buf,sock);
    if (bytes_written<(reqdata->ptr-reqdata->buf)) {
      ap_log_rerror(APLOG_MARK,APLOG_CRIT,OK,r,
		    "Only wrote %ld bytes of request to %s for %s (%s)",
		    (long int)bytes_written,
		    fdsocketinfo(sock,infobuf),
		    r->unparsed_uri,r->filename);
      servlet_close_socket(servlet,sock);
      if (bytes_written==0)
	return HTTP_SERVICE_UNAVAILABLE;
      else return HTTP_INTERNAL_SERVER_ERROR;}
    else ap_log_rerror(APLOG_MARK,LOGDEBUG,OK,r,
		       "Wrote %ld bytes of request data to %s for %s (%s)",
		       ((long int)(reqdata->ptr-reqdata->buf)),
		       fdsocketinfo(sock,infobuf),
		       r->unparsed_uri,
		       r->filename);}
  if (checkabort(r,servlet,sock,1)) return OK;
  
  requested=apr_time_now();
  
  scanner.sock=sock; scanner.req=r;
  
  ap_log_rerror(APLOG_MARK,LOGDEBUG,OK,r,
		"Waiting for response from %s",
		fdsocketinfo(sock,infobuf));
  
  rv=ap_scan_script_header_err_core(r,errbuf,scan_fgets,(void *)&scanner);
  
  if (checkabort(r,servlet,sock,1)) return OK;
  
  xredirect=apr_table_get(r->headers_out,"X-Redirect");
  if (!(xredirect)) xredirect=apr_table_get(r->err_headers_out,"X-Redirect");

  if (xredirect) {
    ap_internal_redirect(xredirect,r);
    return OK;}
  else if (rv==HTTP_INTERNAL_SERVER_ERROR) {
    ap_log_rerror(APLOG_MARK,APLOG_CRIT,rv,r,
		  "Error (%s) status=%d reading header from %s",
		  errbuf,r->status,fdsocketinfo(sock,infobuf));
    servlet_close_socket(servlet,sock);
    return HTTP_INTERNAL_SERVER_ERROR;}
  else ap_log_rerror(APLOG_MARK,LOGDEBUG,OK,r,
		     "Read header from %s, transferring content",
		     fdsocketinfo(sock,infobuf));
  
  computed=apr_time_now();
  
  bytes_transferred=copy_servlet_output(sock,r);
  
  if (checkabort(r,servlet,sock,1)) return OK;
  
  if (bytes_transferred<0) {
    ap_log_rerror(APLOG_MARK,APLOG_WARNING,OK,r,
		  "Error returning content to client from %s",
		  fdsocketinfo(sock,infobuf));
    servlet_close_socket(servlet,sock);
    if (bytes_transferred==-1)
      return HTTP_INTERNAL_SERVER_ERROR;}
  
  responded=apr_time_now();
  
  ap_log_rerror
    (APLOG_MARK,((bytes_transferred<0)?(APLOG_ERR):(APLOG_INFO)),OK,r,
     "%s returning %d bytes for %s (%s) in %ldus=%ld+%ld+%ld+%ld, %d busy",
     ((bytes_transferred<0)?("Error"):("Done")),
     ((bytes_transferred<0)?(-bytes_transferred):(bytes_transferred)),
     r->unparsed_uri,r->filename,
     ((long)(responded-started)),
     ((long)(connected-started)),
     ((long)(requested-connected)),
     ((long)(computed-requested)),
     ((long)(responded-computed)),
     servlet->n_busy);
  
#if TRACK_EXECUTION_TIMES
  {char buf[64]; double interval; ftime(&end);
    interval=1000*(end.time-start.time)+(end.millitm-start.millitm);
    sprintf(buf,"%lf",interval/1000.0);
    apr_table_set(r->subprocess_env,"EXECUTED","yes");
    apr_table_set(r->notes,"exectime",buf);}
#endif
  servlet_recycle_socket(servlet,sock);
  return OK;
}

static int fdserv_post_config(apr_pool_t *p,
			      apr_pool_t *plog,
			      apr_pool_t *ptemp,
			      server_rec *s)
{
  struct FDSERV_SERVER_CONFIG *sconfig=
    ap_get_module_config(s->module_config,&fdserv_module);
  ap_log_perror(APLOG_MARK,APLOG_NOTICE,OK,p,
		"mod_fdserv v%s starting post config for Apache 2.x (%s)",
		version_num,_FILEINFO);
  if (sconfig->socket_prefix==NULL)
    sconfig->socket_prefix=apr_pstrdup(p,"/var/run/fdserv/");
  if (sconfig->socket_prefix[0]=='/') {
    const char *dirname=sconfig->socket_prefix;
    int retval=apr_dir_make_recursive(dirname,RUN_FDSERV_PERMISSIONS,p);
    if (retval)
      ap_log_error
	(APLOG_MARK,APLOG_CRIT,retval,s,
	 "Problem with creating socket prefix directory %s",dirname);
    else if ((sconfig->uid>=0) && (sconfig->gid>=0))
      retval=chown(dirname,sconfig->uid,sconfig->gid);
    if (retval)
      ap_log_error
	(APLOG_MARK,APLOG_CRIT,retval,s,
	 "Problem with setting owner/group on socket prefix directory %s",dirname);
    else ap_log_error
	   (APLOG_MARK,APLOG_NOTICE,retval,s,"Using socket prefix directory %s",dirname);}
  init_version_info();
  ap_add_version_component(p,version_info);
  ap_log_perror(APLOG_MARK,APLOG_NOTICE,OK,p,
		"mod_fdserv v%s finished post config for Apache 2.x",
		version_num);
  return OK;
}

static void fdserv_init(apr_pool_t *p,server_rec *s)
{
  ap_log_perror(APLOG_MARK,APLOG_INFO,OK,p,
		"mod_fdserv v%s starting child init (%d) for Apache 2.x (%s)",
		version_num,(int)getpid(),_FILEINFO);
  socketname_table=apr_table_make(p,64);
  fdserv_pool=p;
  apr_thread_mutex_create(&servlets_lock,APR_THREAD_MUTEX_DEFAULT,fdserv_pool);
  servlets=apr_pcalloc(fdserv_pool,sizeof(struct FDSERVLET)*(FDSERV_INIT_SERVLETS));
  max_servlets=FDSERV_INIT_SERVLETS;
  /* apr_pool_cleanup_register(p,p,close_servlets,NULL); */
  #if ((APR_MAJOR_VERSION>=1)&&(APR_MAJOR_VERSION>=4))
  apr_pool_pre_cleanup_register(p,p,close_servlets);
  #endif
  ap_log_perror(APLOG_MARK,APLOG_INFO,OK,p,
		"mod_fdserv v%s finished child init (%d) for Apache 2.x",
		version_num,(int)getpid());
}
static void register_hooks(apr_pool_t *p)
{
  /* static const char * const run_first[]={ "mod_mime",NULL }; */
  ap_hook_handler(fdserv_handler, NULL, NULL, APR_HOOK_LAST);
  ap_hook_child_init(fdserv_init, NULL, NULL, APR_HOOK_MIDDLE);
  ap_hook_post_config(fdserv_post_config, NULL, NULL, APR_HOOK_MIDDLE);
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

/* Emacs local variables
;;;  Local variables: ***
;;;  compile-command: "cd ..; make mod_fdserv" ***
;;;  End: ***
*/

