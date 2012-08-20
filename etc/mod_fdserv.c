/* C Mode */

/* mod_fdserv.c
   Copyright (C) 2002-2012 beingmeta, inc.  All Rights Reserved
   This is a Apache module supporting persistent FramerD servers

   For Apache 2.0:
     Compile with: apxs2 -c mod_fdserv.c
     Install with: apxs2 -i mod_fdserv.so

*/

static char revisioninfo[] = "$Revision: 3991$";

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

#ifndef HEAVY_DEBUGGING
#define HEAVY_DEBUGGING 0
#endif

#define APACHE20 1
#define APACHE13 0

#if (APR_SIZEOF_VOIDP==8)
typedef unsigned long long INTPOINTER;
#else
typedef unsigned int INTPOINTER;
#endif

#define DEFAULT_SERVLET_WAIT 60
#define MAX_CONFIGS 64

#include "util_script.h"
#include "apr.h"
#include "apr_portable.h"
#include "apr_strings.h"

#include <sys/un.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <unistd.h>

#ifndef TRACK_EXECUTION_TIMES
#if HAVE_FTIME
#define TRACK_EXECUTION_TIMES 1
#else
#define TRACK_EXECUTION_TIMES 1
#endif
#endif

#if TRACK_EXECUTION_TIMES
#include "sys/timeb.h"
#endif

#define FDSERV_MAGIC_TYPE "application/x-httpd-fdserv"

typedef struct FDSOCKET {
  enum {filesock,aprsock,badsock} socktype;
  const char *sockname;
  union { int fd; apr_socket_t *apr;}
    sockdata;}
  *fdsocket;

#ifndef DEFAULT_ISASYNC
#define DEFAULT_ISASYNC 0
#endif

static int default_isasync=DEFAULT_ISASYNC;

/* Compatibility */

#define APLOG_HEAD APLOG_MARK,APLOG_DEBUG,OK
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
  char *newptr=apr_palloc(p,new_size);
  if (newptr != ptr) memmove(newptr,ptr,old_size);
  return newptr;
}

module AP_MODULE_DECLARE_DATA fdserv_module;

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
    (APLOG_MARK,APLOG_DEBUG,OK,s,
     "mod_fdserv: Checking writability of %s with uid=%d gid=%d",
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

static int file_writablep(apr_pool_t *p,server_rec *s,const char *filename)
{
  fileinfo finfo; int retval;
  ap_log_error
    (APLOG_MARK,APLOG_DEBUG,OK,s,
     "mod_fdserv: Checking writability of file %s",filename);
  retval=get_file_info(p,filename,&finfo);
  if (retval) {
    ap_log_error
      (APLOG_MARK,APLOG_DEBUG,retval,s,
       "mod_fdserv: stat failed for %s",filename);
    filename=ap_make_dirstr_parent(p,filename);
    retval=apr_stat(&finfo,filename,FINFO_FLAGS,p);
    if (retval) {
      ap_log_error
	(APLOG_MARK,APLOG_CRIT,retval,s,
	 "mod_fdserv: stat failed for %s",filename);
      return 0;}}
  if (stat_can_writep(p,s,&finfo)) {
    ap_log_error(APLOG_MARK,APLOG_DEBUG,retval,s,
		 "mod_fdserv: found file writable: %s",filename);
    return 1;}
  else return 0;
}

/* Configuration handling */

/* I know this is bad form, but I really want these to be fast to access. */
static apr_table_t *socketname_table;

struct FDSERV_SERVER_CONFIG {
  const char *server_executable;
  const char **config_args;
  const char *socket_prefix;
  const char *socket_spec;
  const char *log_prefix;
  const char *log_file;
  int servlet_wait;
  int is_async;
  uid_t uid; gid_t gid;};

struct FDSERV_DIR_CONFIG {
  const char *server_executable;
  const char **config_args;
  const char *socket_prefix;
  const char *socket_spec;
  const char *log_prefix;
  const char *log_file;
  int servlet_wait;};

static void *create_server_config(apr_pool_t *p,server_rec *s)
{
  struct FDSERV_SERVER_CONFIG *config=
    apr_palloc(p,sizeof(struct FDSERV_SERVER_CONFIG));
  config->server_executable=NULL;
  config->config_args=NULL;
  config->socket_prefix=NULL;
  config->socket_spec=NULL;
  config->log_prefix=NULL;
  config->log_file=NULL;
  config->servlet_wait=-1;
  config->is_async=-1;
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

  if (child->servlet_wait <= 0)
    config->servlet_wait=parent->servlet_wait;
  else config->servlet_wait=child->servlet_wait;

  if (child->is_async <= 0)
    config->is_async=parent->is_async;
  else config->is_async=child->is_async;

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
  config->socket_prefix=NULL;
  config->socket_spec=NULL;
  config->log_prefix=NULL;
  config->log_file=NULL;
  config->servlet_wait=-1;
  return (void *) config;
}

static void *merge_dir_config(apr_pool_t *p,void *base,void *new)
{
  struct FDSERV_DIR_CONFIG *config
    =apr_palloc(p,sizeof(struct FDSERV_DIR_CONFIG));
  struct FDSERV_DIR_CONFIG *parent=base;
  struct FDSERV_DIR_CONFIG *child=new;

  memset(config,0,sizeof(struct FDSERV_DIR_CONFIG));

  if (child->servlet_wait <= 0)
    config->servlet_wait=parent->servlet_wait;
  else config->servlet_wait=child->servlet_wait;

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
    vec[0]=config_arg; vec[1]=NULL;
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
    ap_log_error
      (APLOG_MARK,APLOG_INFO,OK,parms->server,
       "mod_fdserv: Socket Prefix set to %s for path %s",fullpath,parms->path);
  else ap_log_error
	 (APLOG_MARK,APLOG_INFO,OK,parms->server,
	  "mod_fdserv: Socket Prefix set to %s for server %s",
	  fullpath,parms->server->server_hostname);
  
  if (!(file_writablep(parms->pool,parms->server,fullpath)))
    ap_log_error
      (APLOG_MARK,APLOG_CRIT,OK,parms->server,
       "mod_fdserv: Socket Prefix %s (%s) is not writable",arg,fullpath);

  return NULL;
}

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
    ap_log_error
      (APLOG_MARK,APLOG_INFO,OK,parms->server,
       "mod_fdserv: Log Prefix set to %s for path %s",fullpath,parms->path);
  else ap_log_error
	 (APLOG_MARK,APLOG_INFO,OK,parms->server,
	  "mod_fdserv: Log Prefix set to %s for server %s",
	  fullpath,parms->server->server_hostname);

  if (!(file_writablep(parms->pool,parms->server,fullpath)))
    ap_log_error
      (APLOG_MARK,APLOG_CRIT,OK,parms->server,
       "mod_fdserv: Log prefix %s (%s) is not writable",arg,fullpath);
  return NULL;
}

static const char *socket_spec(cmd_parms *parms,void *mconfig,const char *arg)
{
  struct FDSERV_DIR_CONFIG *dconfig=mconfig;
  struct FDSERV_SERVER_CONFIG *sconfig=
    ap_get_module_config(parms->server->module_config,&fdserv_module);
  const char *fullpath=NULL, *spec=NULL;

  if (arg[0]=='/') spec=arg;
  else if ((strchr(arg,'@'))||(strchr(arg,':'))) spec=arg;
  else if (dconfig->socket_prefix)
    spec=fullpath=apr_pstrcat(parms->pool,dconfig->socket_prefix,arg,NULL);
  else if (sconfig->socket_prefix)
    spec=fullpath=apr_pstrcat(parms->pool,sconfig->socket_prefix,arg,NULL);
  else spec=arg;

  dconfig->socket_spec=spec;
  
  if (!(fullpath))
    ap_log_error
      (APLOG_MARK,APLOG_DEBUG,OK,parms->server,
       "mod_fdserv: Socket spec=%s",spec);
  else if (!(file_writablep(parms->pool,parms->server,fullpath)))
    ap_log_error
      (APLOG_MARK,APLOG_CRIT,OK,parms->server,
       "mod_fdserv: Socket file %s=%s is unwritable",
       arg,fullpath);
  else ap_log_error
	 (APLOG_MARK,APLOG_DEBUG,OK,parms->server,
	  "mod_fdserv: Socket file %s=%s",arg,fullpath);
  return NULL;
}

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
    ap_log_error
      (APLOG_MARK,APLOG_CRIT,OK,parms->server,
       "mod_fdserv: Log file %s=%s+%s is unwritable",
       fullpath,log_prefix,arg);
  else ap_log_error
	 (APLOG_MARK,APLOG_DEBUG,OK,parms->server,
	  "mod_fdserv: Log file %s=%s+%s",fullpath,log_prefix,arg);
  return NULL;
}

static const char *servlet_wait(cmd_parms *parms,void *mconfig,const char *arg)
{
  if (parms->path) {
    struct FDSERV_DIR_CONFIG *dconfig=mconfig;
    int wait_interval=atoi(arg);
    dconfig->servlet_wait=wait_interval;
    return NULL;}
  else return NULL;
}

static const char *is_async(cmd_parms *parms,void *mconfig,const char *arg)
{
  struct FDSERV_SERVER_CONFIG *sconfig=mconfig;
  if (!(arg))
    sconfig->is_async=0;
  else if (!(*arg))
    sconfig->is_async=0;
  else if ((*arg=='1')||(*arg=='y')||(*arg=='y'))
    sconfig->is_async=1;
  else sconfig->is_async=0;
  return NULL;
}

static const command_rec fdserv_cmds[] =
{
  AP_INIT_TAKE1("FDServletExecutable", servlet_executable, NULL, OR_ALL,
	       "the executable used to start a servlet"),
  AP_INIT_TAKE2("FDServletConfig", servlet_config, NULL, OR_ALL,
		"configuration parameters to the servlet"),

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
  AP_INIT_TAKE1("FDServletWait", servlet_wait, NULL, OR_ALL,
		"the number of seconds to wait for the servlet to startup"),
  AP_INIT_TAKE1("FDServletAsync", is_async, NULL, OR_ALL,
		"whether to assume asynchronous fdserv support"),
  
  {NULL}
};

/* Launching fdservlet processes */

static int spawn_fdservlet (request_rec *r,apr_pool_t *p,const char *sockname);

static char *convert_spec(const char *spec,char *buf)
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

#define RUN_FDSERV_PERMISSIONS \
 (APR_FPROT_UREAD | APR_FPROT_UWRITE | APR_FPROT_UEXECUTE | \
  APR_FPROT_GREAD | APR_FPROT_GWRITE | APR_FPROT_GEXECUTE | \
  APR_FPROT_WREAD |APR_FPROT_WEXECUTE)

static int check_directory(apr_pool_t *p,const char *filename)
{
  char *dirname=ap_make_dirstr_parent(p,filename);
  int retval=apr_dir_make_recursive(dirname,RUN_FDSERV_PERMISSIONS,p);
  if (retval) return retval;
  else if (chown(dirname,unixd_config.user_id,unixd_config.group_id)<0)
    return 1;
  else return 0;
}

static const char *get_sockname(request_rec *r,const char *spec) /* 2.0 */
{
  char pathbuf[PATH_MAX];
  const char *cached=apr_table_get(socketname_table,spec);
  if (cached) return cached;
  else {
    struct FDSERV_SERVER_CONFIG *sconfig=
      ap_get_module_config(r->server->module_config,&fdserv_module);
    struct FDSERV_DIR_CONFIG *dconfig=
      ap_get_module_config(r->per_dir_config,&fdserv_module);
    const char *socket_spec=
      (((dconfig->socket_spec)!=NULL) ? (dconfig->socket_spec) :
       (convert_spec(spec,pathbuf)));
    const char *socket_prefix=
      ((dconfig->socket_prefix) ? (dconfig->socket_prefix) :
       (sconfig->socket_prefix) ? (sconfig->socket_prefix) :
       "/var/run/fdserv/");

    if ((socket_spec)&&
	((strchr(socket_spec,'@'))||
	 (strchr(socket_spec,':'))))
      return socket_spec;

    /* This generates a socketname for a particular request URI.  It's
       not used when we explicitly set socket specs. */
    
    ap_log_rerror
      (APLOG_MARK,APLOG_DEBUG,OK,r,
       "spawn get_sockname socket_spec='%s', socket_prefix='%s', isdir=%d",
       socket_spec,socket_prefix,
       isdirectoryp(r->pool,socket_prefix));
    
    if ((socket_spec)&&(socket_spec[0]=='/')) {
      /* Absolute socket file name, just use it. */
      apr_table_set(socketname_table,spec,socket_spec);
      return apr_table_get(socketname_table,spec);}
    else {
      /* Relative socket file name, just append it. */
      char *path=makepath(r->pool,socket_prefix,socket_spec);
      apr_table_set(socketname_table,spec,path);
      return apr_table_get(socketname_table,spec);}}
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
     "logs/fdserv/");
  
  ap_log_rerror
    (APLOG_MARK,APLOG_DEBUG,OK,r,
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

static int spawn_fdservlet /* 2.0 */
  (request_rec *r,apr_pool_t *p,const char *sockname) 
{
  /* This launches the fdservlet process.  It probably shouldn't be used
     when using TCP to connect, since the specified server might not be one
     we can lanuch a process on (i.e. not us) */
  apr_proc_t proc; apr_procattr_t *attr;
  /* Executable, socket name, NULL, LOG_FILE env, NULL */
  const char *argv[2+MAX_CONFIGS+1+1+1], **envp, **write_argv=argv;
  struct stat stat_data; int rv, n_configs=0, retval=0;

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
  const char *log_file=get_log_file(r,NULL);
  int servlet_wait=dconfig->servlet_wait;
  uid_t uid; gid_t gid;

  if (servlet_wait<0) servlet_wait=DEFAULT_SERVLET_WAIT;

  if (log_file==NULL) log_file=get_log_file(r,sockname);

  apr_uid_current(&uid,&gid,p);

  if (!(file_writablep(p,s,sockname))) {
    ap_log_error(APLOG_MARK,APLOG_CRIT,500,s,
		 "mod_fdserv: Can't write socket file '%s' (%s) for %s, uid=%d, gid=%d",
		 sockname,exename,r->unparsed_uri,uid,gid);
    return -1;}
  if ((log_file) && (!(file_writablep(p,s,log_file)))) {
    ap_log_error(APLOG_MARK,APLOG_CRIT,500,s,
		 "mod_fdserv: Logfile %s isn't writable for processing %s",
		 log_file,r->unparsed_uri);
    return -1;}

  if (log_file)
    ap_log_error
      (APLOG_MARK,APLOG_NOTICE,OK,s,
       "mod_fdserv: Spawning fdservlet %s @%s>%s for %s, uid=%d, gid=%d",
       exename,sockname,log_file,r->unparsed_uri,uid,gid);
  else ap_log_error
	 (APLOG_MARK,APLOG_NOTICE,OK,s,
	  "mod_fdserv: Spawning fdservlet %s @%s for %s, uid=%d, gid=%d",
	  exename,sockname,r->unparsed_uri,uid,gid);
  
  *write_argv++=(char *)exename;
  *write_argv++=(char *)sockname;
  
  if (scan_config) {
    while (*scan_config) {
      if (n_configs>MAX_CONFIGS) {
	ap_log_error
	  (APLOG_MARK,APLOG_CRIT,OK,s,
	   "mod_fdserv: Stopped after %d configs!",n_configs);}
      *write_argv++=(char *)(*scan_config); scan_config++;
      n_configs++;}}
  
  *write_argv++=NULL;
  
  /* Pass the logfile in through the environment, so we can
     use it to record processing of config variables */
  if (log_file) {
    char *env_entry=apr_psprintf(p,"LOGFILE=%s",log_file);
    envp=write_argv; 
    *write_argv++=(char *)env_entry;
    *write_argv++=NULL;}
  else envp=NULL;
  
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
	 (APLOG_MARK, APLOG_DEBUG, rv, r,
	  "Successfully set child process attributes: %s", r->filename);
  
  {
    const char **scanner=argv; while (scanner<write_argv) {
      if ((envp) && (scanner>=envp))
	ap_log_rerror(APLOG_MARK, APLOG_DEBUG, OK, r,
		      "%s ENV[%ld]='%s'",
		      exename,((long int)(scanner-argv)),*scanner);
      else ap_log_rerror(APLOG_MARK, APLOG_DEBUG, OK, r,
			 "%s ARG[%ld]='%s'",
			 exename,((long int)(scanner-argv)),*scanner);
      scanner++;}}

  if ((stat(sockname,&stat_data) == 0)&&
      (((time(NULL))-stat_data.st_mtime)>15)) {
    if (remove(sockname) == 0)
      ap_log_error
	(APLOG_MARK,APLOG_NOTICE,OK,s,
	 "mod_fdserv: Removed leftover socket file %s",sockname);
    else {
      ap_log_error
	(APLOG_MARK,APLOG_CRIT,500,s,
	 "mod_fdserv: Could not remove socket file %s",sockname);
      return -1;}}
  
  errno=0;
  rv=apr_proc_create(&proc,exename,(const char **)argv,envp,
		     attr,p);
  if (rv!=APR_SUCCESS) {
    ap_log_rerror(APLOG_MARK,APLOG_CRIT, rv, r,
		  "Couldn't spawn %s @%s for %s [rv=%d,pid=%d,uid=%d,gid=%d]",
		  exename,sockname,r->unparsed_uri,rv,proc.pid,uid,gid);
    /* We don't exit right away because there might be a race condition
       so another process is creating the socket.  So we still wait. */
    retval=-1;}
  else if (log_file)
    ap_log_error
      (APLOG_MARK,APLOG_NOTICE,rv,s,
       "mod_fdserv: Spawned %s @%s (logfile=%s) for %s "
       "[rv=%d,pid=%d,uid=%d,uid=%d]",
       exename,sockname,log_file,r->unparsed_uri,rv,proc.pid,uid,gid);
  else ap_log_error
      (APLOG_MARK,APLOG_NOTICE,rv,s,
       "mod_fdserv: Spawned %s @%s (nologfile) for %s "
       "[rv=%d,pid=%d,uid=%d,gid=%d]",
       exename,sockname,r->unparsed_uri,rv,proc.pid,uid,gid);
  
  /* Now wait for the socket file to exist */
  {
    int sleep_count=1;
    sleep(1); while ((rv=stat(sockname,&stat_data)) < 0) {
      if (sleep_count>servlet_wait) {
	ap_log_rerror(APLOG_MARK,APLOG_CRIT,500,r,
		      "Failed to spawn socket file %s (i=%d/wait=%d) (%d:%s)",
		      sockname,sleep_count,servlet_wait,
		      errno,strerror(errno));
	errno=0;
	return -1;}
      if (((sleep_count+1)%4)==0) {
	ap_log_rerror
	  (APLOG_MARK,APLOG_NOTICE,rv,r,
	   "Still waiting for %s to exist (i=%d/wait=%d) (errno=%d:%s)",
	   sockname,sleep_count,servlet_wait,errno,strerror(errno));
	errno=0; sleep(2);}
      else sleep(1);
      sleep_count++;}}

  if (rv>=0) return 0;
  else return retval;
}

static char *get_pathspec(request_rec *r,char *buf)
{
  sprintf(buf,"%s:%s",r->server->server_hostname,r->uri);
  return buf;
}

static fdsocket connect_to_servlet(request_rec *r)
{
  apr_socket_t *sock; char buf[512], *pathspec=get_pathspec(r,buf);
  const char *sockname=get_sockname(r,pathspec);
  struct FDSOCKET *result;
  int rv=0;
  /* TCP socket specs have either the form ttport@hostname or hostname:port
     where ttport might be touch-tone encoded. */
  int isfilesock=((sockname)&&
		  ((strchr(sockname,'@'))==NULL)&&
		  ((strchr(sockname,'/')!=NULL)||
		   ((strchr(sockname,':'))==NULL)));
  
  if (sockname==NULL) {
    ap_log_rerror
      (APLOG_MARK,APLOG_CRIT,500,r,
       "Couldn't resolve socket name for %s",r->unparsed_uri);
    return NULL;}

  ap_log_error
    (APLOG_MARK,APLOG_INFO,OK,r->server,
     "mod_fdserv: Resolving request for %s through %s %s",
     r->unparsed_uri,
     ((isfilesock)?"file socket":"net socket"),
     sockname);

  if (isfilesock) {
    struct sockaddr_un un_servname;
    int unix_sock; int connval;
    memset(&un_servname,0,sizeof(un_servname));
    un_servname.sun_family=AF_LOCAL; strcpy(un_servname.sun_path,sockname);
    unix_sock=socket(PF_LOCAL,SOCK_STREAM,0);
    if (unix_sock<0) {
      ap_log_rerror(APLOG_MARK,APLOG_CRIT,500,r,
		    "Couldn't open socket for %s (errno=%d:%s)",
		    sockname,errno,strerror(errno));
      return NULL;}
    else ap_log_rerror
	   (APLOG_MARK,APLOG_DEBUG,OK,r,
	    "Opened socket %d to connect to %s",unix_sock,sockname);;
    connval=connect(unix_sock,(struct sockaddr *)&un_servname,
		    SUN_LEN(&un_servname));
    if (connval<0) {
      ap_log_rerror
	(APLOG_MARK,APLOG_CRIT,500,
	 r,((isfilesock)?("Couldn't connect socket to %s (errno=%d:%s), spawning"):
	    ("Couldn't connect socket to %s (errno=%d:%s)")),
	 sockname,errno,strerror(errno));
      errno=0;
      rv=spawn_fdservlet(r,r->pool,sockname);
      if (rv<0) {
	ap_log_rerror
	  (APLOG_MARK,APLOG_CRIT,500,r,"Couldn't spawn fdservlet @ %s",sockname);
	return NULL;}
      else ap_log_rerror
	     (APLOG_MARK,APLOG_DEBUG,OK,r,"Waiting to connect to %s",sockname);
      /* Now try again */
      connval=connect(unix_sock,(struct sockaddr *)&un_servname,
		      SUN_LEN(&un_servname));
      if (connval < 0) {
	ap_log_rerror
	(APLOG_MARK,APLOG_CRIT,500,r,"Couldn't connect to %s (errno=%d:%s)",
	 sockname,errno,strerror(errno));
	errno=0;
	return NULL;}
      else {
	ap_log_rerror
	  (APLOG_MARK,APLOG_DEBUG,OK,r,"Successfully connected to %s @ %d",
	   sockname,unix_sock);}}
    else ap_log_rerror
	   (APLOG_MARK,APLOG_DEBUG,OK,r,
	    "Connected to %s using %d",sockname,unix_sock);
    result=apr_palloc(r->pool,sizeof(struct FDSOCKET));
    memset(result,0,sizeof(struct FDSOCKET));
    result->socktype=filesock;
    result->sockname=sockname;
    result->sockdata.fd=unix_sock;
    ap_log_rerror
      (APLOG_MARK,APLOG_INFO,OK,r,
       "Returning unix/local/file socket %d for %s",
       unix_sock,sockname);
    return result;}
  else {
    /* Use APR functions for network connection. */
    apr_status_t retval;
    apr_sockaddr_t *addr;
    char hostname[128]; int portno=-1;
    char *split=strchr(sockname,'@');
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
      return NULL;}
    retval=apr_socket_create(&sock,APR_INET,SOCK_STREAM,APR_PROTO_TCP,
			     r->pool);
    if (retval!=APR_SUCCESS) {
      ap_log_rerror
	(APLOG_MARK,APLOG_WARNING,OK,r,
	 "Unable to allocate socket to connect with %s",
	 sockname);
      return NULL;}
    retval=apr_sockaddr_info_get
      (&addr,hostname,APR_UNSPEC,(short)portno,0,r->pool);
    if (retval!=APR_SUCCESS) {
      ap_log_rerror
	(APLOG_MARK,APLOG_WARNING,OK,r,
	 "Unable to resolve connection info for port %d at %s",
	 portno,sockname);
      return NULL;}
    {char *rname;
      apr_sockaddr_ip_get(&rname,addr);
      ap_log_rerror
	(APLOG_MARK,APLOG_DEBUG,OK,r,
	 "Got info for port %d at %s, addr=%s, port=%d",
	 portno,hostname,rname,addr->port);}
    retval=apr_socket_connect(sock,addr);
    if (retval!=APR_SUCCESS) {
      ap_log_rerror
	(APLOG_MARK,APLOG_WARNING,OK,r,
	 "Unable to make connection to %s",
	 sockname);
      return NULL;}
    result=apr_palloc(r->pool,sizeof(struct FDSOCKET));
    memset(result,0,sizeof(struct FDSOCKET));
    result->socktype=aprsock;
    result->sockname=sockname;
    result->sockdata.apr=sock;
    ap_log_rerror
      (APLOG_MARK,APLOG_DEBUG,OK,r,"Opened socket to use with %s",
       sockname);
    return result;}
}

/* Writing DTYpes to BUFFs */

/* In Apache 2.0, BUFFs seem to be replaced by buckets in brigades,
    but that seems a little overhead heavy for the output buffers used
    here, which are just used to write slotmaps to servlets.  So a
    simple reimplementation happens. here.  */
typedef struct BUFF {
  apr_pool_t *p; unsigned char *buf, *ptr, *lim;} BUFF;
static void ap_bputc(unsigned char c,BUFF *b)
{
  if (b->ptr+1 >= b->lim) {
    int old_size=b->lim-b->buf, off=b->ptr-b->buf;
    unsigned char *nbuf=
      (unsigned char *)prealloc(b->p,(char *)b->buf,old_size*2,old_size);
    b->buf=nbuf; b->ptr=nbuf+off; b->lim=nbuf+old_size*2;}
  *(b->ptr++)=c;
}
static void ap_bputs(char *string,BUFF *b)
{
  int len=strlen(string);
  if (b->ptr+len+1 >= b->lim) {
    int old_size=b->lim-b->buf, off=b->ptr-b->buf;
    int new_size=old_size, need_size=off+len+1;
    unsigned char *nbuf;
    while (new_size < need_size) new_size=new_size*2;
    nbuf=(unsigned char *)prealloc(b->p,(char *)b->buf,new_size,old_size);
    b->buf=nbuf; b->ptr=nbuf+off; b->lim=nbuf+new_size;}
  strcpy((char *)b->ptr,string); b->ptr=b->ptr+len;
}
static void ap_bwrite(BUFF *b,char *string,int len)
{
  if (b->ptr+len+1 >= b->lim) {
    int old_size=b->lim-b->buf, off=b->ptr-b->buf;
    int new_size=old_size, need_size=off+len+1;
    unsigned char *nbuf;
    while (new_size < need_size) new_size=new_size*2;
    nbuf=(unsigned char *)prealloc(b->p,(char *)b->buf,new_size,old_size);
    b->buf=nbuf; b->ptr=nbuf+off; b->lim=nbuf+new_size;}
  memcpy((char *)b->ptr,string,len); b->ptr=b->ptr+len;
}
static BUFF *ap_bcreate(apr_pool_t *p,int ignore_flag)
{
  struct BUFF *b=apr_palloc(p,sizeof(struct BUFF));
  b->p=p; b->ptr=b->buf=apr_palloc(p,1024); b->lim=b->buf+1024;
  return b;
}

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

static void write_table_as_slotmap
  (request_rec *r,apr_table_t *t,BUFF *b,int post_size,char *post_data)
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

struct HEAD_SCANNER {
  fdsocket sock;
  request_rec *req;};

static int scan_fgets(char *buf,int n_bytes,void *stream)
{
  struct HEAD_SCANNER *scan=(struct HEAD_SCANNER *)stream;
  if (scan->sock->socktype==aprsock) {
    apr_socket_t *sock=scan->sock->sockdata.apr;
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
    ap_log_error
      (APLOG_MARK,APLOG_DEBUG,rv,r->server,
       "mod_fdserv/scan_fgets: Read header string %s",buf);
    if (write>=limit) return write-buf;
    else return write-buf;}
  else if (scan->sock->socktype==filesock) {
      int sock=scan->sock->sockdata.fd;
      char bytes[1], *write=buf, *limit=buf+n_bytes;; 
      while (read(sock,bytes,1)>0) { 
	if (write>=limit) break; else *write++=bytes[0];
	if (bytes[0] == '\n') break;}
      *write='\0';
      if (write>=limit) return write-buf;
      else return write-buf;}
  else {
    /*
    ap_log_error
      (APLOG_MARK,APLOG_CRIT,500,r->server,"Bad fdsocket passed");
    */
    return -1;}
}


static int sock_write(request_rec *r,
		      const unsigned char *buf,long int n_bytes,
		      fdsocket sockval)
{
  if (sockval->socktype==filesock)
    ap_log_error
      (APLOG_MARK,APLOG_DEBUG,OK,r->server,"Writing %ld bytes to file socket %d for %s",
       n_bytes,(sockval->sockdata.fd),sockval->sockname);
  else if (sockval->socktype==aprsock)
    ap_log_error
      (APLOG_MARK,APLOG_DEBUG,OK,r->server,"Writing %ld bytes to APR socket for %s",
       n_bytes,sockval->sockname);
  else {}

  if (sockval->socktype==aprsock) {
    apr_socket_t *sock=sockval->sockdata.apr;
    apr_size_t bytes_to_write=n_bytes, block_size=n_bytes;
    apr_ssize_t bytes_written=0;
    while (bytes_written < bytes_to_write) {
      apr_status_t rv=apr_socket_send(sock,buf+bytes_written,&block_size);
      if ((block_size<0)||(rv!=APR_SUCCESS)) {
	char errbuf[256]="unknown", *err;
	err=apr_strerror(rv,errbuf,256);
	ap_log_error
	  (APLOG_MARK,APLOG_DEBUG,OK,r->server,
	   "Error (%s) from APR socket for %s after %ld/%ld/%ld bytes",
	   errbuf,sockval->sockname,
	   (long int)bytes_written,(long int)bytes_to_write,
	   n_bytes);
	break;}
      else if (block_size == 0) {
	ap_log_error
	  (APLOG_MARK,APLOG_DEBUG,OK,r->server,
	   "Zero blocks written of %ld on APR socket for %s",
	   ((long int)(bytes_to_write-bytes_written)),
	   sockval->sockname);
	break;}
      else {
	bytes_written=bytes_written+block_size;
	bytes_to_write=bytes_to_write-bytes_written;
#if HEAVY_DEBUGGING
	ap_log_error
	  (APLOG_MARK,APLOG_DEBUG,OK,
	   r->server,"Wrote %ld (%ld/%ld) bytes to APR socket for %s",
	   block_size,bytes_written,n_bytes,
	   sockval->sockname);
#endif
	block_size=bytes_to_write;}}
    ap_log_error
      (APLOG_MARK,((bytes_written!=n_bytes)?(APLOG_CRIT):(APLOG_DEBUG)),OK,
       r->server,"Wrote %ld bytes to APR socket for %s",
       ((long int)bytes_written),sockval->sockname);
    return bytes_written;}
  else if (sockval->socktype==filesock) {
    int sock=sockval->sockdata.fd;
    long int bytes_written=0, bytes_to_write=n_bytes;
    ap_log_error
      (APLOG_MARK,APLOG_DEBUG,OK,r->server,
       "Writing %ld bytes to file socket %d",
       bytes_to_write,sock);
    while (bytes_written < n_bytes) {
      int block_size=write(sock,buf,n_bytes-bytes_written);
      if (block_size<0) {
	ap_log_error
	  (APLOG_MARK,APLOG_DEBUG,OK,r->server,
	   "Error %d from %d after %ld/%ld bytes",
	   errno,sock,bytes_written,bytes_to_write);
	break;}
      else if (block_size == 0) {
	ap_log_error
	  (APLOG_MARK,APLOG_DEBUG,OK,r->server,
	   "Zero blocks written of %ld,sock=%d",
	   (long int)(bytes_to_write-bytes_written),sock);
	break;}
      bytes_written=bytes_written+block_size;
#if HEAVY_DEBUGGING
      ap_log_error
	(APLOG_MARK,APLOG_DEBUG,OK,r->server,
	 "Wrote %d bytes (%ld/%ld), %ld to go to file socket %d",
	 block_size,bytes_written,n_bytes,
	 bytes_to_write-bytes_written,sock);
#endif
    }
    ap_log_error
      (APLOG_MARK,((bytes_written!=n_bytes)?(APLOG_CRIT):(APLOG_DEBUG)),OK,
       r->server,"Wrote %ld bytes to file socket (%d) for %s",
       ((long int)bytes_written),sock,sockval->sockname);
    return bytes_written;}
  else {
    ap_log_error
      (APLOG_MARK,APLOG_CRIT,500,r->server,"Bad fdsocket passed");
    return -1;}
}

static void copy_script_output(fdsocket sockval,request_rec *r)
{
  char buf[4096]; apr_size_t bytes_read=0;
  if (sockval->socktype==aprsock) {
    apr_socket_t *sock=sockval->sockdata.apr;
    while (1) {
      apr_size_t delta=4096;
      apr_status_t rv;
      rv=apr_socket_recv(sock,buf,&delta);
#if HEAVY_DEBUGGING
      ap_log_error
	(APLOG_MARK,APLOG_DEBUG,OK,r->server,
	 "mod_fdserv: Read %ld %sbytes of %ld (so far)",
	 (long int)delta,((bytes_read>0)?"more ":""),
	 (long int)bytes_read+delta);
#endif
      if (delta>0) {
	bytes_read=bytes_read+delta;
	ap_rwrite(buf,delta,r); ap_rflush(r);}
      else if (rv!=OK) {
	ap_log_error
	  (APLOG_MARK,APLOG_DEBUG,rv,r->server,
	   "mod_fdserv: Stopped after reading %ld bytes",
	   (long int)bytes_read);
	break;}
      else if (errno==EAGAIN) continue;
      else break;}
    ap_log_error
      (APLOG_MARK,APLOG_INFO,OK,r->server,
       "mod_fdserv: Copied %ld content bytes to client",
       (long int)bytes_read);}
  else if (sockval->socktype==filesock) {
    int sock=sockval->sockdata.fd;
    while (1) {
      ssize_t delta=read(sock,buf,4096);
#if HEAVY_DEBUGGING
      ap_log_error
	(APLOG_MARK,APLOG_DEBUG,OK,r->server,
	 "mod_fdserv: Read %ld bytes of %ld (so far)",
	 (long int)delta,(long int)bytes_read);
#endif
      if (delta>0) {
	bytes_read=bytes_read+delta;
	ap_rwrite(buf,delta,r); ap_rflush(r);}
      else if (delta==0) break;
      else if (errno==EAGAIN) continue;
      else break;}
    ap_log_error
      (APLOG_MARK,APLOG_DEBUG,OK,r->server,
       "mod_fdserv: Finished reading %ld bytes",(long int)bytes_read);}
  else ap_log_error
	 (APLOG_MARK,APLOG_CRIT,500,r->server,"Bad fdsocket passed");
}

/* May be used for debugging */
static void log_buf(char *msg,int size,char *data,request_rec *r)
{
  char *buf=apr_palloc(r->pool,size*2+1);
  int i=0, j=0; while (i<size) {
    int byte=data[i++];
    char hi=(byte>>4), lo=byte&0x0F;
    if (hi<10) buf[j++]='0'+hi;
    else buf[j++]='A'+hi;
    if (lo<10) buf[j++]='0'+lo;
    else buf[j++]='A'+lo;}
  buf[j]='\0';
  ap_log_error(APLOG_MARK,APLOG_DEBUG,OK,r->server,
	       "mod_fdserv: (%s) %d bytes of data: %s",
	       msg,size,buf);
}

static int fdserv_handler(request_rec *r) /* 2.0 */
{
  BUFF *reqdata;
  fdsocket sock;
  char *post_data, errbuf[512];
  int post_size; int bytes_written=0;
  struct HEAD_SCANNER scanner;
  struct FDSERV_SERVER_CONFIG *sconfig=
    ap_get_module_config(r->server->module_config,&fdserv_module);
  int using_dtblock=((sconfig->is_async<0)?(default_isasync):
		     ((sconfig->is_async)?(1):(0)));
#if TRACK_EXECUTION_TIMES
  struct timeb start, end; 
#endif
  if(strcmp(r->handler, FDSERV_MAGIC_TYPE) && strcmp(r->handler, "fdservlet"))
    return DECLINED;
  else if (!((r->method_number == M_GET) ||
	     (r->method_number == M_POST) ||
	     (r->method_number == M_OPTIONS)))
    return DECLINED;
  ap_log_rerror(APLOG_MARK,APLOG_DEBUG,OK,r,
		"Entered fdserv_handler for %s from %s",
		r->filename,r->unparsed_uri);
  sock=connect_to_servlet(r);
  errno=0; if (!(sock)) {
    ap_log_error(APLOG_MARK,APLOG_ERR,OK,r->server,
		 "mod_fdserv: Connection failed to %s for %s",
		 r->filename,r->unparsed_uri);
    r->content_type="text/html";
    ap_send_http_header(r);
    ap_rvputs(r,"<HTML>\n<HEAD>\n<TITLE>",NULL);
    ap_rprintf(r,"Cannot start fdservlet for %s",r->filename);
    ap_rvputs(r,"</HEAD>\n<BODY>\n<H1>",NULL);
    ap_rprintf(r,"Cannot start fdservlet for %s",r->filename);
    ap_rvputs(r,"</BODY>\n</HTML>",NULL);
    return HTTP_SERVICE_UNAVAILABLE;}
  else {
#if TRACK_EXECUTION_TIMES
    ftime(&start);
#endif
    ap_log_error(APLOG_MARK,APLOG_DEBUG,OK,r->server,
		 "mod_fdserv: %s serves %s for %s",
		 ((sock->socktype==filesock)?"File socket":"APR socket"),
		 r->filename,r->unparsed_uri);}
  reqdata=ap_bcreate(r->pool,0);
  ap_log_error(APLOG_MARK,APLOG_DEBUG,OK,r->server,
	       "mod_fdserv: Handling request for %s by using %s",
	       r->unparsed_uri,r->filename);
  ap_add_common_vars(r); ap_add_cgi_vars(r);
  if (r->method_number == M_POST) {
    char bigbuf[4096];
    ap_setup_client_block(r,REQUEST_CHUNKED_ERROR);
    if (ap_should_client_block(r)) {
      int bytes_read, size=0, limit=16384;
      char *data=apr_palloc(r->pool,16384);
      while ((bytes_read=ap_get_client_block(r,bigbuf,4096)) > 0) {
	ap_log_error(APLOG_MARK,APLOG_DEBUG,OK,r->server,
		     "mod_fdserv: Read %d bytes of POST data from client",
		     bytes_read);
	ap_reset_timeout(r);
	if (size+bytes_read > limit) {
	  char *newbuf=apr_palloc(r->pool,limit*2);
	  memcpy(newbuf,data,size);
	  data=newbuf; limit=limit*2;}
	memcpy(data+size,bigbuf,bytes_read); size=size+bytes_read;}
      post_data=data; post_size=size;}
    else {post_data=NULL; post_size=0;}}
  else {post_data=NULL; post_size=0;}

  ap_log_error(APLOG_MARK,APLOG_DEBUG,OK,r->server,
	       "mod_fdserv: Composing request data as a slotmap");

  /* Write the slotmap into buf, the write the buf to the servlet socket */
  write_table_as_slotmap(r,r->subprocess_env,reqdata,post_size,post_data);
  ap_log_error(APLOG_MARK,APLOG_DEBUG,OK,r->server,
	       "mod_fdserv: Writing %ld bytes of request data to socket",
	       (long int)(reqdata->ptr-reqdata->buf));
  /* log_buf("REQDATA",reqdata->ptr-reqdata->buf,reqdata->buf,r); */

  if (using_dtblock) {
    unsigned char buf[8];
    unsigned int nbytes=reqdata->ptr-reqdata->buf;
    buf[0]=0x14;
    buf[1]=((nbytes>>24)&0xF);
    buf[2]=((nbytes>>16)&0xF);
    buf[3]=((nbytes>>8)&0xF);
    buf[4]=((nbytes>>0)&0xF);
    bytes_written=sock_write(r,buf,5,sock);}
  if ((using_dtblock)&&(bytes_written<5))
    ap_log_error(APLOG_MARK,APLOG_CRIT,OK,r->server,
		 "mod_fdserv: Only wrote %ld bytes of request data to socket",
		 (long int)bytes_written);
  else {
    bytes_written=sock_write(r,reqdata->buf,reqdata->ptr-reqdata->buf,sock);
    if (bytes_written<(reqdata->ptr-reqdata->buf))
      ap_log_error(APLOG_MARK,APLOG_CRIT,OK,r->server,
		   "mod_fdserv: Only wrote %ld bytes of request data to socket",
		   (long int)bytes_written);
    else ap_log_error(APLOG_MARK,APLOG_DEBUG,OK,r->server,
		      "mod_fdserv: Finished writing %ld bytes of request data to socket",
		      (long int)(reqdata->ptr-reqdata->buf));}

  scanner.sock=sock; scanner.req=r;
  
  ap_log_error(APLOG_MARK,APLOG_DEBUG,OK,r->server,
	       "mod_fdserv: Waiting for response from servlet");
  
  ap_scan_script_header_err_core(r,errbuf,scan_fgets,(void *)&scanner);
  
  ap_log_error(APLOG_MARK,APLOG_DEBUG,OK,r->server,
	       "mod_fdserv: Read servlet header, passing to client");
  ap_send_http_header(r);
  
  ap_log_error(APLOG_MARK,APLOG_DEBUG,OK,r->server,
	       "mod_fdserv: Header sent, now sending content from servlet");
  copy_script_output(sock,r);
  
  ap_log_error(APLOG_MARK,APLOG_DEBUG,OK,r->server,
	       "mod_fdserv: Done sending content");
  
#if TRACK_EXECUTION_TIMES
  {char buf[64]; double interval; ftime(&end);
    interval=1000*(end.time-start.time)+(end.millitm-start.millitm);
    sprintf(buf,"%lf",interval/1000.0);
    apr_table_set(r->subprocess_env,"EXECUTED","yes");
    apr_table_set(r->notes,"exectime",buf);}
#endif
  if (sock->socktype==aprsock) {
    ap_log_error(APLOG_MARK,APLOG_DEBUG,OK,r->server,
		 "mod_fdserv: Closing network socket for %s",
		 sock->sockname);
    apr_socket_close(sock->sockdata.apr);}
  else if (sock->socktype==filesock) {
    ap_log_error(APLOG_MARK,APLOG_DEBUG,OK,r->server,
		 "mod_fdserv: Closing file socket (%d) for %s",
		 sock->sockdata.fd,sock->sockname);
    close(sock->sockdata.fd);}
  else {}
  return OK;
}

int revision=-1;
char *version_num="1.5";
char version_info[256];

static void init_version_info()
{
  sscanf(revisioninfo+10,"%d",&revision);
  sprintf(version_info,"mod_fdserv/%s(%d)",version_num,revision);
}

static int fdserv_init(apr_pool_t *p, apr_pool_t *plog, apr_pool_t *ptemp,
			server_rec *s)
{
  struct FDSERV_SERVER_CONFIG *sconfig=
    ap_get_module_config(s->module_config,&fdserv_module);
  if (sconfig->socket_prefix==NULL)
    sconfig->socket_prefix=apr_pstrdup(p,"/var/run/fdserv/");
  if (sconfig->socket_prefix[0]=='/') {
    const char *dirname=sconfig->socket_prefix;
    int retval=apr_dir_make_recursive(dirname,RUN_FDSERV_PERMISSIONS,p);
    if (retval)
      ap_log_error
	(APLOG_MARK,APLOG_CRIT,retval,s,
	 "mod_fdserv: Problem with creating socket prefix directory %s",dirname);
    else if ((sconfig->uid>=0) && (sconfig->gid>=0))
      retval=chown(dirname,sconfig->uid,sconfig->gid);
    if (retval)
      ap_log_error
	(APLOG_MARK,APLOG_CRIT,retval,s,
	 "mod_fdserv: Problem with setting owner/group on socket prefix directory %s",dirname);
    else ap_log_error
	   (APLOG_MARK,APLOG_NOTICE,retval,s,
	    "mod_fdserv: Using socket prefix directory %s",dirname);}
  socketname_table=apr_table_make(p,64);
  init_version_info();
  ap_add_version_component(p,version_info);
  ap_log_error(APLOG_HEAD,s,
	       "mod_fdserv v%s (rev%d) init for Apache 2.x  completed",
	       version_num,revision);
  return OK;
}
static void register_hooks(apr_pool_t *p)
{
  /* static const char * const run_first[]={ "mod_mime",NULL }; */
  ap_hook_handler(fdserv_handler, NULL, NULL, APR_HOOK_LAST);
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

/* Emacs local variables
;;;  Local variables: ***
;;;  compile-command: "cd ..; make mod_fdserv" ***
;;;  End: ***
*/

