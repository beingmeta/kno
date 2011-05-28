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
#include <libu8/u8rusage.h>

#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <time.h>
#include <signal.h>

#include "revision.h"

#ifndef FD_WITH_FASTCGI
#define FD_WITH_FASTCGI 0
#endif

#if FD_WITH_FASTCGI
#include <fcgiapp.h>
#endif

static u8_condition Startup=_("FDCGIEXEC/Startup");

FD_EXPORT void fd_init_fdweb(void);
FD_EXPORT void fd_init_texttools(void);
FD_EXPORT void fd_init_tagger(void);

/* Logging declarations */
static u8_mutex log_lock;
static u8_string urllogname=NULL;
static FILE *urllog=NULL;
static u8_string reqlogname=NULL;
static fd_dtype_stream reqlog=NULL;
static int reqloglevel=0;
static int traceweb=0;

static char *pidfile;

static fd_exception CantWriteSocket=_("Can't write to socket");

/* When the server started, used by UPTIME */
static struct U8_XTIME boot_time;

#define FD_REQERRS 1 /* records only transactions which return errors */
#define FD_ALLREQS 2 /* records all requests */
#define FD_ALLRESP 3 /* records all requests and the response set back */

static fdtype cgisymbol, main_symbol, setup_symbol, script_filename, uri_symbol;
static fdtype response_symbol, err_symbol, cgidata_symbol, browseinfo_symbol;
static fdtype http_headers, html_headers, doctype_slotid, xmlpi_slotid;
static fdtype content_slotid, content_type, tracep_slotid, query_string;
static fdtype query_string, script_name, path_info;
static fdtype http_referer, http_accept_language, http_accept_encoding, http_accept_charset, http_accept;
static fdtype content_type, content_length, post_data;
static fdtype server_port, server_name, path_translated, script_filename;
static fdtype auth_type, remote_host, remote_user, remote_port, http_cookie, request_method;

static fd_lispenv server_env;

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

static void write_pid_file(char *sockname)
{
  FILE *f;
  int len=strlen(sockname);
  char *dot=strchr(sockname,'.');
  pidfile=u8_malloc(len+8);
  if (dot) {
    strncpy(pidfile,sockname,dot-sockname);
    pidfile[dot-sockname]='\0';}
  else strcpy(pidfile,sockname);
  strcat(pidfile,".pid");
  f=fopen(pidfile,"w");
  if (f==NULL)
    u8_log(LOG_WARN,Startup,"Couldn't write file","Couldn't write PID file %s",pidfile);
  else {
    fprintf(f,"%d",getpid());
    fclose(f);}
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

/* Running the server */

static fdtype reqsetup()
{
  fdtype result=FD_VOID;
  /* Do this ASAP to avoid session leakage */
  fd_reset_threadvars();
  /* Update modules */
  if (fd_update_file_modules(0)<0) {
    u8_condition c; u8_context cxt; u8_string details=NULL;
    fdtype irritant;
    if (fd_poperr(&c,&cxt,&details,&irritant)) 
      result=fd_err(c,cxt,details,irritant);
    if (details) u8_free(details); fd_decref(irritant);}
  else if (update_preloads()<0) {
    u8_condition c; u8_context cxt; u8_string details=NULL;
    fdtype irritant;
    if (fd_poperr(&c,&cxt,&details,&irritant)) 
      result=fd_err(c,cxt,details,irritant);
    if (details) u8_free(details); fd_decref(irritant);}
  return result;
}

static void copy_envparam(char *name,fdtype target,fdtype slotid)
{
  char *param=getenv(name);
  fdtype value=((param) ? (fdtype_string(param)) : (FD_VOID));
  if (!(FD_VOIDP(value))) fd_add(target,slotid,value);
  fd_decref(value);
}

static fdtype get_envcgidata()
{
  int retval=0;
  fdtype slotmap=fd_init_slotmap(NULL,0,NULL);
  char *lenstring=getenv("CONTENT_LENGTH");
  if (lenstring) {
    fdtype packet=FD_VOID;
    int len=atoi(lenstring);
    char *ctype=getenv("CONTENT_TYPE");
    char *buf=u8_malloc(len);
    if (fgets(buf,len,stdin)==NULL) return FD_ERROR_VALUE;
    packet=fd_init_packet(NULL,len,buf);
    fd_store(slotmap,post_data,packet);}
  copy_envparam("QUERY_STRING",slotmap,query_string);
  copy_envparam("SCRIPT_NAME",slotmap,script_name);
  copy_envparam("PATH_INFO",slotmap,path_info);
  copy_envparam("HTTP_REFERER",slotmap,http_referer);
  copy_envparam("HTTP_ACCEPT_LANGUAGE",slotmap,http_accept_language);
  copy_envparam("HTTP_ACCEPT_ENCODING",slotmap,http_accept_encoding);
  copy_envparam("HTTP_ACCEPT_CHARSET",slotmap,http_accept_charset);
  copy_envparam("HTTP_ACCEPT",slotmap,http_accept);
  copy_envparam("CONTENT_TYPE",slotmap,content_type);
  copy_envparam("CONTENT_LENGTH",slotmap,content_length);
  copy_envparam("SERVER_PORT",slotmap,server_port);
  copy_envparam("SERVER_NAME",slotmap,server_name);
  copy_envparam("PATH_TRANSLATED",slotmap,path_translated);
  copy_envparam("PATH_TRANSLATED",slotmap,script_filename);
  copy_envparam("AUTH_TYPE",slotmap,auth_type);
  copy_envparam("REMOTE_HOST",slotmap,remote_host);
  copy_envparam("REMOTE_USER",slotmap,remote_user);
  copy_envparam("REMOTE_PORT",slotmap,remote_port);
  copy_envparam("HTTP_COOKIE",slotmap,http_cookie);
  copy_envparam("REQUEST_METHOD",slotmap,request_method);
  return slotmap;
}

/* Fast CGI */

static char *socketspec=NULL;

#if FD_WITH_FASTCGI

static int fcgi_socket=-1;
static char *portfile=NULL;

static void copy_param(char *name,FCGX_ParamArray envp,fdtype target,fdtype slotid)
{
  char *param=FCGX_GetParam(name,envp);
  fdtype value=((param) ? (fdtype_string(param)) : (FD_VOID));
  if (!(FD_VOIDP(value))) fd_add(target,slotid,value);
  fd_decref(value);
}

static void output_content(FCGX_Request *req,fdtype content)
{
  if (FD_STRINGP(content))
    FCGX_PutStr(FD_STRDATA(content),FD_STRLEN(content),req->out);
  else if (FD_PACKETP(content))
    FCGX_PutStr(FD_PACKET_DATA(content),FD_PACKET_LENGTH(content),req->out);
  else {}
}

static fdtype get_fcgidata(FCGX_Request *req)
{
  fdtype slotmap=fd_init_slotmap(NULL,0,NULL);
  char *lenstring=FCGX_GetParam("CONTENT_LENGTH",req->envp);
  if (lenstring) {
    fdtype packet=FD_VOID;
    char *ctype=FCGX_GetParam("CONTENT_TYPE",req->envp);
    int len=atoi(lenstring);
    char *buf=u8_malloc(len);
    int read_len=FCGX_GetStr(buf,len,req->in);
    if (len!=read_len) u8_log(LOG_CRIT,"Wrong number of bytes","In FastCGI input processing");
    packet=fd_init_packet(NULL,read_len,buf);
    fd_store(slotmap,post_data,packet);}
  copy_param("QUERY_STRING",req->envp,slotmap,query_string);
  copy_param("SCRIPT_NAME",req->envp,slotmap,script_name);
  copy_param("PATH_INFO",req->envp,slotmap,path_info);
  copy_param("HTTP_REFERER",req->envp,slotmap,http_referer);
  copy_param("HTTP_ACCEPT_LANGUAGE",req->envp,slotmap,http_accept_language);
  copy_param("HTTP_ACCEPT_ENCODING",req->envp,slotmap,http_accept_encoding);
  copy_param("HTTP_ACCEPT_CHARSET",req->envp,slotmap,http_accept_charset);
  copy_param("HTTP_ACCEPT",req->envp,slotmap,http_accept);
  copy_param("CONTENT_TYPE",req->envp,slotmap,content_type);
  copy_param("CONTENT_LENGTH",req->envp,slotmap,content_length);
  copy_param("SERVER_PORT",req->envp,slotmap,server_port);
  copy_param("SERVER_NAME",req->envp,slotmap,server_name);
  copy_param("PATH_TRANSLATED",req->envp,slotmap,path_translated);
  copy_param("PATH_TRANSLATED",req->envp,slotmap,script_filename);
  copy_param("AUTH_TYPE",req->envp,slotmap,auth_type);
  copy_param("REMOTE_HOST",req->envp,slotmap,remote_host);
  copy_param("REMOTE_USER",req->envp,slotmap,remote_user);
  copy_param("REMOTE_PORT",req->envp,slotmap,remote_port);
  copy_param("HTTP_COOKIE",req->envp,slotmap,http_cookie);
  copy_param("REQUEST_METHOD",req->envp,slotmap,request_method);
  return slotmap;
}

static int fcgiservefn(FCGX_Request *req,U8_OUTPUT *out)
{
  int write_headers=1;
  double start_time=u8_elapsed_time();
  double setup_time, parse_time, exec_time, write_time;
  struct rusage start_usage, end_usage;
  fdtype proc=reqsetup(), result=FD_VOID, cgidata=FD_VOID, path=FD_VOID;
  if (FD_ABORTP(proc)) {
    parse_time=setup_time=u8_elapsed_time();}
  else {
    fdtype uri;
    setup_time=u8_elapsed_time();
    cgidata=get_fcgidata(req);
    path=fd_get(cgidata,script_filename,FD_VOID);
    if (traceweb>0) {
      uri=fd_get(cgidata,uri_symbol,FD_VOID);
      if (FD_STRINGP(uri)) 
	u8_log(LOG_NOTICE,"REQUEST","Handling request for %s",FD_STRDATA(uri));
      fd_decref(uri);}
    proc=getcontent(path);
    fd_parse_cgidata(cgidata);
    parse_time=u8_elapsed_time();
    if ((reqlog) || (urllog))
      dolog(cgidata,FD_NULL,NULL,parse_time-start_time);}
  u8_getrusage(RUSAGE_SELF,&start_usage);
  fd_set_default_output(out);
  fd_thread_set(cgidata_symbol,cgidata);
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
    fd_output_xml_preface(out,cgidata);
    if (FD_PAIRP(FD_CAR(proc))) {
      FD_DOLIST(expr,FD_CAR(proc)) {
	fd_decref(result);
	result=fd_xmleval(out,expr,runenv);
	if (FD_ABORTP(result)) break;}}
    else result=fd_xmleval(out,FD_CAR(proc),runenv);
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
    FCGX_PutS("Content-type: text/html; charset='utf-8'\r\n\r\n",req->out);
    fd_xhtmlerrorpage(out,ex);
    u8_free_exception(ex,1);
    if ((reqlog) || (urllog))
      dolog(cgidata,result,out->u8_outbuf,u8_elapsed_time()-start_time);
    FCGX_PutStr(out->u8_outbuf,out->u8_outptr-out->u8_outbuf,req->out);}
  else {
    U8_OUTPUT tmp; int retval, tracep;
    fdtype content=fd_get(cgidata,content_slotid,FD_VOID);
    fdtype traceval=fd_get(cgidata,tracep_slotid,FD_VOID);
    if (FD_VOIDP(traceval)) tracep=0; else tracep=1;
    U8_INIT_OUTPUT(&tmp,1024);
    fd_output_http_headers(&tmp,cgidata);
    /* if (tracep) fprintf(stderr,"%s\n",tmp.u8_outbuf); */
    FCGX_PutStr(tmp.u8_outbuf,tmp.u8_outptr-tmp.u8_outbuf,req->out);
    tmp.u8_outptr=tmp.u8_outbuf;
    if (FD_VOIDP(content)) {
      if (write_headers) {
	write_headers=fd_output_xhtml_preface(&tmp,cgidata);
	FCGX_PutStr(tmp.u8_outbuf,tmp.u8_outptr-tmp.u8_outbuf,req->out);}
      retval=FCGX_PutStr(out->u8_outbuf,out->u8_outptr-out->u8_outbuf,req->out);
      if (write_headers) FCGX_PutS("</body>\n</html>\n",req->out);}
    else {
      output_content(req,content);
      out->u8_outptr=out->u8_outbuf;}
    u8_free(tmp.u8_outbuf); fd_decref(content); fd_decref(traceval);
    if ((reqlog) || (urllog))
      dolog(cgidata,result,out->u8_outbuf,u8_elapsed_time()-start_time);}
  fd_thread_set(cgidata_symbol,FD_VOID);
  fd_thread_set(browseinfo_symbol,FD_VOID);
  write_time=u8_elapsed_time();
  u8_getrusage(RUSAGE_SELF,&end_usage);
  if (traceweb>0) {
    fdtype query=fd_get(cgidata,query_string,FD_VOID);
    if (FD_VOIDP(query))
      u8_log(LOG_NOTICE,"DONE","Handled %q in %f=setup:%f+req:%f+run:%f+write:%f secs, stime=%.2fms, utime=%.2fms.",
	     path,write_time-start_time,
	     setup_time-start_time,
	     parse_time-setup_time,
	     exec_time-parse_time,
	     write_time-exec_time,
	     (u8_dbldifftime(end_usage.ru_utime,start_usage.ru_utime))/1000.0,
	     (u8_dbldifftime(end_usage.ru_stime,start_usage.ru_stime))/1000.0);
    else u8_log(LOG_NOTICE,"DONE","Handled %q q=%q in %f=setup:%f+req:%f+run:%f+write:%f secs, stime=%.2fms, utime=%.2fms.",
		path,query,
		write_time-start_time,
		setup_time-start_time,
		parse_time-setup_time,
		exec_time-parse_time,
		write_time-exec_time,
		(u8_dbldifftime(end_usage.ru_utime,start_usage.ru_utime))/1000.0,
		(u8_dbldifftime(end_usage.ru_stime,start_usage.ru_stime))/1000.0);
    /* If we're calling traceweb, keep the log files up to date also. */
    fd_lock_mutex(&log_lock);
    if (urllog) fflush(urllog);
    if (reqlog) fd_dtsflush(reqlog);
    fd_unlock_mutex(&log_lock);
    fd_decref(query);}
  fd_decref(proc); fd_decref(cgidata);
  fd_decref(result); fd_decref(path);
  fd_swapcheck();
  return 1;
}

static fd_ptrbits fcgiserveloop(fd_ptrbits socket)
{
  int retval;
  FCGX_Request req;
  struct U8_OUTPUT out;
  U8_INIT_OUTPUT(&out,8192*4);
  u8_log(LOG_CRIT,"fdcgiexec","Starting loop listening to socket %d",socket);
  if (retval=FCGX_InitRequest(&req,socket,0))
    return retval;
  else while ((retval=FCGX_Accept_r(&req))==0) {
      fcgiservefn(&req,&out);
      FCGX_Finish_r(&req);
      out.u8_outptr=out.u8_outbuf; out.u8_outbuf[0]='\0';
#if 0
      if ((retval=FCGX_InitRequest(&req,socket,0))!=0) break;
#endif
    }
  FCGX_Free(&req,1);
  u8_free(out.u8_outbuf);
  return retval;
}

static void *fcgitop(void *s)
{
  return (void *) fcgiserveloop((fd_ptrbits)s);
}

static void shutdown_server()
{
  if (portfile)
    if (remove(portfile)>=0) {
      u8_free(portfile); portfile=NULL;}
  if (pidfile) u8_removefile(pidfile);
  pidfile=NULL;
  fd_recycle_hashtable(&pagemap);
}

static void signal_shutdown(int sig)
{
  shutdown_server();
}

static int start_fcgi_server(char *socketspec)
{
  pthread_t *threads;
  int each_thread=0, fcgi_socket=-1;

  u8_log(LOG_DEBUG,Startup,"FCGX_Init");

  FCGX_Init();

  u8_log(LOG_CRIT,"OpenSocket","FCGX_Init done");

  if (socketspec) {
    u8_log(LOG_NOTICE,Startup,"Opening fastcgi socket '%s'",socketspec);
    if (*socketspec!=':')
      if (u8_file_existsp(socketspec)) {
	u8_log(LOG_NOTICE,Startup,"Removing existing fastcgi socket '%s'",socketspec);	
	remove(socketspec);}
    fcgi_socket=FCGX_OpenSocket(socketspec,max_backlog);
    if ((*socketspec!=':') && (getenv("UNDERGDB")))
      u8_log(LOG_CRIT,Startup,"Setting fastcgi socket '%s' to be world-writable",socketspec);	
      chmod(socketspec,0777);}
  else {
    char *socknov=getenv("FCGI_LISTENSOCK_FILENO");
    if (socknov) {
      fcgi_socket=(atoi(socknov));
      u8_log(LOG_NOTICE,Startup,"Opening fastcgi fileno '%s'=%d",socknov,fcgi_socket);}
    else {
      fcgi_socket=0;
      u8_log(LOG_NOTICE,Startup,"Listening on default fileno 0");}}
  
  u8_log(LOG_DEBUG,Startup,"Creating/Starting threads");

  threads=u8_alloc_n(servlet_threads,pthread_t);
  each_thread=0; while (each_thread<servlet_threads) {
    pthread_create(&(threads[each_thread]),
		   pthread_attr_default,
		   fcgitop,(void *)((fd_ptrbits)(fcgi_socket)));
    each_thread++;}
  
  u8_log(LOG_DEBUG,Startup,"Threads started");

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

 u8_log(LOG_DEBUG,Startup,"Signals setup");

 if (socketspec) {
   write_pid_file(socketspec);
   portfile=u8_strdup(socketspec);}

 u8_log(LOG_NOTICE,NULL,
	"FramerD (%s) fdcgiexec servlet running, %d/%d pools/indices",
	FDB_SVNREV,fd_n_pools,
	fd_n_primary_indices+fd_n_secondary_indices);
 u8_message("beingmeta FramerD, (C) beingmeta 2004-2011, all rights reserved");
 each_thread=0; while (each_thread<servlet_threads) {
   void *threadval;
   int retval=pthread_join(threads[each_thread],(void **)&threadval);
   each_thread++;}
 
 if (pidfile) {
   u8_removefile(pidfile);
   u8_free(pidfile);
   pidfile=NULL;}

}
#endif

/* SLOW CGI */

static int simplecgi(fdtype path)
{
  int write_headers=1, retval;
  double start_time=u8_elapsed_time();
  double setup_time, parse_time, exec_time, write_time;
  struct rusage start_usage, end_usage;
  fdtype proc=reqsetup(), result=FD_VOID, cgidata=FD_VOID;
  struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,16384);
  if (FD_ABORTP(proc)) {
    parse_time=setup_time=u8_elapsed_time();}
  else {
    fdtype uri;
    setup_time=u8_elapsed_time();
    cgidata=get_envcgidata();
    if (traceweb>0) {
      uri=fd_get(cgidata,uri_symbol,FD_VOID);
      if (FD_STRINGP(uri)) 
	u8_log(LOG_NOTICE,"REQUEST","Handling request for %s",FD_STRDATA(uri));
      fd_decref(uri);}
    proc=getcontent(path);
    fd_parse_cgidata(cgidata);
    parse_time=u8_elapsed_time();
    if ((reqlog) || (urllog))
      dolog(cgidata,FD_NULL,NULL,parse_time-start_time);}
  u8_getrusage(RUSAGE_SELF,&start_usage);
  fd_thread_set(cgidata_symbol,cgidata);
  fd_thread_set(browseinfo_symbol,FD_EMPTY_CHOICE);
  fd_set_default_output(&out);
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
    fd_output_xml_preface(&out,cgidata);
    if (FD_PAIRP(FD_CAR(proc))) {
      FD_DOLIST(expr,FD_CAR(proc)) {
	fd_decref(result);
	result=fd_xmleval(&out,expr,runenv);
	if (FD_ABORTP(result)) break;}}
    else result=fd_xmleval(&out,FD_CAR(proc),runenv);
    fd_decref((fdtype)runenv);}
  exec_time=u8_elapsed_time();
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
    fputs("Content-type: text/html; charset='utf-8'\r\n\r\n",stdout);
    fd_xhtmlerrorpage(&out,ex);
    retval=fwrite(out.u8_outbuf,1,out.u8_outptr-out.u8_outbuf,stdout);
    u8_free_exception(ex,1);}
  else {
    U8_OUTPUT tmp; int retval, tracep;
    fdtype content=fd_get(cgidata,content_slotid,FD_VOID);
    fdtype traceval=fd_get(cgidata,tracep_slotid,FD_VOID);
    if (FD_VOIDP(traceval)) tracep=0; else tracep=1;
    U8_INIT_OUTPUT(&tmp,1024);
    fd_output_http_headers(&tmp,cgidata);
    /* if (tracep) fprintf(stderr,"%s\n",tmp.u8_outbuf); */
    retval=fwrite(tmp.u8_outbuf,1,tmp.u8_outptr-tmp.u8_outbuf,stdout);
    tmp.u8_outptr=tmp.u8_outbuf;
    if (FD_VOIDP(content)) {
      if (write_headers) {
	write_headers=fd_output_xhtml_preface(&tmp,cgidata);
	retval=fwrite(tmp.u8_outbuf,1,tmp.u8_outptr-tmp.u8_outbuf,stdout);
	retval=fwrite(out.u8_outbuf,1,out.u8_outptr-out.u8_outbuf,stdout);}
      if (write_headers) fputs("</body>\n</html>\n",stdout);}
    else {
      if (FD_STRINGP(content)) {
	retval=fwrite(FD_STRDATA(content),1,FD_STRLEN(content),stdout);}
      else if (FD_PACKETP(content))
	retval=fwrite(FD_PACKET_DATA(content),1,FD_PACKET_LENGTH(content),stdout);}
    u8_free(tmp.u8_outbuf); fd_decref(content); fd_decref(traceval);}
  fd_set_default_output(NULL);
  fd_thread_set(cgidata_symbol,FD_VOID);
  fd_thread_set(browseinfo_symbol,FD_VOID);
  write_time=u8_elapsed_time();
  u8_getrusage(RUSAGE_SELF,&end_usage);
  if (traceweb>0) {
    fdtype query=fd_get(cgidata,query_string,FD_VOID);
    if (FD_VOIDP(query))
      u8_log(LOG_NOTICE,"DONE","Handled %q in %f=setup:%f+req:%f+run:%f+write:%f secs, stime=%.2fms, utime=%.2fms.",
	     path,write_time-start_time,
	     setup_time-start_time,
	     parse_time-setup_time,
	     exec_time-parse_time,
	     write_time-exec_time,
	     (u8_dbldifftime(end_usage.ru_utime,start_usage.ru_utime))/1000.0,
	     (u8_dbldifftime(end_usage.ru_stime,start_usage.ru_stime))/1000.0);
    else u8_log(LOG_NOTICE,"DONE","Handled %q q=%q in %f=setup:%f+req:%f+run:%f+write:%f secs, stime=%.2fms, utime=%.2fms.",
		path,query,
		write_time-start_time,
		setup_time-start_time,
		parse_time-setup_time,
		exec_time-parse_time,
		write_time-exec_time,
		(u8_dbldifftime(end_usage.ru_utime,start_usage.ru_utime))/1000.0,
		(u8_dbldifftime(end_usage.ru_stime,start_usage.ru_stime))/1000.0);
    /* If we're calling traceweb, keep the log files up to date also. */
    fd_lock_mutex(&log_lock);
    if (urllog) fflush(urllog);
    if (reqlog) fd_dtsflush(reqlog);
    fd_unlock_mutex(&log_lock);
    fd_decref(query);}
  fd_decref(proc); fd_decref(cgidata);
  fd_decref(result); fd_decref(path);
  return 1;
}

/* Initialization/main stuff */

static void init_symbols()
{
  query_string=fd_intern("QUERY_STRING");
  script_name=fd_intern("SCRIPT_NAME");
  path_info=fd_intern("PATH_INFO");
  http_referer=fd_intern("HTTP_REFERER");
  http_accept_language=fd_intern("HTTP_ACCEPT_LANGUAGE");
  http_accept_encoding=fd_intern("HTTP_ACCEPT_ENCODING");
  http_accept_charset=fd_intern("HTTP_ACCEPT_CHARSET");
  http_accept=fd_intern("HTTP_ACCEPT");
  content_type=fd_intern("CONTENT_TYPE");
  content_length=fd_intern("CONTENT_LENGTH");
  server_port=fd_intern("SERVER_PORT");
  server_name=fd_intern("SERVER_NAME");
  path_translated=fd_intern("PATH_TRANSLATED");
  script_filename=fd_intern("SCRIPT_FILENAME");
  auth_type=fd_intern("AUTH_TYPE");
  remote_host=fd_intern("REMOTE_HOST");
  remote_user=fd_intern("REMOTE_USER");
  remote_port=fd_intern("REMOTE_PORT");
  http_cookie=fd_intern("HTTP_COOKIE");
  request_method=fd_intern("REQUEST_METHOD");
  
  uri_symbol=fd_intern("REQUEST_URI");
  main_symbol=fd_intern("MAIN");
  setup_symbol=fd_intern("SETUP");
  cgisymbol=fd_intern("CGIDATA");
  doctype_slotid=fd_intern("DOCTYPE");
  xmlpi_slotid=fd_intern("XMLPI");
  content_slotid=fd_intern("CONTENT");
  html_headers=fd_intern("HTML-HEADERS");
  http_headers=fd_intern("HTTP-HEADERS");
  tracep_slotid=fd_intern("TRACEP");
  err_symbol=fd_intern("%ERR");
  response_symbol=fd_intern("%RESPONSE");
  cgidata_symbol=fd_intern("CGIDATA");
  browseinfo_symbol=fd_intern("BROWSEINFO");
  post_data=fd_intern("POST_DATA");
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
  unsigned char data[1024], *input, *loadfile=NULL;
  int i=1;
  u8_string source_file=NULL;

  u8_log(LOG_INFO,Startup,"LOGFILE='%s'",getenv("LOGFILE"));

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

  fd_version=fd_init_fdscheme();
  
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
#else
  FD_INIT_SCHEME_BUILTINS();
#endif

  fd_init_fdweb();
  fd_init_dbfile(); 
  init_symbols();
  
  if (server_env==NULL)
    server_env=fd_working_environment();
  fd_idefn((fdtype)server_env,fd_make_cprim0("BOOT-TIME",get_boot_time,0));
  fd_idefn((fdtype)server_env,fd_make_cprim0("UPTIME",get_uptime,0));

  fd_register_config("TRACEWEB",_("Trace all web transactions"),
		     traceweb_get,traceweb_set,NULL);
  fd_register_config("PRELOAD",_("Files to preload into the shared environment"),
		     preload_get,preload_set,NULL);
  fd_register_config("BACKLOG",
		     _("Number of pending connection requests allowed"),
		     fd_intconfig_get,fd_intconfig_set,&max_backlog);
  fd_register_config("NTHREADS",_("Number of threads in the thread pool"),
		     fd_intconfig_get,fd_intconfig_set,&servlet_threads);
  fd_register_config("URLLOG",_("Where to write URLs where were requested"),
		     urllog_get,urllog_set,NULL);
  fd_register_config("REQLOG",_("Where to write request objects"),
		     reqlog_get,reqlog_set,NULL);
  fd_register_config("REQLOGLEVEL",_("Level of transaction logging"),
		     fd_intconfig_get,fd_intconfig_set,&reqloglevel);
  fd_register_config("CGISOCK",_("The socket file used by this server for use with external versions"),
		     fd_sconfig_get,fd_sconfig_set,&socketspec);
#if FD_THREADS_ENABLED
  fd_init_mutex(&log_lock);
#endif

  while (i<argc)
    if (strchr(argv[i],'=')) {
      u8_log(LOG_NOTICE,"CONFIG","   %s",argv[i]);
      fd_config_assignment(argv[i++]);}
    else if (loadfile) i++;
    else loadfile=argv[i++];

  if (socketspec==NULL) {
    socketspec=getenv("FCGISOCK");
    if (socketspec) socketspec=u8_strdup(socketspec);}

  u8_log(LOG_DEBUG,Startup,"Updating preloads");

  update_preloads();

  u8_log(LOG_DEBUG,Startup,"Initializing pagemaps");

  fd_make_hashtable(&pagemap,0);

#if FD_WITH_FASTCGI
  if ((loadfile) || (FCGX_IsCGI()))
    simplecgi(fdtype_string(loadfile));
  else start_fcgi_server(socketspec);
#else
  if (loadfile==NULL) {
    u8_log(LOG_CRIT,"No file","No script file specified");}
  else simplecgi(fdtype_string(loadfile));
#endif
  return 0;
}

