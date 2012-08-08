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
#include <libu8/u8stringfns.h>
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

#include "webcommon.h"

static int servlet_threads=8;
/* This is the backlog of connection requests not transactions.
   It is passed as the argument to listen() */
static int max_backlog=-1;

/* Writing the PID file */

#if 0
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
#endif

static void shutdown_server(u8_condition reason)
{
  if (reason) 
    u8_log(LOG_WARN,reason,
	   "Shutting down, removing socket %s and pidfile %s",
	   portfile,pidfile);
  webcommon_shutdown();
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
  fdtype slotmap=fd_empty_slotmap();
  char *lenstring=getenv("CONTENT_LENGTH");
  if (lenstring) {
    fdtype packet=FD_VOID;
    int len=atoi(lenstring);
    /* char *ctype=getenv("CONTENT_TYPE"); */
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
  fdtype slotmap=fd_empty_slotmap();
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
  if (docroot) webcommon_adjust_docroot(slotmap,docroot);
  return slotmap;
}

static int fcgiservefn(FCGX_Request *req,U8_OUTPUT *out)
{
  int write_headers=1;
  double start_time=u8_elapsed_time();
  double setup_time, parse_time, exec_time, write_time;
  struct FD_THREAD_CACHE *threadcache=NULL;
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
  u8_set_default_output(out);
  fd_use_reqinfo(cgidata);
  fd_thread_set(browseinfo_symbol,FD_EMPTY_CHOICE);
  if (FD_ABORTP(proc)) result=fd_incref(proc);
  else if (FD_PRIM_TYPEP(proc,fd_sproc_type)) {
    struct FD_SPROC *sp=FD_GET_CONS(proc,fd_sproc_type,fd_sproc);
    if (traceweb>1)
      u8_log(LOG_NOTICE,"START","Handling %q with Scheme procedure %q",path,proc);
    threadcache=checkthreadcache(sp->env);
    result=fd_cgiexec(proc,cgidata);}
  else if ((FD_PAIRP(proc)) && (FD_PRIM_TYPEP((FD_CAR(proc)),fd_sproc_type))) {
    struct FD_SPROC *sp=FD_GET_CONS(FD_CAR(proc),fd_sproc_type,fd_sproc);
    if (traceweb>1)
      u8_log(LOG_NOTICE,"START","Handling %q with Scheme procedure %q",path,proc);
    threadcache=checkthreadcache(sp->env);
    result=fd_cgiexec(FD_CAR(proc),cgidata);}
  else if (FD_PAIRP(proc)) {
    fdtype xml=FD_CAR(proc), lenv=FD_CDR(proc), setup_proc=FD_VOID;
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
    fd_output_xml_preface(out,cgidata);
    if (FD_PAIRP(FD_CAR(proc))) {
      FD_DOLIST(expr,FD_CAR(proc)) {
	fd_decref(result);
	result=fd_xmleval(out,expr,runenv);
	if (FD_ABORTP(result)) break;}}
    else result=fd_xmleval(out,FD_CAR(proc),runenv);
    fd_decref((fdtype)runenv);}
  exec_time=u8_elapsed_time();
  u8_set_default_output(NULL);
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
    if ((cgitrace)&&(tracep)) fprintf(stderr,"%s\n",tmp.u8_outbuf);
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
  if (threadcache) fd_pop_threadcache(threadcache);
  fd_use_reqinfo(FD_EMPTY_CHOICE);
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

  init_webcommon_finalize();

  u8_log(LOG_DEBUG,Startup,"Signals setup");

 if (socketspec) {
   write_pid_file(socketspec);
   portfile=u8_strdup(socketspec);}

 u8_log(LOG_NOTICE,NULL,
	"FramerD (%s) fdcgiexec servlet running, %d/%d pools/indices",
	FRAMERD_REV,fd_n_pools,
	fd_n_primary_indices+fd_n_secondary_indices);
 u8_message("beingmeta FramerD, (C) beingmeta 2004-2012, all rights reserved");
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
  struct FD_THREAD_CACHE *threadcache=NULL;
  struct rusage start_usage, end_usage;
  fdtype proc=reqsetup(), result=FD_VOID, cgidata=FD_VOID;
  struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,16384);
  if (FD_ABORTP(proc)) {
    parse_time=setup_time=u8_elapsed_time();}
  else {
    fdtype uri;
    setup_time=u8_elapsed_time();
    cgidata=get_envcgidata();
    if (docroot) webcommon_adjust_docroot(cgidata,docroot);
    if (traceweb>0) {
      uri=fd_get(cgidata,uri_symbol,FD_VOID);
      if (FD_STRINGP(uri)) 
	u8_log(LOG_NOTICE,"REQUEST","Handling request for %s",FD_STRDATA(uri));
      fd_decref(uri);}
    proc=getcontent(path);
    fd_parse_cgidata(cgidata);
    parse_time=u8_elapsed_time();
    if ((reqlog) || (urllog))
      dolog(cgidata,FD_NULL,NULL,0,parse_time-start_time);}
  u8_getrusage(RUSAGE_SELF,&start_usage);
  fd_use_reqinfo(cgidata);
  fd_thread_set(browseinfo_symbol,FD_EMPTY_CHOICE);
  u8_set_default_output(&out);
  if (FD_ABORTP(proc)) result=fd_incref(proc);
  else if (FD_PRIM_TYPEP(proc,fd_sproc_type)) {
    struct FD_SPROC *sp=FD_GET_CONS(proc,fd_sproc_type,fd_sproc);
    if (traceweb>1)
      u8_log(LOG_NOTICE,"START","Handling %q with Scheme procedure %q",path,proc);
    threadcache=checkthreadcache(sp->env);
    result=fd_cgiexec(proc,cgidata);}
  else if ((FD_PAIRP(proc)) && (FD_PRIM_TYPEP((FD_CAR(proc)),fd_sproc_type))) {
    struct FD_SPROC *sp=FD_GET_CONS(FD_CAR(proc),fd_sproc_type,fd_sproc);
    if (traceweb>1)
      u8_log(LOG_NOTICE,"START","Handling %q with Scheme procedure %q",path,proc);
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
    u8_context excxt=((ex->u8x_context) ?
		      (ex->u8x_context) :
		      ((u8_context)"somewhere"));
    u8_context exdetails=
      ((ex->u8x_details) ? (ex->u8x_details) : ((u8_string)"no more details"));
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
    U8_OUTPUT tmp; int tracep;
    fdtype content=fd_get(cgidata,content_slotid,FD_VOID);
    fdtype traceval=fd_get(cgidata,tracep_slotid,FD_VOID);
    if (FD_VOIDP(traceval)) tracep=0; else tracep=1;
    U8_INIT_OUTPUT(&tmp,1024);
    fd_output_http_headers(&tmp,cgidata);
    if ((cgitrace)&&(tracep)) fprintf(stderr,"%s\n",tmp.u8_outbuf);
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
	retval=fwrite(FD_PACKET_DATA(content),1,FD_PACKET_LENGTH(content),
		      stdout);}
    u8_free(tmp.u8_outbuf); fd_decref(content); fd_decref(traceval);}
  if (retval<0)
    u8_log(LOG_ERROR,"BADRET","Bad retval from writing data");
  u8_set_default_output(NULL);
  fd_use_reqinfo(FD_EMPTY_CHOICE);
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

/* The main() event */

FD_EXPORT void fd_init_dbfile(void); 

int main(int argc,char **argv)
{
  int u8_version=u8_initialize();
  int fd_version; /* Wait to set this until we have a log file */
  unsigned char *loadfile=NULL;
  int i=1;

  if (u8_version<0) {
    u8_log(LOG_ERROR,"STARTUP","Can't initialize LIBU8");
    exit(1);}

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
  
  if (fd_version<0) {
    u8_log(LOG_WARN,Startup,"Couldn't initialize FramerD");
    exit(1);}

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

  init_webcommon_data();
  init_webcommon_symbols();
  
  if (server_env==NULL)
    server_env=fd_working_environment();
  fd_idefn((fdtype)server_env,fd_make_cprim0("BOOT-TIME",get_boot_time,0));
  fd_idefn((fdtype)server_env,fd_make_cprim0("UPTIME",get_uptime,0));

  init_webcommon_configs();
  fd_register_config("BACKLOG",
		     _("Number of pending connection requests allowed"),
		     fd_intconfig_get,fd_intconfig_set,&max_backlog);
  fd_register_config("NTHREADS",_("Number of threads in the thread pool"),
		     fd_intconfig_get,fd_intconfig_set,&servlet_threads);
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
  /* Initial handling of preloads */
  if (update_preloads()<0) {
    /* Error here, rather than repeatedly */
    fd_clear_errors(1);
    exit(EXIT_FAILURE);}

  init_webcommon_finalize();

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

