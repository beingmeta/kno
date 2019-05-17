/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/defines.h"
#include "kno/dtype.h"
#include "kno/tables.h"
#include "kno/numbers.h"
#include "kno/eval.h"
#include "kno/storage.h"
#include "kno/webtools.h"
#include "kno/ports.h"
#include "kno/fileprims.h"

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

#include "main.h"

#ifndef KNO_WITH_FASTCGI
#define KNO_WITH_FASTCGI 0
#endif

#if KNO_WITH_FASTCGI
#include <fcgiapp.h>
#endif

#include "main.c"

static u8_condition Startup=_("KNOCGI/Startup");

KNO_EXPORT void kno_init_webtools(void);
KNO_EXPORT void kno_init_texttools(void);

#include "webcommon.h"

static int servlet_threads = 8;
/* This is the backlog of connection requests not transactions.
   It is passed as the argument to listen() */
static int max_backlog = -1;

/* Writing the PID file */

static void shutdown_server()
{
  if (server_shutdown)
    return;
  else server_shutdown = 1;
}

/* Running the server */

static lispval reqsetup()
{
  lispval result = VOID;
  /* Do this ASAP to avoid session leakage */
  kno_reset_threadvars();
  /* Update modules */
  if (kno_update_file_modules(0)<0) {
    u8_condition c = NULL; u8_context cxt = NULL;
    u8_string details = NULL;
    lispval irritant = VOID;
    if (kno_poperr(&c,&cxt,&details,&irritant))
      result = kno_err(c,cxt,details,irritant);
    if (details) u8_free(details);
    kno_decref(irritant);}
  else if (update_preloads()<0) {
    u8_condition c = NULL; u8_context cxt = NULL;
    u8_string details = NULL;
    lispval irritant;
    if (kno_poperr(&c,&cxt,&details,&irritant))
      result = kno_err(c,cxt,details,irritant);
    if (details) u8_free(details);
    kno_decref(irritant);}
  return result;
}

static void copy_envparam(char *name,lispval target,lispval slotid)
{
  char *param = getenv(name);
  lispval value = ((param) ? (fdstring(param)) : (VOID));
  if (!(KNO_VOIDP(value))) kno_add(target,slotid,value);
  kno_decref(value);
}

static lispval get_envcgidata()
{
  lispval slotmap = kno_empty_slotmap();
  char *lenstring = getenv("CONTENT_LENGTH");
  if (lenstring) {
    lispval packet = VOID;
    int len = atoi(lenstring);
    /* char *ctype = getenv("CONTENT_TYPE"); */
    char *buf = u8_malloc(len);
    if (fgets(buf,len,stdin) == NULL) return KNO_ERROR_VALUE;
    packet = kno_init_packet(NULL,len,buf);
    kno_store(slotmap,post_data,packet);}
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

static char *socketspec = NULL;

#if KNO_WITH_FASTCGI

static int fcgi_socket = -1;

static void copy_param(char *name,FCGX_ParamArray envp,lispval target,lispval slotid)
{
  char *param = FCGX_GetParam(name,envp);
  lispval value = ((param) ? (fdstring(param)) : (VOID));
  if (!(KNO_VOIDP(value))) kno_add(target,slotid,value);
  kno_decref(value);
}

static void output_content(FCGX_Request *req,lispval content)
{
  if (KNO_STRINGP(content))
    FCGX_PutStr(CSTRING(content),STRLEN(content),req->out);
  else if (KNO_PACKETP(content))
    FCGX_PutStr(KNO_PACKET_DATA(content),KNO_PACKET_LENGTH(content),req->out);
  else {}
}

static lispval get_fcgidata(FCGX_Request *req)
{
  lispval slotmap = kno_empty_slotmap();
  char *lenstring = FCGX_GetParam("CONTENT_LENGTH",req->envp);
  if (lenstring) {
    lispval packet = VOID;
    char *ctype = FCGX_GetParam("CONTENT_TYPE",req->envp);
    int len = atoi(lenstring);
    char *buf = u8_malloc(len);
    int read_len = FCGX_GetStr(buf,len,req->in);
    if (len!=read_len)
      u8_log(LOG_CRIT,"Wrong number of bytes","In FastCGI input processing");
    packet = kno_init_packet(NULL,read_len,buf);
    kno_store(slotmap,post_data,packet);}
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
  int write_headers = 1;
  double start_time = u8_elapsed_time();
  double setup_time, parse_time, exec_time, write_time;
  struct KNO_THREAD_CACHE *threadcache = NULL;
  struct rusage start_usage, end_usage;
  lispval proc = reqsetup(), result = VOID, cgidata = VOID, path = VOID;
  if (KNO_ABORTP(proc)) {
    parse_time = setup_time = u8_elapsed_time();}
  else {
    lispval uri;
    setup_time = u8_elapsed_time();
    cgidata = get_fcgidata(req);
    path = kno_get(cgidata,script_filename,VOID);
    if (traceweb>0) {
      uri = kno_get(cgidata,uri_symbol,VOID);
      if (KNO_STRINGP(uri))
        u8_log(LOG_NOTICE,"REQUEST","Handling request for %s",CSTRING(uri));
      kno_decref(uri);}
    proc = getcontent(path);
    kno_parse_cgidata(cgidata);
    parse_time = u8_elapsed_time();
    if ((reqlog) || (urllog))
      dolog(cgidata,KNO_NULL,NULL,parse_time-start_time);}
  u8_getrusage(RUSAGE_SELF,&start_usage);
  u8_set_default_output(out);
  kno_use_reqinfo(cgidata); kno_reqlog(1);
  kno_thread_set(browseinfo_symbol,EMPTY);
  if (KNO_ABORTP(proc)) result = kno_incref(proc);
  else if (KNO_LAMBDAP(proc)) {
    struct KNO_LAMBDA *sp = KNO_CONSPTR(kno_lambda,proc);
    if (traceweb>1)
      u8_log(LOG_NOTICE,"START","Handling %q with lambda procedure %q",path,proc);
    threadcache = checkthreadcache(sp->env);
    result = kno_cgiexec(proc,cgidata);}
  else if ((KNO_PAIRP(proc)) && (KNO_LAMBDAP((KNO_CAR(proc))))) {
    struct KNO_LAMBDA *sp = KNO_CONSPTR(kno_lambda,KNO_CAR(proc));
    if (traceweb>1)
      u8_log(LOG_NOTICE,"START","Handling %q with lambda procedure %q",path,proc);
    threadcache = checkthreadcache(sp->env);
    result = kno_cgiexec(KNO_CAR(proc),cgidata);}
  else if (KNO_PAIRP(proc)) {
    lispval xml = KNO_CAR(proc), setup_proc = VOID;
    kno_lexenv base = kno_consptr(kno_lexenv,KNO_CDR(proc),kno_lexenv_type);
    kno_lexenv runenv = kno_make_env(kno_incref(cgidata),base);
    if (base) kno_load_latest(NULL,base,NULL);
    threadcache = checkthreadcache(base);
    if (traceweb>1)
      u8_log(LOG_NOTICE,"START","Handling %q with template",path);
    setup_proc = kno_symeval(setup_symbol,base);
    if (KNO_VOIDP(setup_proc)) {}
    else if (KNO_CHOICEP(setup_proc)) {
      KNO_DO_CHOICES(proc,setup_proc)
        if (KNO_APPLICABLEP(proc)) {
          lispval v = kno_apply(proc,0,NULL);
          kno_decref(v);}}
    else if (KNO_APPLICABLEP(setup_proc)) {
      lispval v = kno_apply(setup_proc,0,NULL);
      kno_decref(v);}
    kno_decref(setup_proc);
    write_headers = 0;
    kno_output_xml_preface(out,cgidata);
    if (KNO_PAIRP(KNO_CAR(proc))) {
      KNO_DOLIST(expr,KNO_CAR(proc)) {
        kno_decref(result);
        result = kno_xmleval(out,expr,runenv);
        if (KNO_ABORTP(result)) break;}}
    else result = kno_xmleval(out,KNO_CAR(proc),runenv);
    kno_decref((lispval)runenv);}
  exec_time = u8_elapsed_time();
  u8_set_default_output(NULL);
  if (KNO_TROUBLEP(result)) {
    u8_exception ex = u8_erreify();
    u8_condition excond = ex->u8x_cond;
    u8_context excxt = ((ex->u8x_context) ?
                      (ex->u8x_context) :
                      ((u8_context)"somewhere"));
    u8_context exdetails = ((ex->u8x_details) ?
                          (ex->u8x_details) :
                          ((u8_string)"no more details"));
    lispval irritant = kno_exception_xdata(ex);
    if (KNO_VOIDP(irritant))
      u8_log(LOG_INFO,excond,"Unexpected error \"%m \"for %s:@%s (%s)",
             excond,CSTRING(path),excxt,exdetails);
    else u8_log(LOG_INFO,excond,"Unexpected error \"%m\" for %s:%s (%s) %q",
                excond,CSTRING(path),excxt,exdetails,irritant);
    FCGX_PutS("Content-type: text/html; charset = utf-8\r\n\r\n",req->out);
    kno_xhtmlerrorpage(out,ex);
    u8_free_exception(ex,1);
    if ((reqlog) || (urllog))
      dolog(cgidata,result,out->u8_outbuf,u8_elapsed_time()-start_time);
    FCGX_PutStr(out->u8_outbuf,out->u8_write-out->u8_outbuf,req->out);}
  else {
    U8_OUTPUT tmp; int retval, tracep;
    lispval content = kno_get(cgidata,content_slotid,VOID);
    lispval traceval = kno_get(cgidata,tracep_slotid,VOID);
    if (KNO_VOIDP(traceval)) tracep = 0; else tracep = 1;
    U8_INIT_STATIC_OUTPUT(tmp,1024);
    kno_output_http_headers(&tmp,cgidata);
    u8_putn(&tmp,"\r\n",2);
    if ((cgitrace)&&(tracep)) fprintf(stderr,"%s\n",tmp.u8_outbuf);
    FCGX_PutStr(tmp.u8_outbuf,tmp.u8_write-tmp.u8_outbuf,req->out);
    tmp.u8_write = tmp.u8_outbuf;
    if (KNO_VOIDP(content)) {
      if (write_headers) {
        write_headers = kno_output_xhtml_preface(&tmp,cgidata);
        FCGX_PutStr(tmp.u8_outbuf,tmp.u8_write-tmp.u8_outbuf,req->out);}
      retval = FCGX_PutStr(out->u8_outbuf,out->u8_write-out->u8_outbuf,req->out);
      if (write_headers) FCGX_PutS("</body>\n</html>\n",req->out);}
    else {
      output_content(req,content);
      out->u8_write = out->u8_outbuf;}
    u8_free(tmp.u8_outbuf); kno_decref(content); kno_decref(traceval);
    if ((reqlog) || (urllog))
      dolog(cgidata,result,out->u8_outbuf,u8_elapsed_time()-start_time);}
  if (threadcache) kno_pop_threadcache(threadcache);
  kno_use_reqinfo(EMPTY); kno_reqlog(-1);
  kno_thread_set(browseinfo_symbol,VOID);
  write_time = u8_elapsed_time();
  u8_getrusage(RUSAGE_SELF,&end_usage);
  if (traceweb>0) {
    lispval query = kno_get(cgidata,query_string,VOID);
    if (KNO_VOIDP(query))
      u8_log(LOG_NOTICE,"DONE","Handled %q in %f = setup:%f+req:%f+run:%f+write:%f secs, stime=%.2fms, utime=%.2fms.",
             path,write_time-start_time,
             setup_time-start_time,
             parse_time-setup_time,
             exec_time-parse_time,
             write_time-exec_time,
             (u8_dbldifftime(end_usage.ru_utime,start_usage.ru_utime))/1000.0,
             (u8_dbldifftime(end_usage.ru_stime,start_usage.ru_stime))/1000.0);
    else u8_log(LOG_NOTICE,"DONE","Handled %q q=%q in %f = setup:%f+req:%f+run:%f+write:%f secs, stime=%.2fms, utime=%.2fms.",
                path,query,
                write_time-start_time,
                setup_time-start_time,
                parse_time-setup_time,
                exec_time-parse_time,
                write_time-exec_time,
                (u8_dbldifftime(end_usage.ru_utime,start_usage.ru_utime))/1000.0,
                (u8_dbldifftime(end_usage.ru_stime,start_usage.ru_stime))/1000.0);
    /* If we're calling traceweb, keep the log files up to date also. */
    u8_lock_mutex(&log_lock);
    if (urllog) fflush(urllog);
    if (reqlog) kno_flush_stream(reqlog);
    u8_unlock_mutex(&log_lock);
    kno_decref(query);}
  kno_decref(proc); kno_decref(cgidata);
  kno_decref(result); kno_decref(path);
  kno_swapcheck();
  return 1;
}

static kno_ptrbits fcgiserveloop(kno_ptrbits socket)
{
  int retval;
  FCGX_Request req;
  struct U8_OUTPUT out;
  U8_INIT_STATIC_OUTPUT(out,8192*4);
  u8_log(LOG_CRIT,"knocgi","Starting loop listening to socket %d",socket);
  if (retval = FCGX_InitRequest(&req,socket,0))
    return retval;
  else while ((retval = FCGX_Accept_r(&req))==0) {
      fcgiservefn(&req,&out);
      FCGX_Finish_r(&req);
      out.u8_write = out.u8_outbuf; out.u8_outbuf[0]='\0';
#if 0
      if ((retval = FCGX_InitRequest(&req,socket,0))!=0) break;
#endif
    }
  FCGX_Free(&req,1);
  u8_free(out.u8_outbuf);
  return retval;
}

static void *fcgitop(void *s)
{
  return (void *) fcgiserveloop((kno_ptrbits)s);
}

static int start_fcgi_server(char *socketspec)
{
  pthread_t *threads;
  int each_thread = 0, fcgi_socket = -1;

  u8_log(LOG_DEBUG,Startup,"FCGX_Init");

  FCGX_Init();

  u8_log(LOG_CRIT,"OpenSocket","FCGX_Init done");

  if (socketspec) {
    u8_log(LOG_NOTICE,Startup,"Opening fastcgi socket '%s'",socketspec);
    if (*socketspec!=':')
      if (u8_file_existsp(socketspec)) {
        u8_log(LOG_NOTICE,Startup,"Removing existing fastcgi socket '%s'",socketspec);
        remove(socketspec);}
    fcgi_socket = FCGX_OpenSocket(socketspec,max_backlog);
    if ((*socketspec!=':') && (getenv("UNDERGDB")))
      u8_log(LOG_CRIT,Startup,"Setting fastcgi socket '%s' to be world-writable",socketspec);
      chmod(socketspec,0777);}
  else {
    char *socknov = getenv("FCGI_LISTENSOCK_FILENO");
    if (socknov) {
      fcgi_socket = (atoi(socknov));
      u8_log(LOG_NOTICE,Startup,"Opening fastcgi fileno '%s'=%d",socknov,fcgi_socket);}
    else {
      fcgi_socket = 0;
      u8_log(LOG_NOTICE,Startup,"Listening on default fileno 0");}}

  u8_log(LOG_DEBUG,Startup,"Creating/Starting threads");

  threads = u8_alloc_n(servlet_threads,pthread_t);
  each_thread = 0; while (each_thread<servlet_threads) {
    pthread_create(&(threads[each_thread]),
                   pthread_attr_default,
                   fcgitop,(void *)((kno_ptrbits)(fcgi_socket)));
    each_thread++;}

  u8_log(LOG_DEBUG,Startup,"Threads started");

  init_webcommon_finalize();

  u8_log(LOG_DEBUG,Startup,"Signals setup");

 if (socketspec) {
   write_pid_file(socketspec);
   portfile = u8_strdup(socketspec);}

 u8_log(LOG_NOTICE,NULL,
        "Kno (%s) knocgi servlet running, %d/%d pools/indexes",
        KNO_REVISION,kno_n_pools,
        kno_n_primary_indexes+kno_n_secondary_indexes);
 u8_message("beingmeta Kno, (C) beingmeta 2004-2019, all rights reserved");
 each_thread = 0; while (each_thread<servlet_threads) {
   void *threadval;
   int retval = pthread_join(threads[each_thread],(void **)&threadval);
   each_thread++;}

 if (pidfile) {
   u8_removefile(pidfile);
   u8_free(pidfile);
   pidfile = NULL;}

}
#endif

/* SLOW CGI */

static int simplecgi(lispval path)
{
  int write_headers = 1, retval;
  double start_time = u8_elapsed_time();
  double setup_time, parse_time, exec_time, write_time;
  struct rusage start_usage, end_usage;
  lispval proc = reqsetup(), result = VOID, cgidata = VOID;
  struct U8_OUTPUT out; U8_INIT_STATIC_OUTPUT(out,16384);
  if (KNO_ABORTP(proc)) {
    parse_time = setup_time = u8_elapsed_time();}
  else {
    lispval uri;
    setup_time = u8_elapsed_time();
    cgidata = get_envcgidata();
    if (docroot) webcommon_adjust_docroot(cgidata,docroot);
    if (traceweb>0) {
      uri = kno_get(cgidata,uri_slotid,VOID);
      if (KNO_STRINGP(uri))
        u8_log(LOG_NOTICE,"REQUEST","Handling request for %s",CSTRING(uri));
      kno_decref(uri);}
    proc = getcontent(path);
    kno_parse_cgidata(cgidata);
    parse_time = u8_elapsed_time();
    if ((reqlog) || (urllog))
      dolog(cgidata,KNO_NULL,NULL,0,parse_time-start_time);}
  u8_getrusage(RUSAGE_SELF,&start_usage);
  kno_use_reqinfo(cgidata); kno_reqlog(1);
  kno_thread_set(browseinfo_symbol,EMPTY);
  u8_set_default_output(&out);
  if (KNO_ABORTP(proc)) result = kno_incref(proc);
  else if (KNO_LAMBDAP(proc)) {
    if (traceweb>1)
      u8_log(LOG_NOTICE,"START","Handling %q with lambda procedure %q",path,proc);
    result = kno_cgiexec(proc,cgidata);}
  else if ((KNO_PAIRP(proc)) && (KNO_LAMBDAP((KNO_CAR(proc))))) {
    if (traceweb>1)
      u8_log(LOG_NOTICE,"START","Handling %q with lambda procedure %q",path,proc);
    result = kno_cgiexec(KNO_CAR(proc),cgidata);}
  else if (KNO_PAIRP(proc)) {
    lispval setup_proc = VOID;
    kno_lexenv base = kno_consptr(kno_lexenv,KNO_CDR(proc),kno_lexenv_type);
    kno_lexenv runenv = kno_make_env(kno_incref(cgidata),base);
    if (base) kno_load_latest(NULL,base,NULL);
    if (traceweb>1)
      u8_log(LOG_NOTICE,"START","Handling %q with template",path);
    setup_proc = kno_symeval(setup_symbol,base);
    if (KNO_VOIDP(setup_proc)) {}
    else if (CHOICEP(setup_proc)) {
      KNO_DO_CHOICES(proc,setup_proc)
        if (KNO_APPLICABLEP(proc)) {
          lispval v = kno_apply(proc,0,NULL);
          kno_decref(v);}}
    else if (KNO_APPLICABLEP(setup_proc)) {
      lispval v = kno_apply(setup_proc,0,NULL);
      kno_decref(v);}
    kno_decref(setup_proc);
    write_headers = 0;
    kno_output_xml_preface(&out,cgidata);
    if (KNO_PAIRP(KNO_CAR(proc))) {
      KNO_DOLIST(expr,KNO_CAR(proc)) {
        kno_decref(result);
        result = kno_xmleval(&out,expr,runenv);
        if (KNO_ABORTP(result)) break;}}
    else result = kno_xmleval(&out,KNO_CAR(proc),runenv);
    kno_decref((lispval)runenv);}
  exec_time = u8_elapsed_time();
  if (KNO_TROUBLEP(result)) {
    u8_exception ex = u8_erreify();
    u8_condition excond = ex->u8x_cond;
    u8_context excxt = ((ex->u8x_context) ?
                      (ex->u8x_context) :
                      ((u8_context)"somewhere"));
    u8_context exdetails=
      ((ex->u8x_details) ? (ex->u8x_details) : ((u8_string)"no more details"));
    lispval irritant = kno_exception_xdata(ex);
    if (KNO_VOIDP(irritant))
      u8_log(LOG_INFO,excond,"Unexpected error \"%m \"for %s:@%s (%s)",
             excond,CSTRING(path),excxt,exdetails);
    else u8_log(LOG_INFO,excond,"Unexpected error \"%m\" for %s:%s (%s) %q",
                excond,CSTRING(path),excxt,exdetails,irritant);
    fputs("Content-type: text/html; charset = utf-8\r\n\r\n",stdout);
    kno_xhtmlerrorpage(&out,ex);
    retval = fwrite(out.u8_outbuf,1,out.u8_write-out.u8_outbuf,stdout);
    u8_free_exception(ex,1);}
  else {
    U8_OUTPUT tmp; int tracep;
    lispval content = kno_get(cgidata,content_slotid,VOID);
    lispval traceval = kno_get(cgidata,tracep_slotid,VOID);
    if (KNO_VOIDP(traceval)) tracep = 0; else tracep = 1;
    U8_INIT_STATIC_OUTPUT(tmp,1024);
    kno_output_http_headers(&tmp,cgidata);
    u8_putn(&tmp,"\r\n",2);
    if ((cgitrace)&&(tracep)) fprintf(stderr,"%s\n",tmp.u8_outbuf);
    retval = fwrite(tmp.u8_outbuf,1,tmp.u8_write-tmp.u8_outbuf,stdout);
    tmp.u8_write = tmp.u8_outbuf;
    if (KNO_VOIDP(content)) {
      if (write_headers) {
        write_headers = kno_output_xhtml_preface(&tmp,cgidata);
        retval = fwrite(tmp.u8_outbuf,1,tmp.u8_write-tmp.u8_outbuf,stdout);
        retval = fwrite(out.u8_outbuf,1,out.u8_write-out.u8_outbuf,stdout);}
      if (write_headers) fputs("</body>\n</html>\n",stdout);}
    else {
      if (KNO_STRINGP(content)) {
        retval = fwrite(CSTRING(content),1,STRLEN(content),stdout);}
      else if (PACKETP(content))
        retval = fwrite(KNO_PACKET_DATA(content),1,KNO_PACKET_LENGTH(content),
                      stdout);}
    u8_free(tmp.u8_outbuf); kno_decref(content); kno_decref(traceval);}
  if (retval<0)
    u8_log(LOG_ERROR,"BADRET","Bad retval from writing data");
  u8_set_default_output(NULL);
  kno_use_reqinfo(EMPTY); kno_reqlog(-1);
  kno_thread_set(browseinfo_symbol,VOID);
  write_time = u8_elapsed_time();
  u8_getrusage(RUSAGE_SELF,&end_usage);
  if (traceweb>0) {
    lispval query = kno_get(cgidata,query_string,VOID);
    if (KNO_VOIDP(query))
      u8_log(LOG_NOTICE,"DONE","Handled %q in %f = setup:%f+req:%f+run:%f+write:%f secs, stime=%.2fms, utime=%.2fms.",
             path,write_time-start_time,
             setup_time-start_time,
             parse_time-setup_time,
             exec_time-parse_time,
             write_time-exec_time,
             (u8_dbldifftime(end_usage.ru_utime,start_usage.ru_utime))/1000.0,
             (u8_dbldifftime(end_usage.ru_stime,start_usage.ru_stime))/1000.0);
    else u8_log(LOG_NOTICE,"DONE","Handled %q q=%q in %f = setup:%f+req:%f+run:%f+write:%f secs, stime=%.2fms, utime=%.2fms.",
                path,query,
                write_time-start_time,
                setup_time-start_time,
                parse_time-setup_time,
                exec_time-parse_time,
                write_time-exec_time,
                (u8_dbldifftime(end_usage.ru_utime,start_usage.ru_utime))/1000.0,
                (u8_dbldifftime(end_usage.ru_stime,start_usage.ru_stime))/1000.0);
    /* If we're calling traceweb, keep the log files up to date also. */
    u8_lock_mutex(&log_lock);
    if (urllog) fflush(urllog);
    if (reqlog) kno_flush_stream(reqlog);
    u8_unlock_mutex(&log_lock);
    kno_decref(query);}
  kno_decref(proc); kno_decref(cgidata);
  kno_decref(result); kno_decref(path);
  return 1;
}

/* The main() event */

KNO_EXPORT int kno_init_drivers(void);

static void webcommon_atexit()
{
  webcommon_shutdown("atexit");
}

int main(int argc,char **argv)
{
  int i = 1;
  int u8_version = u8_initialize(), kno_version;
  /* Mask of args which we handle */
  unsigned char arg_mask[argc];  memset(arg_mask,0,argc);
  unsigned char *loadfile = NULL;

  kno_main_errno_ptr = &errno;

  KNO_INIT_STACK();

  if (u8_version<0) {
    u8_log(LOG_ERROR,"STARTUP","Can't initialize LIBU8");
    exit(1);}

  u8_log(LOG_INFO,Startup,"LOGFILE='%s'",getenv("LOGFILE"));

  /* We do this using the Unix environment (rather than configuration
      variables) because we want to redirect errors from the configuration
      variables themselves and we want to be able to set this in the
      environment we wrap around calls. */
  if (getenv("LOGFILE")) {
    char *logfile = u8_strdup(getenv("LOGFILE"));
    int log_fd = open(logfile,O_RDWR|O_APPEND|O_CREAT|O_SYNC,0644);
    if (log_fd<0) {
      u8_log(LOG_WARN,Startup,"Couldn't open log file %s",logfile);
      exit(1);}
    dup2(log_fd,1);
    dup2(log_fd,2);}

  kno_version = kno_init_scheme();

  if (kno_version<0) {
    u8_log(LOG_WARN,Startup,"Couldn't initialize Kno");
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

  u8_log_show_date = 1;
  u8_log_show_procinfo = 1;
  u8_use_syslog(1);

#if ((!(HAVE_CONSTRUCTOR_ATTRIBUTES)) || (KNO_TESTCONFIG))
  kno_init_scheme();
  kno_init_schemeio();
  kno_init_texttools();
#else
  KNO_INIT_SCHEME_BUILTINS();
#endif

  kno_init_webtools();
  kno_init_drivers();

  atexit(webcommon_atexit);

  init_webcommon_data();
  init_webcommon_symbols();

  if (server_env == NULL)
    server_env = kno_working_lexenv();
  kno_idefn((lispval)server_env,kno_make_cprim0("BOOT-TIME",get_boot_time));
  kno_idefn((lispval)server_env,kno_make_cprim0("UPTIME",get_uptime));

  init_webcommon_configs();
  kno_register_config("BACKLOG",
                     _("Number of pending connection requests allowed"),
                     kno_intconfig_get,kno_intconfig_set,&max_backlog);
  kno_register_config("NTHREADS",_("Number of threads in the thread pool"),
                     kno_intconfig_get,kno_intconfig_set,&servlet_threads);
  kno_register_config("CGISOCK",_("The socket file used by this server for use with external versions"),
                     kno_sconfig_get,kno_sconfig_set,&socketspec);

  u8_init_mutex(&log_lock);

  while (i<argc) {
    if (isconfig(argv[i])) {
      u8_log(LOG_NOTICE,"CONFIG","   %s",argv[i++]);}
    else if (loadfile) i++;
    else {
      arg_mask[i] = 'X';
      loadfile = argv[i++];}}

  if (socketspec == NULL) {
    socketspec = getenv("FCGISOCK");
    if (socketspec) socketspec = u8_strdup(socketspec);}

  kno_handle_argv(argc,argv,arg_mask,NULL);

  KNO_NEW_STACK(((struct KNO_STACK *)NULL),"knocgi",NULL,VOID);
  _stack->stack_label=u8_strdup(u8_appid());
  _stack->stack_free_label=1;

  u8_log(LOG_DEBUG,Startup,"Updating preloads");
  /* Initial handling of preloads */
  if (update_preloads()<0) {
    /* Error here, rather than repeatedly */
    kno_clear_errors(1);
    exit(EXIT_FAILURE);}

  kno_boot_message();
  u8_now(&boot_time);

  init_webcommon_finalize();

  update_preloads();

#if KNO_WITH_FASTCGI
  if ((loadfile) || (FCGX_IsCGI()))
    simplecgi(fdstring(loadfile));
  else start_fcgi_server(socketspec);
#else
  if (loadfile == NULL) {
    u8_log(LOG_CRIT,"No file","No script file specified");}
  else simplecgi(fdstring(loadfile));
#endif
  return 0;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
