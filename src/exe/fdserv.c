/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "fdb/dtype.h"
#include "fdb/tables.h"
#include "fdb/eval.h"
#include "fdb/fddb.h"
#include "fdb/fdweb.h"
#include "fdb/ports.h"
#include "fdb/fileprims.h"

#include <libu8/libu8io.h>
#include <libu8/u8timefns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8netfns.h>
#include <libu8/u8srvfns.h>

#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <time.h>
#include <signal.h>

#include "revision.h"

FD_EXPORT void fd_init_fdweb(void);
FD_EXPORT void fd_init_texttools(void);

/* Logging declarations */
static u8_mutex log_lock;
static u8_string urllogname=NULL;
static FILE *urllog=NULL;
static u8_string reqlogname=NULL;
static fd_dtype_stream reqlog=NULL;
static int reqloglevel=0;
static int traceweb=0;

static char *pidfile;

#define FD_REQERRS 1 /* records only transactions which return errors */
#define FD_ALLREQS 2 /* records all requests */
#define FD_ALLRESP 3 /* records all requests and the response set back */


typedef struct FD_WEBCONN {
  U8_CLIENT_FIELDS;
  struct FD_DTYPE_STREAM in;
  struct U8_OUTPUT out;} FD_WEBCONN;
typedef struct FD_WEBCONN *fd_webconn;

static fdtype cgisymbol, main_symbol, setup_symbol, script_filename, uri_symbol;
static fdtype response_symbol, err_symbol, cgidata_symbol;
static fdtype http_headers, html_headers, doctype_slotid, xmlpi_slotid;
static fdtype content_slotid, content_type, tracep_slotid, query_symbol;
static fd_lispenv server_env;
struct U8_SERVER fdwebserver;

/* This environment is the parent of all script/page environments spun off
   from this server. */
static fd_lispenv server_env=NULL;
static int servlet_backlog=-1, servlet_threads=-1;

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
      fdtype logstart_entry=fd_init_string(NULL,-1,logstart);
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
      if (FD_ERRORP(val)) {
	struct FD_EXCEPTION_OBJECT *eo=(FD_EXCEPTION_OBJECT *)val;
	if (eo->data.cxt)
	  tmp=u8_mkstring("!%s\n@%*lt %g/%g %s %s\n",FD_STRDATA(uri),
			  exectime,u8_elapsed_time(),
			  eo->data.cond,eo->data.cxt);
	else tmp=u8_mkstring("!%s\n@%*lt %g/%g %s\n",FD_STRDATA(uri),
			     exectime,u8_elapsed_time(),eo->data.cond);}
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
    u8_warn("Couldn't write file","Couldn't write PID file %s",pidfile);
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

static fdtype loadcontent(fdtype path)
{
  u8_string pathname=FD_STRDATA(path), oldsource;
  double load_start=u8_elapsed_time();
  u8_string content=u8_filestring(pathname,NULL);
  if (traceweb>0)
    u8_notify("LOADING","Loading %s",pathname);
  if (content[0]=='<') {
    U8_INPUT in; FD_XML *xml; fd_lispenv env;
    fdtype lenv, ldata;
    U8_INIT_STRING_INPUT(&in,strlen(content),content);
    oldsource=fd_bind_sourcebase(pathname);
    xml=fd_read_fdxml(&in,(FD_SLOPPY_XML|FD_XML_KEEP_RAW));
    fd_restore_sourcebase(oldsource);
    if (xml==NULL) {
      u8_free(content);
      u8_warn("ERROR","Error parsing %s",pathname);
      return fd_erreify();}
    env=(fd_lispenv)xml->data;
    lenv=(fdtype)env; ldata=xml->head;
    if (traceweb>0)
      u8_notify("LOADED","Loaded %s in %f secs",
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
    if (FD_ERRORP(load_result)) return load_result;
    fd_decref(load_result);
    main_proc=fd_eval(main_symbol,newenv);
    fd_decref((fdtype)newenv);
    if (traceweb>0)
      u8_notify("LOADED","Loaded %s in %f secs",
		pathname,u8_elapsed_time()-load_start);
    return main_proc;}
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
	return fd_erreify();}
      u8_offtime(&mtime,fileinfo.st_mtime,0);
      content=loadcontent(path);
      if (FD_ABORTP(content)) {
	u8_free(lpath); return content;}
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
	return fd_erreify();}
      else if (fileinfo.st_mtime>lmtime->xtime.u8_secs) {
	fdtype new_content=loadcontent(path);
	struct U8_XTIME mtime;
	u8_offtime(&mtime,fileinfo.st_mtime,0);
	fd_hashtable_store
	  (&pagemap,path,
	   fd_init_pair(NULL,fd_make_timestamp(&mtime),
			fd_incref(new_content)));
	u8_free(lpath); fd_decref(value);
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
    fdtype uri;
    setup_time=u8_elapsed_time();
    cgidata=fd_dtsread_dtype(&(client->in)), result;
    path=fd_get(cgidata,script_filename,FD_VOID);
    if (traceweb>0) {
      uri=fd_get(cgidata,uri_symbol,FD_VOID);
      if (FD_STRINGP(uri)) 
	u8_notify("REQUEST","Handling request for %s",FD_STRDATA(uri));
      fd_decref(uri);}
    proc=getcontent(path);
    fd_parse_cgidata(cgidata);
    parse_time=u8_elapsed_time();
    if ((reqlog) || (urllog))
      dolog(cgidata,FD_NULL,NULL,parse_time-start_time);}
  fd_set_default_output(&(client->out));
  if (FD_ABORTP(proc)) result=fd_incref(proc);
  else if (FD_PRIM_TYPEP(proc,fd_sproc_type)) {
    if (traceweb>1)
      u8_notify("START","Handling %q with Scheme procedure %q",path,proc);
    result=fd_cgiexec(proc,cgidata);}
  else if (FD_PAIRP(proc)) {
    fdtype xml=FD_CAR(proc), lenv=FD_CDR(proc), setup_proc=FD_VOID;
    fd_lispenv base=((FD_PTR_TYPEP(lenv,fd_environment_type)) ?
		     (FD_GET_CONS(FD_CDR(proc),fd_environment_type,fd_environment)) :
		     (NULL));
    fd_lispenv runenv=fd_make_env(fd_incref(cgidata),base);
    if (base) fd_load_latest(NULL,base,NULL);
    if (traceweb>1)
      u8_notify("START","Handling %q with template",path);
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
    fd_thread_set(cgidata_symbol,cgidata);
    if (FD_PAIRP(FD_CAR(proc))) {
      FD_DOLIST(expr,FD_CAR(proc)) {
	fd_decref(result);
	result=fd_xmleval(&(client->out),expr,runenv);
	if (FD_ABORTP(result)) break;}}
    else result=fd_xmleval(&(client->out),FD_CAR(proc),runenv);
    fd_thread_set(cgidata_symbol,FD_VOID);
    fd_decref((fdtype)runenv);}
  exec_time=u8_elapsed_time();
  fd_set_default_output(NULL);
  if (FD_ERRORP(result)) {
    struct FD_EXCEPTION_OBJECT *eo=(FD_EXCEPTION_OBJECT *)result;
    if (FD_VOIDP(eo->data.irritant))
      u8_message("Unexpected error \"%m \"for %s:@%s (%s)",
		 eo->data.cond,FD_STRDATA(path),
		 ((eo->data.cxt) ? (eo->data.cxt) : ((u8_context)"somewhere")),
		 ((eo->data.details) ? (eo->data.details) : ((u8_string)"no more details")));
    else u8_message("Unexpected error \"%m\" for %s:%s (%s) %q",
		    eo->data.cond,FD_STRDATA(path),
		    ((eo->data.cxt) ? (eo->data.cxt) : ((u8_context)"somewhere")),
		    ((eo->data.details) ? (eo->data.details) : ((u8_string)"no more details")),
		    eo->data.irritant);
    write_string(client->socket,
		 "Content-type: text/html; charset='utf-8'\r\n\r\n");
    fd_xhtmlerrorpage(&(client->out),result);
    if ((reqlog) || (urllog))
      dolog(cgidata,result,client->out.u8_outbuf,u8_elapsed_time()-start_time);
    u8_writeall(client->socket,client->out.u8_outbuf,
		client->out.u8_outptr-client->out.u8_outbuf);}
  else {
    U8_OUTPUT tmp; int retval, tracep;
    fdtype content=fd_get(cgidata,content_slotid,FD_VOID);
    fdtype traceval=fd_get(cgidata,tracep_slotid,FD_VOID);
    if (FD_VOIDP(traceval)) tracep=0; else tracep=1;
    U8_INIT_OUTPUT(&tmp,1024);
    fd_output_http_headers(&tmp,cgidata);
    /* if (tracep) fprintf(stderr,"%s\n",tmp.u8_outbuf); */
    u8_writeall(client->socket,tmp.u8_outbuf,tmp.u8_outptr-tmp.u8_outbuf);
    tmp.u8_outptr=tmp.u8_outbuf;
    if (FD_VOIDP(content)) {
      if (write_headers) {
	write_headers=fd_output_xhtml_preface(&tmp,cgidata);
	u8_writeall(client->socket,tmp.u8_outbuf,tmp.u8_outptr-tmp.u8_outbuf);}
      retval=u8_writeall(client->socket,client->out.u8_outbuf,
			 client->out.u8_outptr-client->out.u8_outbuf);
      if (write_headers)
	write_string(client->socket,"</body>\n</html>\n");}
    else {
      output_content(client,content);
      client->out.u8_outptr=client->out.u8_outbuf;}
    u8_free(tmp.u8_outbuf); fd_decref(content); fd_decref(traceval);
    if ((reqlog) || (urllog))
      dolog(cgidata,result,client->out.u8_outbuf,u8_elapsed_time()-start_time);}
  write_time=u8_elapsed_time();
  if (traceweb>0) {
    fdtype query=fd_get(cgidata,query_symbol,FD_VOID);
    if (FD_VOIDP(query))
      u8_notify("DONE","Handled %q in %f=setup:%f+req:%f+run:%f+write:%f secs.",
		path,write_time-start_time,
		setup_time-start_time,
		parse_time-setup_time,
		exec_time-parse_time,
		write_time-exec_time);
    else u8_notify("DONE","Handled %q q=%q in %f=setup:%f+req:%f+run:%f+write:%f secs",
		   path,query,
		   write_time-start_time,
		   setup_time-start_time,
		   parse_time-setup_time,
		   exec_time-parse_time,
		   write_time-exec_time);
    /* If we're calling traceweb, keep the log files up to date also. */
    fd_lock_mutex(&log_lock);
    if (urllog) fflush(urllog);
    if (reqlog) fd_dtsflush(reqlog);
    fd_unlock_mutex(&log_lock);
    fd_decref(query);}
  fd_decref(proc); fd_decref(cgidata);
  fd_decref(result); fd_decref(path);
  u8_client_close(ucl);
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

static void shutdown_fdwebserver()
{
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
  shutdown_fdwebserver();
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
  html_headers=fd_intern("HTML-HEADERS");
  http_headers=fd_intern("HTTP-HEADERS");
  tracep_slotid=fd_intern("TRACEP");
  err_symbol=fd_intern("%ERR");
  response_symbol=fd_intern("%RESPONSE");
  cgidata_symbol=fd_intern("CGIDATA");
}

/* The main() event */

static void doexit(int sig)
{
  exit(0);
}

FD_EXPORT void fd_init_dbfile(void); 

int main(int argc,char **argv)
{
  int fd_version=fd_init_fdscheme();
  unsigned char data[1024], *input;
  int i=2, n_threads=-1, n_tasks=-1;
  u8_string source_file=NULL;
  if (argc<2) {
    fprintf(stderr,"Usage: fdserv <socketfile> [config]*\n");
    exit(2);}

  
  /* We doe this using the Unix environment (rather than configuration
      variables) because we want to redirect errors from the configuration
      variables themselves and we want to be able to set this in the
      environment we wrap around calls. */
  if (getenv("LOGFILE")) {
    /* This doesn't seem to work and I don't know why. */
    char *logfile=u8_strdup(getenv("LOGFILE"));
    int log_fd=open(logfile,O_RDWR|O_APPEND|O_CREAT|O_SYNC,0644);
    if (log_fd<0) {
      u8_warn("Couldn't open log file %s",logfile);
      exit(1);}
    dup2(log_fd,1);
    dup2(log_fd,2);}

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

  u8_show_procinfo=1;
  u8_use_syslog(1);

#if ((!(HAVE_CONSTRUCTOR_ATTRIBUTES)) || (FD_TESTCONFIG))
  fd_init_fdscheme();
  fd_init_schemeio();
  fd_init_texttools();
#else
  FD_INIT_SCHEME_BUILTINS();
#endif

  fd_init_fdweb();
  fd_init_dbfile(); 
  init_symbols();
  
  fd_register_config("TRACEWEB",traceweb_get,traceweb_set,NULL);
  fd_register_config("PRELOAD",preload_get,preload_set,NULL);
  fd_register_config("THREADS",fd_intconfig_get,fd_intconfig_set,&servlet_threads);
  fd_register_config("BACKLOG",fd_intconfig_get,fd_intconfig_set,&servlet_backlog);
  fd_register_config("URLLOG",urllog_get,urllog_set,NULL);
  fd_register_config("REQLOG",reqlog_get,reqlog_set,NULL);
  fd_register_config("REQLOGLEVEL",fd_intconfig_get,fd_intconfig_set,&reqloglevel);
#if FD_THREADS_ENABLED
  fd_init_mutex(&log_lock);
#endif

  u8_notify("LAUNCH","fdserv %s",argv[1]);

  while (i<argc)
    if (strchr(argv[i],'=')) {
      u8_notify("CONFIG","   %s",argv[i]);
      fd_config_assignment(argv[i++]);}
    else i++;
  if (u8_file_existsp(argv[1])) remove(argv[1]);
  
  if (servlet_threads<0) servlet_threads=4;
  if (servlet_backlog<0) servlet_backlog=32;

  update_preloads();

  fd_make_hashtable(&pagemap,0);
  u8_server_init(&fdwebserver,
		 servlet_backlog,servlet_threads,
		 simply_accept,webservefn,close_webclient);
  fdwebserver.flags=fdwebserver.flags|U8_SERVER_LOG_LISTEN;
  atexit(shutdown_fdwebserver);

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
     u8_warn("Sigerror","Error setting signal mask");
 }
#elif HAVE_SIGSETMASK
  /* We set this here because otherwise, it will often inherit
     the signal mask of its apache parent, which is inappropriate. */
 sigsetmask(0);
#endif

  if (u8_add_server(&fdwebserver,argv[1],-1)<0) {
    fd_recycle_hashtable(&pagemap);
    fd_clear_errors(1);
    return -1;}
  chmod(argv[1],0777);

  write_pid_file(argv[1]);

  portfile=u8_strdup(argv[1]);

  u8_message("FramerD (r%s) fdserv servlet running, %d/%d pools/indices",
	     SVN_REVISION,fd_n_pools,
	     fd_n_primary_indices+fd_n_secondary_indices);
  u8_message("beingmeta FramerD, (C) beingmeta 2004-2006, all rights reserved");
  u8_server_loop(&fdwebserver);

  if (pidfile) u8_removefile(pidfile);
  pidfile=NULL;

  return 0;
}

