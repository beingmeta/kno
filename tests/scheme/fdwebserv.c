/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include <strings.h>
#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <time.h>
#include <libu8/u8.h>
#include <libu8/timefns.h>
#include <libu8/filefns.h>
#include <libu8/netfns.h>
#include <libu8/xfiles.h>
#include "fdb/dtype.h"
#include "fdb/tables.h"
#include "fdb/eval.h"
#include "fdb/fddb.h"
#include "fdb/pools.h"
#include "fdb/fdweb.h"

typedef struct FD_WEBCONN {
  U8_CLIENT_FIELDS;
  struct FD_DTYPE_STREAM in;
  struct U8_OUTPUT out;} FD_WEBCONN;
typedef struct FD_WEBCONN *fd_webconn;

static fdtype cgisymbol, main_symbol, script_filename;
static fdtype http_headers, html_headers, doctype_slotid, xmlpi_slotid;
static fdtype content_slotid, content_type, tracep_slotid;
static fd_lispenv server_env;
struct U8_SERVER fdwebserver;

/* Getting content for pages */

static FD_HASHTABLE pagemap;

static fdtype loadcontent(fdtype path)
{
  u8_string pathname=FD_STRDATA(path);
  u8_string content=u8_filestring(pathname,NULL);
  if (content[0]=='<') {
    U8_INPUT in; FD_XML *xml; fd_lispenv env;
    fdtype lenv, ldata;
    U8_INIT_INPUT(&in,strlen(content),content);
    xml=fd_read_fdxml(&in,FD_SLOPPY_XML);
    env=(fd_lispenv)xml->data;
    lenv=(fdtype)env; ldata=xml->head;
    u8_free(content);
    return fd_init_pair(NULL,ldata,lenv);}
  else {
    fd_environment newenv=fd_working_environment();
    fdtype main_proc, load_result;
    load_result=fd_load_source(pathname,newenv,NULL);
    if (FD_EXCEPTIONP(load_result)) return load_result;
    fd_decref(load_result);
    main_proc=fd_eval(main_symbol,newenv);
    fd_decref((fdtype)newenv);
    u8_free(content);
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
      table_value=fd_init_pair(NULL,fd_make_timestamp(&mtime,NULL),
			       fd_incref(content));
      fd_hashtable_set(&pagemap,path,table_value);
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
	u8_free(lpath);
	return fd_erreify();}
      else if (fileinfo.st_mtime>lmtime->xtime.secs) {
	fdtype new_content=loadcontent(path);
	struct U8_XTIME mtime;
	u8_offtime(&mtime,fileinfo.st_mtime,0);
	fd_hashtable_set(&pagemap,path,
			 fd_init_pair(NULL,fd_make_timestamp(&mtime,NULL),
				      fd_incref(new_content)));
	u8_free(lpath);
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
  fd_webconn consed=u8_malloc(sizeof(FD_WEBCONN));
  consed->socket=sock; consed->flags=0;
  fd_init_dtype_stream(&(consed->in),sock,4096,NULL,NULL);
  U8_INIT_OUTPUT(&(consed->out),8192);
  return (u8_client) consed;
}
static int webservefn(u8_client ucl)
{
  fd_webconn client=(fd_webconn)ucl; int write_headers=1;
  fdtype cgidata=fd_dtsread_dtype(&(client->in)), result;
  fdtype path=fd_get(cgidata,script_filename,FD_VOID), proc=getcontent(path);
  fd_parse_cgidata(cgidata);
  fd_set_default_output(&(client->out));
  if (FD_EXCEPTIONP(proc)) result=fd_incref(proc);
  else if (FD_PRIM_TYPEP(proc,fd_sproc_type))
    result=fd_cgiexec(proc,cgidata);
  else if (FD_PAIRP(proc)) {
    fdtype xml=FD_CAR(proc), lenv=FD_CDR(proc);
    fd_lispenv base=((FD_PRIM_TYPEP(lenv,fd_environment_type)) ?
		     (FD_GET_CONS(FD_CDR(proc),fd_environment_type,fd_environment)) :
		     (NULL));
    fd_lispenv runenv=fd_make_env(fd_incref(cgidata),base);
    write_headers=0;
    result=fd_xmleval(&(client->out),FD_CAR(proc),runenv);
    fd_decref((fdtype)runenv);}
  fd_set_default_output(NULL);
  if (FD_EXCEPTIONP(result)) {
    write_string(client->socket,
		 "Content-type: text/html; charset='utf-8'\r\n\r\n");
    fd_xhtmlerrorpage(&(client->out),result);
    u8_writeall(client->socket,client->out.bytes,
		client->out.point-client->out.bytes);}
  else {
    U8_OUTPUT tmp; int retval, tracep;
    fdtype content=fd_get(cgidata,content_slotid,FD_VOID);
    fdtype traceval=fd_get(cgidata,tracep_slotid,FD_VOID);
    if (FD_VOIDP(traceval)) tracep=0; else tracep=1;
    U8_INIT_OUTPUT(&tmp,1024);
    fd_output_http_headers(&tmp,cgidata);
    /* if (tracep) fprintf(stderr,"%s\n",tmp.bytes); */
    u8_writeall(client->socket,tmp.bytes,tmp.point-tmp.bytes);
    tmp.point=tmp.bytes;
    if (FD_VOIDP(content)) {
      if (write_headers) {
	write_headers=fd_output_xhtml_preface(&tmp,cgidata);
	u8_writeall(client->socket,tmp.bytes,tmp.point-tmp.bytes);}
      /* if (tracep) fprintf(stderr,"%s\n",tmp.bytes); */
      retval=u8_writeall(client->socket,client->out.bytes,
			 client->out.point-client->out.bytes);
      /* if (tracep) fprintf(stderr,"%s\n",client->out.bytes); */
      if (write_headers)
	write_string(client->socket,"</body>\n</html>\n");}
    else {
      output_content(client,content);
      client->out.point=client->out.bytes;}}
  fd_decref(proc); fd_decref(result);
  u8_client_close(ucl);
  return 1;
}

static int close_webclient(u8_client ucl)
{
  fd_webconn client=(fd_webconn)ucl;
  fd_dtsclose(&(client->in),2);
  u8_close(&(client->out));
}

static void shutdown_dtypeserver()
{
  u8_server_shutdown(&fdwebserver);
}

static void init_symbols()
{
  main_symbol=fd_intern("MAIN");
  cgisymbol=fd_intern("CGIDATA");
  script_filename=fd_intern("SCRIPT_FILENAME");
  doctype_slotid=fd_intern("DOCTYPE");
  xmlpi_slotid=fd_intern("XMLPI");
  content_type=fd_intern("CONTENT-TYPE");
  content_slotid=fd_intern("CONTENT");
  html_headers=fd_intern("HTML-HEADERS");
  http_headers=fd_intern("HTTP-HEADERS");
  tracep_slotid=fd_intern("TRACEP");
}

/* The main() event */

static void doexit(int sig)
{
  exit(0);
}

int main(int argc,char **argv)
{
  int fd_version=fd_init_fdscheme();
  unsigned char data[1024], *input;
  double showtime=-1.0;
  int i=2; u8_string source_file=NULL;
  if (argc<2) {
    fprintf(stderr,"Usage: fdwebserv <socket> [config]*\n");
    exit(2);}
  fd_init_fdscheme(); fd_init_schemeio();
  fd_init_texttools(); fd_init_fdweb();
  u8_init_chardata_c();
  signal(SIGHUP,doexit);
  init_symbols();
  fd_init_hashtable(&pagemap,0,NULL,NULL);
  u8_server_init(&fdwebserver,8,8,simply_accept,webservefn,close_webclient);
  fdwebserver.flags=
    fdwebserver.flags|U8_SERVER_LOG_LISTEN|U8_SERVER_LOG_CONNECT;
  atexit(shutdown_dtypeserver);
  while (i<argc)
    if (strchr(argv[i],'=')) 
      fd_config_assignment(argv[i++]);
    else i++;
  if (u8_file_existsp(argv[1])) remove(argv[1]);
  u8_add_server(&fdwebserver,argv[1],-1);
  chmod(argv[1],0777);
  u8_server_loop(&fdwebserver);
  return 0;
}


/* The CVS log for this file
   $Log: fdwebserv.c,v $
   Revision 1.21  2006/01/31 13:47:24  haase
   Changed fd_str[n]dup into u8_str[n]dup

   Revision 1.20  2006/01/26 14:44:33  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.19  2006/01/20 04:11:15  haase
   Fixed leak in fdwebserv

   Revision 1.18  2005/12/20 18:03:58  haase
   Made test executables include all libraries statically

   Revision 1.17  2005/10/10 17:01:19  haase
   Fixes for new mktime/offtime functions

   Revision 1.16  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.15  2005/06/01 13:07:55  haase
   Fixes for less forgiving compilers

   Revision 1.14  2005/05/10 18:43:35  haase
   Added context argument to fd_type_error

   Revision 1.13  2005/05/09 20:06:24  haase
   Added SIGHUP handler for fdwebserv

   Revision 1.12  2005/05/04 09:11:44  haase
   Moved FD_INIT_SCHEME_BUILTINS into individual executables in order to dance around an OS X dependency problem

   Revision 1.11  2005/05/03 02:13:57  haase
   Added TRACEP slot for cgidata which can cause trace statements to be emitted

   Revision 1.10  2005/04/29 04:05:58  haase
   Made preface production be surpressed in the event of a non-string doctype

   Revision 1.9  2005/04/24 22:06:55  haase
   Added BODY! primitive to set attribute body and changed xthtml_header fn to xhtml_preface

   Revision 1.8  2005/04/24 02:17:41  haase
   Fixed writeall

   Revision 1.7  2005/04/21 19:07:58  haase
   Fixed bug in writeall which rewrite prefixes

   Revision 1.6  2005/04/16 17:00:39  haase
   Check argc

   Revision 1.5  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.4  2005/04/13 14:59:26  haase
   Handle load exceptions

   Revision 1.3  2005/04/11 04:32:27  haase
   Fixes to handling of fdxml documents

   Revision 1.2  2005/04/10 17:24:03  haase
   Initial attempt at allowing fdxml handling in fdwebserv

   Revision 1.1  2005/04/10 02:05:25  haase
   Added fdwebserv to tests

   Revision 1.6  2005/04/06 19:25:05  haase
   Added MODULES config to fdtypeserver

   Revision 1.5  2005/04/06 15:39:17  haase
   Added server level logging

   Revision 1.4  2005/04/04 22:22:27  haase
   Better error reporting from executables

   Revision 1.3  2005/03/26 00:16:13  haase
   Made loading facility be generic and moved the rest of file access into fileio.c

   Revision 1.2  2005/03/24 17:16:00  haase
   Fixes to fdtypeserver

   Revision 1.1  2005/03/24 00:30:18  haase
   Added fdtypeserver

   Revision 1.22  2005/03/06 02:03:06  haase
   Fixed include statements for system header files

   Revision 1.21  2005/03/05 21:07:39  haase
   Numerous i18n updates

   Revision 1.20  2005/03/05 19:38:39  haase
   Added setlocale call

   Revision 1.19  2005/02/28 03:20:01  haase
   Added optional load file and config processing to fdconsole

   Revision 1.18  2005/02/24 19:12:46  haase
   Fixes to handling index arguments which are strings specifiying index sources

   Revision 1.17  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
