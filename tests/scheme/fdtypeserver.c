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
#include <sys/time.h>
#include <time.h>
#include <libu8/u8.h>
#include <libu8/timefns.h>
#include <libu8/filefns.h>
#include <libu8/netfns.h>
#include "fdb/dtype.h"
#include "fdb/tables.h"
#include "fdb/eval.h"
#include "fdb/fddb.h"
#include "fdb/pools.h"

static fd_lispenv server_env;
struct U8_SERVER dtype_server;

static int config_serve_port(fdtype var,fdtype val,void *data)
{
  if (FD_FIXNUMP(val)) 
    return u8_add_server(&dtype_server,NULL,FD_FIX2INT(val));
  else if (FD_STRINGP(val)) 
    return u8_add_server(&dtype_server,FD_STRDATA(val),0);
  else return -1;
}

static fdtype config_get_ports(fdtype var,void *data)
{
  fdtype results=FD_EMPTY_CHOICE;
  int i=0, lim=dtype_server.n_servers;
  while (i<lim) {
    fdtype id=fdtype_string(dtype_server.server_info[i].idstring);
    FD_ADD_TO_CHOICE(results,id); i++;}
  return results;
}

typedef struct FD_CLIENT {
  U8_CLIENT_FIELDS;
  struct FD_DTYPE_STREAM stream;
  fd_lispenv env;} FD_CLIENT;
typedef struct FD_CLIENT *fd_client;

static u8_client simply_accept(int sock,struct sockaddr *addr,int len)
{
  fd_client consed=u8_malloc(sizeof(FD_CLIENT));
  consed->socket=sock; consed->flags=0;
  fd_init_dtype_stream(&(consed->stream),sock,4096,NULL,NULL);
  consed->env=fd_make_env(fd_make_hashtable(NULL,16,NULL),server_env);
  return (u8_client) consed;
}

static int dtypeserver(u8_client ucl)
{
  fd_client client=(fd_client)ucl;
  fdtype expr=fd_dtsread_dtype(&(client->stream));
  if (expr == FD_EOD) {
    u8_client_close(ucl);
    return 0;}
  else {
    fdtype value=fd_eval(expr,client->env);
    fd_dtswrite_dtype(&(client->stream),value);
    fd_dtsflush(&(client->stream));
    fd_decref(expr); fd_decref(value);
    return 1;}
}

static int close_fdclient(u8_client ucl)
{
  fd_client client=(fd_client)ucl;
  fd_dtsclose(&(client->stream),2);
}


static void shutdown_dtypeserver()
{
  u8_server_shutdown(&dtype_server);
}

/* Module configuration */

static fdtype module_list=FD_EMPTY_LIST;
static fd_lispenv exposed_environment=NULL;

static fdtype config_get_modules(fdtype var,void *data)
{
  return fd_incref(module_list);
}
static fdtype config_use_module(fdtype var,fdtype val,void *data)
{
  fdtype safe_module=fd_find_module(val,1), module=safe_module;
  if (FD_VOIDP(module)) {}
  else if (FD_HASHTABLEP(module)) 
    exposed_environment=
      fd_make_env(fd_incref(module),exposed_environment);
  else if (FD_PRIM_TYPEP(module,fd_environment_type)) {
    FD_ENVIRONMENT *env=
      FD_GET_CONS(module,fd_environment_type,FD_ENVIRONMENT *);
    if (FD_HASHTABLEP(env->exports))
      exposed_environment=
	fd_make_env(fd_incref(env->exports),exposed_environment);}
  module=fd_find_module(val,0);
  if ((FD_EQ(module,safe_module)))
    if (FD_VOIDP(module)) return 0;
    else return 1;
  else if (FD_HASHTABLEP(module)) 
    exposed_environment=
      fd_make_env(fd_incref(module),exposed_environment);
  else if (FD_PRIM_TYPEP(module,fd_environment_type)) {
    FD_ENVIRONMENT *env=
      FD_GET_CONS(module,fd_environment_type,FD_ENVIRONMENT *);
    if (FD_HASHTABLEP(env->exports))
      exposed_environment=
	fd_make_env(fd_incref(env->exports),exposed_environment);}
  module_list=fd_init_pair(NULL,fd_incref(val),module_list);
  return 1;
}

/* The main() event */

int main(int argc,char **argv)
{
  int fd_version=fd_init_fdscheme();
  unsigned char data[1024], *input;
  double showtime=-1.0;
  int i=1; u8_string source_file=NULL;
  fd_lispenv env=fd_working_environment();
  fd_init_fdscheme(); fd_init_schemeio();
  fd_init_texttools(); fd_init_fdweb();
  u8_init_chardata_c();
  u8_server_init(&dtype_server,8,8,simply_accept,dtypeserver,close_fdclient);
  dtype_server.flags=
    dtype_server.flags|U8_SERVER_LOG_LISTEN|U8_SERVER_LOG_CONNECT;
  fd_register_config("PORT",config_get_ports,config_serve_port,NULL);
  fd_register_config("MODULE",config_get_modules,config_use_module,NULL);
  atexit(shutdown_dtypeserver);
  while (i<argc)
    if (strchr(argv[i],'=')) 
      fd_config_assignment(argv[i++]);
    else if (source_file) i++;
    else {
      source_file=u8_fromlibc(argv[i++]);
      u8_default_appid(source_file);}
  if (source_file) {
    fdtype interp=fd_init_string(NULL,-1,u8_fromlibc(argv[0]));
    fdtype src=fd_init_string(NULL,-1,u8_realpath(source_file,NULL));
    fd_load_source(source_file,env,NULL);
    fd_config_set("INTERPRETER",interp);
    fd_config_set("SOURCE",src);
    fd_decref(src); fd_decref(interp); u8_free(source_file);
    if (FD_HASHTABLEP(env->exports))
      server_env=fd_make_env(env->exports,exposed_environment);
    else server_env=env;
    source_file=NULL;}
  else {
    fprintf(stderr,
	    "Usage: fdtypeserver [conf=val]* source_file [conf=val]*\n");
    return 1;}
  if (dtype_server.n_servers==0) {
    if (u8_file_existsp("temp.socket")) remove("temp.socket");
    u8_add_server(&dtype_server,"temp.socket",0);}
  u8_server_loop(&dtype_server);
  return 0;
}


/* The CVS log for this file
   $Log: fdtypeserver.c,v $
   Revision 1.12  2006/01/26 14:44:33  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.11  2005/12/20 18:03:58  haase
   Made test executables include all libraries statically

   Revision 1.10  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.9  2005/06/01 13:07:55  haase
   Fixes for less forgiving compilers

   Revision 1.8  2005/05/04 09:11:44  haase
   Moved FD_INIT_SCHEME_BUILTINS into individual executables in order to dance around an OS X dependency problem

   Revision 1.7  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

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
