/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "fdb/dtype.h"
#include "fdb/numbers.h"
#include "fdb/tables.h"
#include "fdb/fddb.h"
#include "fdb/eval.h"
#include "fdb/ports.h"


#include <libu8/libu8.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8timefns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8fileio.h>
#include <libu8/u8netfns.h>
#include <libu8/u8srvfns.h>
#include <libu8/u8rusage.h>

#include <strings.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <signal.h>
#include <stdio.h>

#include "revision.h"

FD_EXPORT void fd_init_texttools(void);

/* Various exceptions */
static fd_exception BadPortSpec=_("Bad port spec");
static fd_exception BadRequest=_("Bad client request");
static u8_condition NoServers=_("NoServers");
static u8_condition ServerStartup=_("ServerStart");
static u8_condition ServerShutdown=_("ServerShutdown");

/* This is the global lisp environment for all servers.
   It is modified to include various modules, including dbserv. */
static fd_lispenv server_env;

/* This is the server struct used to establish the server. */
static struct U8_SERVER dtype_server;
static int server_flags=U8_SERVER_LOG_LISTEN|U8_SERVER_LOG_CONNECT;

/* When the server started, used by UPTIME */
static struct U8_XTIME boot_time;

/*  Total number of queued requests, served threads, etc. */
static int max_tasks=32, n_threads=8, server_initialized=0;
/* Controlling trace activity: logeval prints expressions, logtrans reports
   transactions (request/response pairs). */
static int logeval=0, logtrans=0, tracktime=1;

#if FD_THREADS_ENABLED
static u8_mutex init_server_lock;
#endif

static void init_server(void);

/* Configuration
   This uses the CONFIG facility to setup the server.  Some
    config options just set static variables which control the server,
    while others actually call functions to initialize the server.
*/

/* Configuring which ports you're serving.  A port is typically
    a fixnum or a touchtone encoded string and the server will listen
    on all the addresses known for the current hostname.  To listen
    on an additional address (or to limit the port to a particular address,
    specify "port@host".  This can be especially useful to make sure
    the server is listening on localhost (e.g. port@localhost) which
    is often not aliased to the hostname. */
   
static int n_ports=0;

static int config_serve_port(fdtype var,fdtype val,void MAYBE_UNUSED *data)
{
  if (server_initialized==0) init_server();
  if (n_ports<0) return -1;
  else if (FD_FIXNUMP(val)) {
    int retval=u8_add_server(&dtype_server,NULL,FD_FIX2INT(val));
    if (retval<0) n_ports=-1;
    else n_ports=n_ports+retval;
    return retval;}
  else if (FD_STRINGP(val)) {
    int retval=u8_add_server(&dtype_server,FD_STRDATA(val),0);
    if (retval<0) n_ports=-1;
    else n_ports=n_ports+retval;
    return retval;}
  else {
    fd_seterr(BadPortSpec,"config_serve_port",NULL,val);
    return -1;}
}

static fdtype config_get_ports(fdtype var,void MAYBE_UNUSED *data)
{
  fdtype results=FD_EMPTY_CHOICE;
  int i=0, lim=dtype_server.n_servers;
  while (i<lim) {
    fdtype id=fdtype_string(dtype_server.server_info[i].idstring);
    FD_ADD_TO_CHOICE(results,id); i++;}
  return results;
}

/* Configuring dtype_server fields */

static fdtype config_get_dtype_server_flag(fdtype var,void *data)
{
  unsigned int mask=(unsigned int)data, flags; fdtype result;
  fd_lock_mutex(&init_server_lock);
  if (server_initialized) flags=dtype_server.flags;
  else flags=server_flags;
  fd_unlock_mutex(&init_server_lock);
  if ((flags)&(mask)) return FD_TRUE; else return FD_FALSE;
}

static int config_set_dtype_server_flag(fdtype var,fdtype val,void *data)
{
  unsigned int mask=(unsigned int)data, *flagsp, flags;
  fd_lock_mutex(&init_server_lock);
  if (server_initialized) {
    flags=dtype_server.flags; flagsp=&(dtype_server.flags);}
  else {
    flags=server_flags; flagsp=&(server_flags);}
  if (FD_FALSEP(val))
    *flagsp=flags&(~(mask));
  else *flagsp=flags|mask;
  fd_unlock_mutex(&init_server_lock);
  return 1;
}

/* Configuring whether clients have access to a full Scheme interpreter
   or only explicitly exported functions.  Note that even if they have
   a full scheme interpreter, they still only have access to "safe" functions,
   and can't access the filesystem, network, etc. */
   
static int fullscheme=0;

static int config_set_fullscheme(fdtype var,fdtype val,void MAYBE_UNUSED *data)
{
  int oldval=fullscheme;
  if (FD_TRUEP(val))  fullscheme=1; else fullscheme=0;
  return (oldval!=fullscheme);
}

static fdtype config_get_fullscheme(fdtype var,void MAYBE_UNUSED *data)
{
  if (fullscheme) return FD_TRUE; else return FD_FALSE;
}

/* Cleaning up state files */

/* The server currently writes two state files: a pid file indicating
    the server's process id, and an nid file indicating the ports on which
    the server is listening. */

static u8_string pid_file=NULL, nid_file=NULL;

static void cleanup_state_files()
{
  if (pid_file) u8_removefile(pid_file);
  if (nid_file) u8_removefile(nid_file);
}

/* Core functions */

/* This represents a live client connection and its environment. */
typedef struct FD_CLIENT {
  U8_CLIENT_FIELDS;
  struct FD_DTYPE_STREAM stream; int n_errs;
  time_t lastlive; double elapsed;
  fd_lispenv env;} FD_CLIENT;
typedef struct FD_CLIENT *fd_client;

/* This creates the client structure when called by the server loop. */
static u8_client simply_accept(int sock,struct sockaddr *addr,int len)
{
  fd_client consed=u8_alloc(FD_CLIENT);
  consed->socket=sock; consed->flags=0; consed->n_trans=0;
  fd_init_dtype_stream(&(consed->stream),sock,4096);
  consed->env=fd_make_env(fd_make_hashtable(NULL,16),server_env);
  consed->n_errs=0; consed->lastlive=(time_t)-1;
  u8_set_nodelay(sock,1);
  return (u8_client) consed;
}

/* This serves a request on a client. If the libu8 server code
   is functioning properly, it will only be called on connections
   with data on them, so there shouldn't be too much waiting. */
static int dtypeserver(u8_client ucl)
{
  fd_client client=(fd_client)ucl;
  fdtype expr;
  /* To help debugging, move the client->idstring (libu8)
     into the stream's id (fdb). */
  client->n_trans++;
  if (client->stream.id==NULL) {
    if (client->idstring)
      client->stream.id=u8_strdup(client->idstring);
    else client->stream.id=u8_strdup("anonymous");}
  expr=fd_dtsread_dtype(&(client->stream));
  if (expr == FD_EOD) {
    u8_client_close(ucl);
    return 0;}
  else if (FD_ABORTP(expr)) {
    u8_warn(BadRequest,"%s[%d]: Received bad request",client->idstring,client->n_trans);
    fd_clear_errors(1);
    u8_client_close(ucl);
    return 0;}
  else {
    fdtype value;
    int tracethis=((logtrans) && ((client->n_trans==1) || (((client->n_trans)%logtrans)==0)));
    double xstart, elapsed=-1.0;
    if (logeval)
      u8_message("%s[%d]: > %q",client->idstring,client->n_trans,expr);
    else if (logtrans) u8_message("%s[%d]: Received request",client->idstring,client->n_trans);
    xstart=((tracktime) ? (u8_elapsed_time()) : (-1.0));
    value=fd_eval(expr,client->env);
    if (xstart>=0) elapsed=u8_elapsed_time()-xstart;
    if (logeval)
      if (xstart>=0)
	u8_message("%s[%d]: < %q in %f",client->idstring,client->n_trans,value,
		   u8_elapsed_time()-xstart);
      else u8_message("%s[%d]: < %q",client->idstring,client->n_trans,value);
    else if (tracethis)
      if ((FD_ABORTP(value)) || (FD_EXCEPTIONP(value))) {
	if (elapsed>=0)
	  u8_message("%s[%d]: Error returned in %fs",client->idstring,client->n_trans,elapsed);
	else u8_message("%s[%d]: Error returned",client->idstring,client->n_trans);
      	client->n_errs++;}
      else if (elapsed>=0)
	u8_message("%s[%d]: Normal return in %fs",client->idstring,client->n_trans,elapsed);
      else u8_message("%s[%d]: Normal return",client->idstring,client->n_trans);
    if (tracktime) client->elapsed=client->elapsed+elapsed;
    fd_dtswrite_dtype(&(client->stream),value);
    fd_dtsflush(&(client->stream));
    if (tracktime) time(&(client->lastlive));
    if ((tracethis) && (xstart>=0))
      u8_message("%s[%d]: Response sent after %fs",
		 client->idstring,client->n_trans,u8_elapsed_time()-xstart);
    else if (logtrans) u8_message("%s[%d]: Response sent",client->idstring,client->n_trans);
    fd_decref(expr); fd_decref(value);
    fd_swapcheck();
    return 1;}
}

static int close_fdclient(u8_client ucl)
{
  fd_client client=(fd_client)ucl;
  fd_dtsclose(&(client->stream),2);
  fd_decref((fdtype)((fd_client)ucl)->env);
}

/* Module configuration */

/* A list of exposed modules */
static fdtype module_list=FD_EMPTY_LIST;
/* This is the exposed environment. */
static fd_lispenv exposed_environment=NULL;
/* This is the shutdown procedure to be called when the
   server shutdowns. */
static fdtype shutdown_proc=FD_EMPTY_CHOICE;
static int normal_exit=0;

static fdtype config_get_modules(fdtype var,void *data)
{
  return fd_incref(module_list);
}
static int config_use_module(fdtype var,fdtype val,void *data)
{
  fdtype safe_module=fd_find_module(val,1,1), module=safe_module;
  if (FD_VOIDP(module)) {}
  else if (FD_HASHTABLEP(module)) 
    exposed_environment=
      fd_make_env(fd_incref(module),exposed_environment);
  else if (FD_PTR_TYPEP(module,fd_environment_type)) {
    FD_ENVIRONMENT *env=
      FD_GET_CONS(module,fd_environment_type,FD_ENVIRONMENT *);
    if (FD_HASHTABLEP(env->exports))
      exposed_environment=
	fd_make_env(fd_incref(env->exports),exposed_environment);}
  module=fd_find_module(val,0,1);
  if ((FD_EQ(module,safe_module)))
    if (FD_VOIDP(module)) return 0;
    else return 1;
  else if (FD_HASHTABLEP(module)) 
    exposed_environment=
      fd_make_env(fd_incref(module),exposed_environment);
  else if (FD_PTR_TYPEP(module,fd_environment_type)) {
    FD_ENVIRONMENT *env=
      FD_GET_CONS(module,fd_environment_type,FD_ENVIRONMENT *);
    if (FD_HASHTABLEP(env->exports))
      exposed_environment=
	fd_make_env(fd_incref(env->exports),exposed_environment);}
  module_list=fd_init_pair(NULL,fd_incref(val),module_list);
  return 1;
}

/* Handling signals, etc. */

static void shutdown_dtypeserver_onsignal(int sig)
{
  u8_warn(ServerShutdown,"Shutting down server on signal %d",sig);
  u8_server_shutdown(&dtype_server);
  if (FD_APPLICABLEP(shutdown_proc)) {
    fdtype sigval=FD_INT2DTYPE(sig), value;
    u8_warn(ServerShutdown,"Calling shutdown procedure %q",shutdown_proc);
    value=fd_apply(shutdown_proc,1,&sigval);
    fd_decref(value);}
  cleanup_state_files();
  u8_warn(ServerShutdown,"Done shutting down server");
}

static void shutdown_dtypeserver_onexit()
{
  u8_warn(ServerShutdown,"Shutting down server on exit");
  u8_server_shutdown(&dtype_server);
  if (FD_APPLICABLEP(shutdown_proc)) {
    fdtype shutval, value;
    if (normal_exit) shutval=FD_FALSE; else shutval=FD_TRUE;
    u8_warn(ServerShutdown,"Calling shutdown procedure %q",shutdown_proc);
    value=fd_apply(shutdown_proc,1,&shutval);
    fd_decref(value);}
  cleanup_state_files();
  u8_warn(ServerShutdown,"Done shutting down server");
}

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

static fdtype boundp_handler(fdtype expr,fd_lispenv env)
{
  fdtype symbol=fd_get_arg(expr,1);
  if (!(FD_SYMBOLP(symbol)))
    return fd_err(fd_SyntaxError,"boundp_handler",NULL,fd_incref(expr));
  else {
    fdtype val=fd_symeval(symbol,env);
    if (FD_VOIDP(val)) return FD_FALSE;
    else if (val == FD_UNBOUND) return FD_FALSE;
    else {
      fd_decref(val); return FD_TRUE;}}
}

/* The main() event */

static void init_server()
{
  fd_lock_mutex(&init_server_lock);
  if (server_initialized) return;
  server_initialized=1;
  u8_server_init(&dtype_server,max_tasks,n_threads,simply_accept,dtypeserver,close_fdclient);
  dtype_server.flags=dtype_server.flags|server_flags;
  fd_unlock_mutex(&init_server_lock);
}

FD_EXPORT void fd_init_fddbserv(void);

int main(int argc,char **argv)
{
  int fd_version=fd_init_fdscheme();
  unsigned char data[1024], *input;
  double showtime=-1.0;
  int i=1; u8_string source_file=NULL;
  /* This is the base of the environment used to be passed to the server.
     It is augmented by the fdbserv module, all of the modules declared by
     MODULE= configurations, and either the exports or the definitions of
     the server control file from the command line.
     It starts out built on the default safe environment, but loses that if
     fullscheme is zero after configuration and file loading.  fullscheme can be
     set by the FULLSCHEME configuration parameter. */
  fd_lispenv core_env; 

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

  /* Show more information */
  u8_show_procinfo=1;
  u8_use_syslog(1);

#if ((!(HAVE_CONSTRUCTOR_ATTRIBUTES)) || (FD_TESTCONFIG))
  fd_init_fdscheme();
  fd_init_schemeio();
  fd_init_texttools();
#else
  FD_INIT_SCHEME_BUILTINS();
#endif

  fd_init_fddbserv();
  fd_register_module("FDBSERV",fd_incref(fd_fdbserv_module),FD_MODULE_SAFE);
  fd_register_config("MAXTASKS",fd_intconfig_get,fd_intconfig_set,&max_tasks);
  fd_register_config("NTHREADS",fd_intconfig_get,fd_intconfig_set,&n_threads);
  fd_register_config("PORT",config_get_ports,config_serve_port,NULL);
  fd_register_config("MODULE",config_get_modules,config_use_module,NULL);
  fd_register_config("FULLSCHEME",config_get_fullscheme,config_set_fullscheme,NULL);
  fd_register_config("LOGEVAL",fd_boolconfig_get,fd_boolconfig_set,&logeval);
  fd_register_config("LOGTRANS",fd_intconfig_get,fd_intconfig_set,&logtrans);
  fd_register_config("TRACKTIME",fd_boolconfig_get,fd_boolconfig_set,&tracktime);
  fd_register_config("U8LOGCONN",config_get_dtype_server_flag,config_set_dtype_server_flag,
		     (void *)(U8_SERVER_LOG_CONNECT));
  fd_register_config("U8LOGTRANS",config_get_dtype_server_flag,config_set_dtype_server_flag,
		     (void *)(U8_SERVER_LOG_TRANSACT));

  /* Prepare for the end */
  atexit(shutdown_dtypeserver_onexit);
#ifdef SIGTERM
  signal(SIGTERM,shutdown_dtypeserver_onsignal);
#endif
#ifdef SIGQUIT
  signal(SIGQUIT,shutdown_dtypeserver_onsignal);
#endif

  /* This is a safe environment (e.g. a sandbox without file/io etc). */
  core_env=fd_safe_working_environment();

  /* We add some special functions */
  fd_defspecial((fdtype)core_env,"BOUND?",boundp_handler);
  fd_idefn((fdtype)core_env,fd_make_cprim0("BOOT-TIME",get_boot_time,0));
  fd_idefn((fdtype)core_env,fd_make_cprim0("UPTIME",get_uptime,0));

  /* And create the exposed environment */
  exposed_environment=fd_make_env(fd_incref(fd_fdbserv_module),core_env);
  
  /* Now process all the configuration arguments and find the source file */
  while (i<argc)
    if (strchr(argv[i],'=')) 
      fd_config_assignment(argv[i++]);
    else if (source_file) i++;
    else {
      source_file=u8_fromlibc(argv[i++]);
      u8_default_appid(source_file);}

  if (source_file) {
    /* The source file is loaded into a full (non sandbox environment).
       It's exports are then exposed through the server. */
    fd_lispenv env=fd_working_environment();
    fdtype result=fd_load_source(source_file,env,NULL);
    if (FD_ERRORP(result)) {
      struct FD_EXCEPTION_OBJECT *e=(struct FD_EXCEPTION_OBJECT *)result;
      U8_OUTPUT out; U8_INIT_OUTPUT(&out,512);
      fd_print_error(&out,e);
      fd_print_backtrace(&out,80,e->backtrace);
      fd_print_error(&out,e);
      fputs(out.u8_outbuf,stderr);
      u8_free(out.u8_outbuf);
      u8_free(source_file);
      fd_decref(result);
      fd_decref((fdtype)env);
      return -1;}
    else if (FD_TROUBLEP(result)) {
      fd_exception ex; u8_context cxt; u8_string details; fdtype irritant;
      /* Abort if there are any errors loading the file. */
      if (fd_poperr(&ex,&cxt,&details,&irritant)) {
	u8_warn(ex,";; (ERROR %m) %m (%s)\n",ex,
		((details)?(details):((u8_string)"")),
		((cxt)?(cxt):((u8_string)"")));
	if (!(FD_VOIDP(irritant)))
	  u8_warn("INIT Error",";; %q\n",irritant);
	if (details) u8_free(details); fd_decref(irritant);}
      else u8_warn("INIT Error",";; Unexplained error result %q\n",result);
      u8_free(source_file);
      fd_decref((fdtype)env);
      return -1;}
    else {fd_decref(result); result=FD_VOID;}

    /* Store server initialization information in the configuration
       environment. */
    {
      fdtype interp=fd_init_string(NULL,-1,u8_fromlibc(argv[0]));
      fdtype src=fd_init_string(NULL,-1,u8_realpath(source_file,NULL));
      fd_config_set("INTERPRETER",interp);
      fd_config_set("SOURCE",src);
      fd_decref(interp); fd_decref(src);}

    /* If the init file did any exporting, expose those exports to clients.
       Otherwise, expose all the definitions in the init file.  Note that the clients
       won't be able to get at the unsafe "empowered" environment but that the
       procedures defined are closed in that environment. */
    if (FD_HASHTABLEP(env->exports))
      server_env=fd_make_env(fd_incref(env->exports),exposed_environment);
    else server_env=fd_make_env(fd_incref(env->bindings),exposed_environment);

    /* Handle server startup and shutdown procedures defined in the environment. */
    {
      fdtype startup_proc=fd_symeval(fd_intern("STARTUP"),env);
      shutdown_proc=fd_symeval(fd_intern("SHUTDOWN"),env);
      if (FD_VOIDP(startup_proc)) {}
      else {
	FD_DO_CHOICES(p,startup_proc) {
	  fdtype result=fd_apply(p,0,NULL);
	  if (FD_ABORTP(result))
	    exit(fd_interr(result));
	  else fd_decref(result);}}}

    /* You're done loading the file now. */
    fd_decref((fdtype)env);

    /* Now, write the state files.  */
    {
      u8_string bname=u8_basename(source_file,".fdz");
      u8_string fullname=u8_abspath(bname,NULL);
      FILE *f;
      /* Get state files and write info */
      pid_file=u8_string_append(fullname,".pid",NULL);
      nid_file=u8_string_append(fullname,".nid",NULL);
      atexit(cleanup_state_files);
      /* Write the PID file */
      f=u8_fopen(pid_file,"w");
      if (f) {
	fprintf(f,"%d\n",getpid());
	fclose(f);}
      else {
	u8_warn(u8_strerror(errno),
		"Couldn't write PID %d to '%s'",getpid(),pid_file);
	errno=0;}
      /* Write the NID file */
      f=u8_fopen(nid_file,"w");
      if (f) {
	if (dtype_server.n_servers) {
	  int i=0; while (i<dtype_server.n_servers) {
	    fprintf(f,"%s\n",dtype_server.server_info[i].idstring);
	    i++;}}
	else fprintf(f,"temp.socket\n");
	fclose(f);}
      else {
	u8_warn(u8_strerror(errno),
		"Couldn't write NID info to '%s'",getpid(),pid_file);
	if (dtype_server.n_servers) {
	  int i=0; while (i<dtype_server.n_servers) {
	    u8_notify(ServerStartup,"%s\n",dtype_server.server_info[i].idstring);
	    i++;}}
	else u8_notify(ServerStartup,"temp.socket\n");
	errno=0;}
      u8_free(fullname); u8_free(bname);}
    u8_free(source_file);
    source_file=NULL;}
  else {
    fprintf(stderr,
	    "Usage: fdtypeserver [conf=val]* source_file [conf=val]*\n");
    return 1;}
  if (fullscheme==0) {
    fd_decref((fdtype)(core_env->parent)); core_env->parent=NULL;}
  if (n_ports>0) {
    u8_message("FramerD (r%s) fdbserver running, %d/%d pools/indices",
	       SVN_REVISION,fd_n_pools,
	       fd_n_primary_indices+fd_n_secondary_indices);
    u8_message
      ("beingmeta FramerD, (C) beingmeta 2004-2007, all rights reserved");
    u8_notify(ServerStartup,"Serving on %d sockets",n_ports);
    u8_server_loop(&dtype_server); normal_exit=1;
    u8_notify(ServerShutdown,"Exited server loop",n_ports);
    return 0;}
  else if (n_ports==0) {
    u8_warn(NoServers,"No servers configured, exiting..");
    return -1;}
  else {
    fd_clear_errors(1);
    shutdown_dtypeserver_onexit();
    fd_clear_errors(1);
    return -1;}
}

