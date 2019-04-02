/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/numbers.h"
#include "framerd/apply.h"

#include <libu8/u8signals.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8printf.h>
#include <libu8/u8logging.h>

#include <signal.h>
#include <sys/types.h>
#include <pwd.h>

#include <libu8/libu8.h>
#include <libu8/u8netfns.h>
#if FD_FILECONFIG_ENABLED
#include <libu8/u8filefns.h>
#include <libu8/libu8io.h>
#endif

#include <sys/time.h>

#if HAVE_SYS_RESOURCE_H
#include <sys/resource.h>
#endif

#if HAVE_SYS_TIME_H
#include <sys/time.h>
#endif


#if HAVE_UNISTD_H
#include <unistd.h>
#endif

#if HAVE_GRP_H
#include <grp.h>
#endif

u8_condition fd_ArgvConfig=_("Config (argv)");
u8_condition fd_CmdArg=_("Command arg");
u8_condition fd_CmdLine=_("Command");
u8_condition SetRLimit=_("SetRLimit");
u8_condition fd_ExitException=_("Unhandled exception at exit");

static u8_mutex atexit_handlers_lock;

#define PID_OPEN_FLAGS O_WRONLY|O_CREAT|O_EXCL
u8_string pid_filename = NULL;

int fd_in_doexit = 0;
int fd_logcmd    = 0;
int fd_exiting   = 0;
int fd_exited    = 0;

/* This determines whether to memory should be freed while exiting */
int fd_fast_exit = 0;

static u8_string logdir = NULL, sharedir = NULL, datadir = NULL;

int fd_be_vewy_quiet = 0;
static int boot_message_delivered = 0;

/* Processing argc,argv */

lispval *fd_argv = NULL;
int fd_argc = -1;

static void set_vector_length(lispval vector,int len);
static lispval exec_arg = FD_FALSE, lisp_argv = FD_FALSE, string_argv = FD_FALSE;
static lispval raw_argv = FD_FALSE, config_argv = FD_FALSE;
static u8_string exe_name = NULL;
static int init_argc = 0;
static size_t app_argc;

static void log_argv(int n,lispval *argv)
{
  U8_STATIC_OUTPUT(args,1000);
  int arg_i = 0; ssize_t start = 0;
  while ( arg_i < n) {
    if (((args.u8_write-args.u8_outbuf)-start) > 80) {
      u8_puts(argsout,"\n  ");
      start = args.u8_write-args.u8_outbuf;}
    u8_printf(argsout,"%q ",argv[arg_i]);
    arg_i++;}
  u8_log(U8_LOG_MSG,NULL,"Command = %s %s",u8_appid(),args.u8_outbuf);
}

/* This takes an argv, argc combination and processes the argv elements
   which are configs (var = value again) */
FD_EXPORT lispval *fd_handle_argv(int argc,char **argv,
                                  unsigned char *arg_mask,
                                  size_t *arglen_ptr)
{
  if (argc>0) {
    exe_name = u8_fromlibc(argv[0]);
    lispval interp = fdstring(exe_name);
    u8_string exec_path = NULL;
    fd_set_config("INTERPRETER",interp);
    fd_set_config("EXE",interp);
    if (exe_name[0]=='/')
      exec_path = exe_name;
    else if ( (u8_file_existsp("/proc/self/exe")) &&
              (exec_path = u8_filestring("/proc/self/exe",NULL)) ) {}
    else if (strchr(exe_name,'/'))
      exec_path = u8_abspath(exe_name,NULL);
    else if (u8_file_existsp(exe_name))
      exec_path = u8_abspath(exe_name,NULL);
    else {}
    if (exec_path == exe_name)
      fd_set_config("EXECPATH",interp);
    else if (exec_path) {
      lispval exec_val = fd_init_string(NULL,-1,exec_path);
      fd_set_config("EXECPATH",exec_val);
      fd_decref(exec_val);}
    else fd_set_config("EXECPATH",interp);
    fd_decref(interp);}

  if (fd_argv!=NULL)  {
    if ((init_argc>0) && (argc != init_argc))
      u8_log(LOG_WARN,"InconsistentArgv/c",
             "Trying to reprocess argv with a different argc (%d) length != %d",
             argc,init_argc);
    if (arglen_ptr) *arglen_ptr = fd_argc;
    return fd_argv;}
  else if (argc<=0) {
    u8_log(LOG_CRIT,"fd_handle_arg(invalid argv)",
           _("The argc length %d is not valid (>0)"),argc);
    return NULL;}
  else if (argv == NULL) {
    u8_log(LOG_CRIT,"fd_handle_arg(invalid argv)",
           _("The argv argument cannot be NULL!"),argc);
    return NULL;}
  else {
    int i = 0, n = 0, config_i = 0;
    lispval string_args = fd_make_vector(argc-1,NULL), string_arg = VOID;
    lispval lisp_args = fd_make_vector(argc-1,NULL), lisp_arg = VOID;
    lispval config_args = fd_make_vector(argc-1,NULL);
    lispval raw_args = fd_make_vector(argc,NULL);
    lispval *return_args = (arglen_ptr) ? (u8_alloc_n(argc-1,lispval)) : (NULL);
    lispval *_fd_argv = u8_alloc_n(argc-1,lispval);

    u8_threadcheck();

    init_argc = argc;

    while (i<argc) {
      char *carg = argv[i];
      u8_string arg = u8_fromlibc(carg), eq = strchr(arg,'=');
      FD_VECTOR_SET(raw_args,i,lispval_string(arg));
      /* Don't include argv[0] in the arglists */
      if ( (i==0) || (arg_mask[i]) ) {
        /* Skip first and masked args */
        u8_free(arg);
        i++;
        continue;}
      else i++;
      if ((eq!=NULL) && (eq>arg) && (*(eq-1)!='\\')) {
        int retval = (arg!=NULL) ? (fd_config_assignment(arg)) : (-1);
        FD_VECTOR_SET(config_args,config_i,lispval_string(arg));
        config_i++;
        if (retval<0) {
          u8_log(LOG_CRIT,"FailedConfig",
                 "Couldn't handle the config argument `%s`",
                 (arg == NULL) ? ((u8_string)carg) : (arg));
          u8_clear_errors(0);}
        else u8_log(LOG_INFO,fd_ArgvConfig,"   %s",arg);
        u8_free(arg);
        continue;}
      string_arg = lispval_string(arg);
      /* Note that fd_parse_arg should always return at least a lisp
         string */
      lisp_arg = fd_parse_arg(arg);
      if (return_args) {
        return_args[n]=lisp_arg;
        fd_incref(lisp_arg);}
      _fd_argv[n]=lisp_arg; fd_incref(lisp_arg);
      FD_VECTOR_SET(lisp_args,n,lisp_arg);
      FD_VECTOR_SET(string_args,n,string_arg);
      u8_log(LOG_INFO,fd_CmdArg,"[%d] %s => %q",n+1,arg,lisp_arg);
      u8_free(arg);
      n++;}
    set_vector_length(lisp_args,n);
    lisp_argv = lisp_args;
    set_vector_length(string_args,n);
    string_argv = string_args;
    set_vector_length(config_args,config_i);
    config_argv = config_args;
    raw_argv = raw_args;
    app_argc = n;
    fd_argv = _fd_argv;
    fd_argc = n;
    if (return_args) {
      if (arglen_ptr) *arglen_ptr = n;
      return return_args;}
    else return NULL;}
}

static void set_vector_length(lispval vector,int len)
{
  if (VECTORP(vector)) {
    struct FD_VECTOR *vec = (struct FD_VECTOR *) vector;
    if (len>=0) {
      vec->vec_length = len;
      return;}}
  u8_log(LOG_CRIT,"Internal/CmdArgInitVec","Not a vector! %q",
         vector);
}

/* Accessing source file registry */

static void add_source_file(u8_string s,void *vp)
{
  lispval *valp = (lispval *)vp;
  lispval val = *valp;
  CHOICE_ADD(val,lispval_string(s));
  *valp = val;
}

static lispval config_get_source_files(lispval var,void *data)
{
  lispval result = EMPTY;
  u8_for_source_files(add_source_file,&result);
  return result;
}

static int config_add_source_file(lispval var,lispval val,void *data)
{
  if (STRINGP(val)) {
    u8_string stringval = u8_strdup(CSTRING(val));
    u8_register_source_file(stringval);
    return 1;}
  else {
    fd_type_error(_("string"),"config_addsourcefile",val);
    return -1;}
}

/* Termination */

static struct FD_ATEXIT {
  lispval exitfn_handler;
  lispval exitfn_name;
  struct FD_ATEXIT *exitfn_next;} *atexit_handlers = NULL;
static int n_atexit_handlers = 0;

static lispval config_atexit_get(lispval var,void *data)
{
  struct FD_ATEXIT *scan; int i = 0; lispval result;
  u8_lock_mutex(&atexit_handlers_lock);
  result = fd_make_vector(n_atexit_handlers,NULL);
  scan = atexit_handlers; while (scan) {
    lispval handler = scan->exitfn_handler, result;
    if (FD_VOIDP(scan->exitfn_name))
      result=fd_incref(handler);
    else result=fd_make_pair(scan->exitfn_name,handler);
    FD_VECTOR_SET(result,i,result);
    scan = scan->exitfn_next;
    i++;}
  u8_unlock_mutex(&atexit_handlers_lock);
  return result;
}

static int config_atexit_set(lispval var,lispval val,void *data)
{
  lispval fn, name=FD_VOID;
  if (FD_PAIRP(val)) {
    name=FD_CAR(val); fn=FD_CDR(val);}
  else fn=val;
  if (!(FD_APPLICABLEP(fn))) {
    fd_type_error("applicable","config_atexit",val);
    return -1;}
  u8_lock_mutex(&atexit_handlers_lock);
  struct FD_ATEXIT *scan=atexit_handlers;
  while (scan) {
    if ( (FD_EQUALP(name,scan->exitfn_name)) ||
         (fn==scan->exitfn_handler) ) {
      lispval oldfn=scan->exitfn_handler;
      int changed = (fn != oldfn);
      if (changed) {
        fd_incref(fn);
        scan->exitfn_handler=fn;
        fd_decref(oldfn);}
      u8_unlock_mutex(&atexit_handlers_lock);
      return changed;}
    else scan=scan->exitfn_next;}
  struct FD_ATEXIT *fresh = u8_malloc(sizeof(struct FD_ATEXIT));
  fresh->exitfn_next = atexit_handlers;
  fresh->exitfn_name = name;
  fresh->exitfn_handler = fn;
  fd_incref(val);
  n_atexit_handlers++; atexit_handlers = fresh;
  u8_unlock_mutex(&atexit_handlers_lock);
  return 1;
}

FD_EXPORT void fd_doexit(lispval arg)
{
  struct FD_ATEXIT *scan, *tmp;
  if (fd_exited) return;
  if (fd_in_doexit) {
    u8_log(LOG_WARN,"RecursiveExit","Recursive fd_doexit");
    return;}
  fd_in_doexit = 1;
  fd_exiting = 1;
  if (atexit_handlers) {
    u8_lock_mutex(&atexit_handlers_lock);
    u8_log(LOG_NOTICE,"fd_doexit","Running %d FramerD exit handlers",
           n_atexit_handlers);
    scan = atexit_handlers; atexit_handlers = NULL;
    u8_unlock_mutex(&atexit_handlers_lock);
    while (scan) {
      lispval handler = scan->exitfn_handler, result = VOID;
      u8_log(LOG_INFO,"fd_doexit","Running FramerD exit handler %q",handler);
      if ((FD_FUNCTIONP(handler))&&(FD_FUNCTION_ARITY(handler)))
        result = fd_apply(handler,1,&arg);
      else result = fd_apply(handler,0,NULL);
      if (FD_ABORTP(result)) {
        fd_clear_errors(1);}
      else fd_decref(result);
      fd_decref(handler);
      scan->exitfn_handler = FD_VOID;
      tmp = scan;
      scan = scan->exitfn_next;
      u8_free(tmp);}}
  else u8_log(LOG_DEBUG,"fd_doexit","No FramerD exit handlers!");
  if (pid_filename) {
    int rv = u8_removefile(pid_filename);
    if (rv<0) {
      if (errno)
        u8_log(LOG_CRIT,"PIDFile",
               "Couldn't remove PID file %s (%s)",
               pid_filename,u8_strerror(errno));
      else u8_log(LOG_CRIT,"PIDFile",
                  "Couldn't remove PID file %s",pid_filename);}
    u8_free(pid_filename);
    pid_filename=NULL;}

  if ( (fd_logcmd) && (fd_argc > 1) )
    log_argv(fd_argc,fd_argv);

  if (fd_argv) {
    int i = 0, n = fd_argc; while (i<n) {
      lispval elt = fd_argv[i++];
      fd_decref(elt);}
    u8_free(fd_argv);
    fd_argv = NULL;
    fd_argc = -1;}
  fd_decref(exec_arg); exec_arg = FD_FALSE;
  fd_decref(lisp_argv); lisp_argv = FD_FALSE;
  fd_decref(string_argv); string_argv = FD_FALSE;
  fd_decref(raw_argv); raw_argv = FD_FALSE;
  fd_decref(config_argv); config_argv = FD_FALSE;
  fd_exited=1;
  fd_exiting=1;
  fd_in_doexit=0;
}

static void doexit_atexit()
{
  if ( (fd_exited) || (fd_in_doexit))
    return;
  else fd_doexit(FD_FALSE);
}

FD_EXPORT void fd_signal_doexit(int sig)
{
  fd_doexit(FD_INT2FIX(sig));
}


/* RLIMIT configs */

#if HAVE_SYS_RESOURCE_H

struct NAMED_RLIMIT {
  u8_string name; int code;};

#ifdef RLIMIT_CPU
static struct NAMED_RLIMIT MAXCPU={"max cpu",RLIMIT_CPU};
#endif
#ifdef RLIMIT_RSS
static struct NAMED_RLIMIT MAXRSS={"max resident memory",RLIMIT_RSS};
#endif
#ifdef RLIMIT_CORE
static struct NAMED_RLIMIT MAXCORE={"max core file size",RLIMIT_CORE};
#endif
#ifdef RLIMIT_NPROC
static struct NAMED_RLIMIT MAXNPROC={"max number of simulaneous processes",RLIMIT_NPROC};
#endif
#ifdef RLIMIT_NOFILE
static struct NAMED_RLIMIT MAXFILES={"max number of open files",RLIMIT_NOFILE};
#endif
#ifdef RLIMIT_STACK
static struct NAMED_RLIMIT MAXSTACK={"max tack size",RLIMIT_STACK};
#endif

FD_EXPORT lispval fd_config_rlimit_get(lispval ignored,void *vptr)
{
  struct rlimit rlim;
  struct NAMED_RLIMIT *nrl = (struct NAMED_RLIMIT *)vptr;
  int retval = getrlimit(nrl->code,&rlim);
  if (retval<0) {
    u8_condition cond = u8_strerror(errno);
    errno = 0;
    return fd_err(cond,"rlimit_get",u8_strdup(nrl->name),VOID);}
  else if (rlim.rlim_cur == RLIM_INFINITY)
    return FD_FALSE;
  else return FD_INT((long long)(rlim.rlim_cur));
}

FD_EXPORT int fd_config_rlimit_set(lispval ignored,lispval v,void *vptr)
{
  struct rlimit rlim;
  struct NAMED_RLIMIT *nrl = (struct NAMED_RLIMIT *)vptr;
  int retval = getrlimit(nrl->code,&rlim); rlim_t setval;
  if ((FIXNUMP(v))||(FD_BIGINTP(v))) {
    long lval = fd_getint(v);
    if (lval<0) {
      fd_incref(v);
      fd_seterr(fd_TypeError,"fd_config_rlimit_set",
               "resource limit (integer)",v);
      return -1;}
    else setval = lval;}
  else if (FALSEP(v)) setval = (RLIM_INFINITY);
  else if ((STRINGP(v))&&
           ((strcasecmp(CSTRING(v),"unlimited")==0)||
            (strcasecmp(CSTRING(v),"nolimit")==0)||
            (strcasecmp(CSTRING(v),"infinity")==0)||
            (strcasecmp(CSTRING(v),"infinite")==0)||
            (strcasecmp(CSTRING(v),"false")==0)||
            (strcasecmp(CSTRING(v),"none")==0)))
    setval = (RLIM_INFINITY);
  else {
    fd_incref(v);
    fd_seterr(fd_TypeError,"fd_config_rlimit_set",
              "resource limit (integer)",v);
    return -1;}
  if (retval<0) {
    u8_condition cond = u8_strerror(errno); errno = 0;
    return fd_err(cond,"rlimit_get",u8_strdup(nrl->name),VOID);}
  else if (setval>rlim.rlim_max) {
    /* Should be more informative */
    fd_seterr(_("RLIMIT too high"),"set_rlimit",nrl->name,VOID);
    return -1;}
  if (setval == rlim.rlim_cur)
    u8_log(LOG_WARN,SetRLimit,
           "Setting for %s did not need to change",
           nrl->name);
  else if (setval == RLIM_INFINITY)
    u8_log(LOG_WARN,SetRLimit,
           "Setting %s to unlimited from %d",
           nrl->name,rlim.rlim_cur);
  else u8_log(LOG_WARN,SetRLimit,
              "Setting %s to %lld from %lld",
              nrl->name,
              (long long)setval,
              rlim.rlim_cur);
  rlim.rlim_cur = setval;
  retval = setrlimit(nrl->code,&rlim);
  if (retval<0) {
    u8_condition cond = u8_strerror(errno); errno = 0;
    return fd_err(cond,"rlimit_set",u8_strdup(nrl->name),VOID);}
  else return 1;
}
#endif

/* Configuration for session and app id info */

static lispval config_getappid(lispval var,void *data)
{
  return lispval_string(u8_appid());
}

static int config_setappid(lispval var,lispval val,void *data)
{
  if (STRINGP(val)) {
    u8_identify_application(CSTRING(val));
    return 1;}
  else return -1;
}

static lispval config_getpid(lispval var,void *data)
{
  pid_t pid = getpid();
  return FD_INT(((unsigned long)pid));
}

static lispval config_getppid(lispval var,void *data)
{
  pid_t pid = getppid();
  return FD_INT(((unsigned long)pid));
}

static lispval config_getsessionid(lispval var,void *data)
{
  return lispval_string(u8_sessionid());
}

static int config_setsessionid(lispval var,lispval val,void *data)
{
  if (STRINGP(val)) {
    u8_identify_session(CSTRING(val));
    return 1;}
  else return -1;
}

static lispval config_getutf8warn(lispval var,void *data)
{
  if (u8_config_utf8warn(-1))
    return FD_TRUE;
  else return FD_FALSE;
}

static int config_setutf8warn(lispval var,lispval val,void *data)
{
  if (FD_TRUEP(val))
    if (u8_config_utf8warn(1)) return 0;
    else return 1;
  else if (u8_config_utf8warn(0)) return 1;
  else return 0;
}

static lispval config_getutf8err(lispval var,void *data)
{
  if (u8_config_utf8err(-1))
    return FD_TRUE;
  else return FD_FALSE;
}

static int config_setutf8err(lispval var,lispval val,void *data)
{
  if (FD_TRUEP(val))
    if (u8_config_utf8err(1)) return 0;
    else return 1;
  else if (u8_config_utf8err(0)) return 1;
  else return 0;
}

/* Random seed initialization */

static u8_condition TimeFailed="call to time() failed";

static long long randomseed = 0x327b23c6;

static lispval config_getrandomseed(lispval var,void *data)
{
  if (randomseed<FD_MAX_FIXNUM)
    return FD_INT(randomseed);
  else return (lispval)fd_long_long_to_bigint(randomseed);
}

static int config_setrandomseed(lispval var,lispval val,void *data)
{
  if (((SYMBOLP(val)) && ((strcasecmp(FD_XSYMBOL_NAME(val),"TIME"))==0)) ||
      ((STRINGP(val)) && ((strcasecmp(CSTRING(val),"TIME"))==0))) {
    time_t tick = time(NULL);
    if (tick<0) {
      u8_graberrno("time",NULL);
      fd_seterr2(TimeFailed,"setrandomseed");
      return -1;}
    else {
      randomseed = (unsigned int)tick;
      u8_randomize(randomseed);
      return 1;}}
  else if (FIXNUMP(val)) {
    long long intval = FIX2INT(val);
    if (intval<0) intval=-intval;
    if (intval>=UINT_MAX) intval=intval%UINT_MAX;
    randomseed = (unsigned int)intval;
    u8_randomize(randomseed);
    return 1;}
  else {
    fd_type_error("random seed (small fixnum)","config_setrandomseed",val);
    return -1;}
}

/* RUNBASE */

static u8_string runbase_config = NULL, runbase = NULL;

FD_EXPORT u8_string fd_runbase_filename(u8_string suffix)
{
  if (runbase == NULL) {
    if (runbase_config == NULL) {
      u8_string wd = u8_getcwd(), appid = u8_string_subst(u8_appid(),"/",":");
      runbase = u8_mkpath(wd,appid);
      u8_free(wd);
      u8_free(appid);}
    else if (u8_directoryp(runbase_config)) {
      /* If the runbase is a directory, create files using the appid. */
      u8_string appid = u8_string_subst(u8_appid(),"/",":");
      runbase = u8_mkpath(runbase_config,appid);
      u8_free(appid);}
  /* Otherwise, use the configured name as the prefix */
    else runbase = u8_strdup(runbase_config);}
  if (suffix == NULL)
    return u8_strdup(runbase);
  else return u8_string_append(runbase,suffix,NULL);
}

static lispval config_getrunbase(lispval var,void *data)
{
  if (runbase == NULL) return FD_FALSE;
  else return lispval_string(runbase);
}

static int config_setrunbase(lispval var,lispval val,void *data)
{
  if (runbase)
    return fd_err("Runbase already set and used","config_set_runbase",runbase,VOID);
  else if (STRINGP(val)) {
    runbase_config = u8_strdup(CSTRING(val));
    return 1;}
  else return fd_type_error(_("string"),"config_setrunbase",val);
}

/* fd_setapp */

FD_EXPORT void fd_setapp(u8_string spec,u8_string statedir)
{
  if (strchr(spec,'/')) {
    u8_string fullpath=
      ((spec[0]=='/')?((u8_string)(u8_strdup(spec))):(u8_abspath(spec,NULL)));
    u8_string base = u8_basename(spec,"*");
    u8_identify_application(base);
    if (statedir) runbase = u8_mkpath(statedir,base);
    else {
      u8_string dir = u8_dirname(fullpath);
      runbase = u8_mkpath(dir,base);
      u8_free(dir);}
    u8_free(base); u8_free(fullpath);}
  else {
    u8_byte *atpos = strchr(spec,'@');
    u8_string appid = ((atpos)?(u8_slice(spec,atpos)):
                       ((u8_string)(u8_strdup(spec))));
    u8_identify_application(appid);
    if (statedir)
      runbase = u8_mkpath(statedir,appid);
    else {
      u8_string wd = u8_getcwd();
      runbase = u8_mkpath(wd,appid);
      u8_free(wd);}
    u8_free(appid);}
}

/* UID/GID setting */

static int resolve_uid(lispval val)
{
  if (FIXNUMP(val)) return (u8_gid)(FIX2INT(val));
#if ((HAVE_GETPWNAM_R)||(HAVE_GETPWNAM))
  if (STRINGP(val)) {
    struct passwd _uinfo, *uinfo; char buf[1024]; int retval;
#if HAVE_GETPWNAM_R
    retval = getpwnam_r(CSTRING(val),&_uinfo,buf,1024,&uinfo);
#else
    uinfo = getpwnam(CSTRING(val));
#endif
    if ((retval<0)||(uinfo == NULL)) {
      if (errno) u8_graberr(errno,"resolve_uid",NULL);
      fd_seterr("BadUser","resolve_uid",NULL,val);
      return (u8_uid) -1;}
    else return uinfo->pw_uid;}
  else return fd_type_error("userid","resolve_uid",val);
#else
  return -1;
#endif
}

static int resolve_gid(lispval val)
{
  if (FIXNUMP(val)) return (u8_gid)(FIX2INT(val));
#if ((HAVE_GETGRNAM_R)||(HAVE_GETGRNAM))
  if (STRINGP(val)) {
    struct group _ginfo, *ginfo; char buf[1024]; int retval;
#if HAVE_GETGRNAM_R
    retval = getgrnam_r(CSTRING(val),&_ginfo,buf,1024,&ginfo);
#else
    ginfo = getgrnam(CSTRING(val));
#endif
    if ((retval<0)||(ginfo == NULL)) {
      if (errno) u8_graberr(errno,"resolve_gid",NULL);
      fd_seterr("BadGroup","resolve_gid",NULL,val);
      return (u8_uid) -1;}
    else return ginfo->gr_gid;}
  else return fd_type_error("groupid","resolve_gid",val);
#else
  return -1;
#endif
}

/* Determine which functions to use */

#if HAVE_GETEUID
#define GETUIDFN geteuid
#elif HAVE_GETUID
#define GETUIDFN getuid
#endif

#if HAVE_SETEUID
#define SETUIDFN seteuid
#elif HAVE_SETUID
#define SETUIDFN setuid
#endif

#if HAVE_GETEGID
#define GETGIDFN getegid
#elif HAVE_GETGID
#define GETGIDFN getgid
#endif

#if HAVE_SETEGID
#define SETGIDFN setegid
#elif HAVE_SETGID
#define SETGIDFN setgid
#endif

/* User config functions */

#if (HAVE_GETUID|HAVE_GETEUID)
static lispval config_getuser(lispval var,void *data)
{
  u8_uid gid = GETUIDFN(); int ival = (int)gid;
  return FD_INT2FIX(ival);
}
#else
static lispval config_getuser(lispval var,void *fd_vecelts)
{
  return FD_FALSE;
}
#endif

#if ((HAVE_GETUID|HAVE_GETEUID)&((HAVE_SETUID|HAVE_SETEUID)))
static int config_setuser(lispval var,lispval val,void *data)
{
  u8_uid cur_uid = GETUIDFN(); int uid = resolve_uid(val);
  if (uid<0) return -1;
  else if (cur_uid == uid) return 0;
  else {
    int rv = SETUIDFN(uid);
    if (rv<0) {
      u8_graberr(errno,"config_setuser",NULL);
      u8_log(LOG_CRIT,"Can't set user","Can't change user ID from %d",cur_uid);
      fd_seterr("CantSetUser","config_setuser",NULL,uid);
      return -1;}
    return 1;}
}
#else
static int config_setuser(lispval var,lispval val,void *fd_vecelts)
{
  u8_log(LOG_CRIT,"Can't set user","Can't change user ID in this OS");
  fd_seterr("SystemCantSetUser","config_setuser",NULL,val);
  return -1;
}
#endif

/* User config functions */

#if (HAVE_GETGID|HAVE_GETEGID)
static lispval config_getgroup(lispval var,void *data)
{
  u8_gid gid = GETGIDFN(); int i = (int)gid;
  return FD_INT(i);
}
#else
static lispval config_getgroup(lispval var,void *fd_vecelts)
{
  return FD_FALSE;
}
#endif

#if (HAVE_SETGID|HAVE_SETEGID)
static int config_setgroup(lispval var,lispval val,void *data)
{
  u8_gid cur_gid = GETGIDFN(); int gid = resolve_gid(val);
  if (gid<0) return -1;
  else if (cur_gid == gid) return 0;
  else {
    int rv = SETGIDFN(gid);
    if (rv<0) {
      u8_graberr(errno,"config_setgroup",NULL);
      u8_log(LOG_CRIT,"Can't set group","Can't change group ID from %d",cur_gid);
      fd_seterr("CantSetGroup","config_setgroup",NULL,gid);
      return -1;}
    return 1;}
}
#else
static int config_setgroup(lispval var,lispval val,void *fd_vecelts)
{
  u8_log(LOG_CRIT,"Can't set group","Can't change group ID in this OS");
  fd_seterr("SystemCantSetGroup","config_setgroup",NULL,val);
  return -1;
}
#endif

/* Initialization */

static int boot_config()
{
  u8_byte *config_string = (u8_byte *)u8_getenv("FD_BOOT_CONFIG");
  u8_byte *scan, *end; int count = 0;
  if (config_string == NULL) config_string = u8_strdup(FD_BOOT_CONFIG);
  else config_string = u8_strdup(config_string);
  scan = config_string; end = strchr(scan,';');
  while (scan) {
    if (end == NULL) {
      fd_config_assignment(scan); count++;
      break;}
    *end='\0'; fd_config_assignment(scan); count++;
    scan = end+1; end = strchr(scan,';');}
  u8_free(config_string);
  return count;
}

/* Bootup message */

#define NO_COPYRIGHT_MESSAGE ""
#define COPYRIGHT_MESSAGE \
  "\nCopyright (C) beingmeta 2004-2019, all rights reserved"

FD_EXPORT int fd_boot_message()
{
  if (fd_be_vewy_quiet) return 0;
  if (boot_message_delivered) return 0;
  struct U8_XTIME xt; u8_localtime(&xt,time(NULL));
  u8_uid uid = getuid();
  U8_FIXED_OUTPUT(curtime,256);
  u8_xtime_to_rfc822_x(curtimeout,&xt,xt.u8_tzoff,0);
  u8_string appid = u8_appid();
  u8_log(U8_LOG_MSG,NULL,
         _("(%s:%lld) %-s@%-s:%-s (%s)\n%s %s%s"),
         ((appid) ? (appid) : (exe_name) ? (exe_name) : ((u8_string)"exe")),
         (unsigned long long)getpid(),
         u8_username(uid),u8_gethostname(),u8_getcwd(),
         curtime.u8_outbuf,
         fd_getrevision(),u8_getrevision(),
         COPYRIGHT_MESSAGE);
  if ( (fd_logcmd) && (fd_argc > 1) )
    log_argv(fd_argc,fd_argv);
  boot_message_delivered = 1;
  return 1;
}

/* LIMIT configs */

/* Corelimit config variable */

static lispval rlimit_get(lispval symbol,void *rlimit_id)
{
  struct rlimit limit;
  long long RLIMIT_ID = (long long) U8_PTR2INT(rlimit_id);
  int rv = getrlimit(RLIMIT_ID,&limit);
  if (rv<0) {
    u8_graberr(errno,"rlimit_get",FD_SYMBOL_NAME(symbol));
    return FD_ERROR;}
  else return FD_INT(limit.rlim_cur);
}

static int rlimit_set(lispval symbol,lispval value,void *rlimit_id)
{
  struct rlimit limit;
  long long RLIMIT_ID = (long long) U8_PTR2INT(rlimit_id);
  int rv = getrlimit(RLIMIT_ID,&limit);
  if (rv<0) {
    u8_graberr(errno,"rlimit_set",FD_SYMBOL_NAME(symbol));
    return -1;}
  else if (FIXNUMP(value))
    limit.rlim_cur = FIX2INT(value);
  else if (FD_TRUEP(value))
    limit.rlim_cur = RLIM_INFINITY;
  else if (TYPEP(value,fd_bigint_type))
    limit.rlim_cur =
      fd_bigint_to_long_long((struct FD_BIGINT *)(value));
  else {
    fd_seterr(fd_TypeError,"rlimit_set",NULL,value);
    return -1;}
  rv = setrlimit(RLIMIT_ID,&limit);
  if (rv<0) {
    u8_graberr(errno,"rlimit_set",FD_SYMBOL_NAME(symbol));
    return rv;}
  else return 1;
}

/* STDIO redirects */

u8_string stdin_filename = NULL;
u8_string stdout_filename = NULL;
u8_string stderr_filename = NULL;

static int stdout_config_set(lispval var,lispval val,void *data)
{
  if (FD_STRINGP(val)) {
    u8_string filename = FD_CSTRING(val);
    int fd = (filename[0] == '+') ?
      (open(filename+1,O_WRONLY|O_APPEND|O_CREAT,0664)) :
      (open(filename,O_WRONLY|O_TRUNC|O_CREAT,0664));
    if (fd<0) u8_graberrno("stdout_config_set/open",u8_strdup(filename));
    int rv = dup2(fd,STDOUT_FILENO);
    if (rv<0) {
      u8_graberrno("stdout_config_set/dup",u8_strdup(filename));
      close(fd);}
    return rv;}
  else {
    fd_seterr("Not a filename","stdout_config_set",NULL,val);
    return -1;}
}

static int stderr_config_set(lispval var,lispval val,void *data)
{
  if (FD_STRINGP(val)) {
    u8_string filename = FD_CSTRING(val);
    u8_string *save_filename = data;
    int fd = (filename[0] == '+') ?
      (open(filename+1,O_WRONLY|O_APPEND|O_CREAT,0664)) :
      (open(filename,O_WRONLY|O_TRUNC|O_CREAT,0664));
    if (fd<0) u8_graberrno("stderr_config_set/open",u8_strdup(filename));
    int rv = dup2(fd,STDERR_FILENO);
    if (rv<0) {
      u8_graberrno("stderr_config_set/dup",u8_strdup(filename));
      close(fd);}
    else if (filename[0] == '+')
      *save_filename = u8_strdup(filename+1);
    else *save_filename = u8_strdup(filename);
    return rv;}
  else {
    fd_seterr("Not a filename","stderr_config_set",NULL,val);
    return -1;}
}

static int stdin_config_set(lispval var,lispval val,void *data)
{
  if (FD_STRINGP(val)) {
    u8_string filename = FD_CSTRING(val);
    if (!(u8_file_existsp(filename))) {
      fd_seterr("MissingFile","stdin_config_set",filename,FD_VOID);
      return -1;}
    else {
      int fd = open(filename,O_RDONLY,0664), rv=0;
      if (fd<0) {
        u8_graberrno("stdin_config_set/open",u8_strdup(filename));}
      else rv = dup2(fd,STDIN_FILENO);
      if (rv<0) {
        u8_graberrno("stdin_config_set/dup",u8_strdup(filename));
        close(fd);}
      return rv;}}
  else {
    fd_seterr("Not a filename","stdin_config_set",NULL,val);
    return -1;}
}

/* Setting a pid file */

static int pidfile_config_set(lispval var,lispval val,void *data)
{
  u8_string filename=NULL, *fname_ptr = (u8_string *) data;
  if (FD_STRINGP(val))
    filename=u8_realpath(FD_CSTRING(val),NULL);
  else if (FD_TRUEP(val)) {
    u8_string basename = u8_string_append(u8_appid(),".pid",NULL);
    filename=u8_abspath(basename,NULL);
    u8_free(basename);}
  else filename = NULL;
  if (filename == NULL) {
    fd_seterr("BadPIDFilename","pidfile_config_set",NULL,val);
    return -1;}
  if (*fname_ptr) {
    if ( ( (*fname_ptr) == filename ) ||
         ( strcmp((*fname_ptr),filename) == 0 ) )
      return 0;
    u8_string rpath = u8_realpath(filename,NULL);
    int changed = strcmp(*fname_ptr,rpath);
    u8_free(rpath);
    if (changed == 0)
      return 0;}
  if (u8_file_existsp(filename)) {
    int read_fd = u8_open_fd(filename,O_RDONLY,0);
    if (read_fd<0) {
      u8_graberrno("pidfile_config_set/write",filename);
      return read_fd;}
    u8_byte buf[64];
    ssize_t rv = read(read_fd,buf,63); buf[rv]='\0';
    long long disk_pid = strtoll(buf,NULL,10);
    pid_t cur_pid = getpid();
    close(read_fd);
    if (disk_pid == cur_pid) {
      u8_log(LOG_NOTICE,"MatchingPID","`%s` = %d",filename,cur_pid);
      *fname_ptr = filename;
      return 0;}}
  int fd = u8_open_fd(filename,PID_OPEN_FLAGS,0644);
  if (fd<0) {
    u8_graberrno("pid_config_set",filename);
    return -1;}
  else {
    pid_t pid = getpid();
    char buf[32], *pidstring = u8_uitoa10(pid,buf);
    int rv = u8_writeall(fd,pidstring,strlen(pidstring));
    if (rv<0) {
      u8_graberrno("pidfile_config_set/write",filename);}
    else if (fname_ptr)
      *fname_ptr = filename;
    else NO_ELSE;
    close(fd);
    return rv;}
}

static void remove_pidfile()
{
  if ( (fd_exited) || (fd_in_doexit))
    return;
  else if (pid_filename) {
    if (u8_file_existsp(pid_filename))
      u8_removefile(pid_filename);}
  else fd_doexit(FD_FALSE);
}

/* Full startup */

void fd_init_startup_c()
{
  u8_register_source_file(_FILEINFO);

  u8_init_mutex(&atexit_handlers_lock);

  atexit(doexit_atexit);
  atexit(remove_pidfile);

  boot_config();

  fd_register_config("QUIET",_("Avoid unneccessary verbiage"),
                     fd_intconfig_get,fd_boolconfig_set,&fd_be_vewy_quiet);
  fd_register_config("PID",_("system process ID (read-only)"),
                     config_getpid,NULL,NULL);
  fd_register_config("PPID",_("parent's process ID (read-only)"),
                     config_getppid,NULL,NULL);

  fd_register_config("UTF8WARN",_("warn on bad UTF-8 sequences"),
                     config_getutf8warn,config_setutf8warn,NULL);
  fd_register_config("UTF8ERR",_("fail (error) on bad UTF-8 sequences"),
                     config_getutf8err,config_setutf8err,NULL);
  fd_register_config("RANDOMSEED",_("random seed used for stochastic operations"),
                     config_getrandomseed,config_setrandomseed,NULL);

  fd_register_config("CHECKUTF8",_("check that strings are valid UTF-8 on creation"),
                     fd_boolconfig_get,fd_boolconfig_set,&fd_check_utf8);

  fd_register_config("APPID",_("application ID used in messages and SESSIONID"),
                     config_getappid,config_setappid,NULL);

  fd_register_config("ARGV",
                     _("the vector of args (before parsing) to the application (no configs)"),
                     fd_lconfig_get,NULL,&raw_argv);
  fd_register_config("RAWARGS",
                     _("the vector of args (before parsing) to the application (no configs)"),
                     fd_lconfig_get,NULL,&raw_argv);
  fd_register_config("CMDARGS",
                     _("the vector of parsed args to the application (no configs)"),
     fd_lconfig_get,NULL,&lisp_argv);
  fd_register_config("ARGS",
                     _("the vector of parsed args to the application (no configs)"),
     fd_lconfig_get,NULL,&lisp_argv);
  fd_register_config("STRINGARGS",
                     _("the vector of args (before parsing) to the application (no configs)"),
     fd_lconfig_get,NULL,&string_argv);
  fd_register_config("CONFIGARGS",
                     _("config arguments passed to the application (unparsed)"),
                     fd_lconfig_get,NULL,&config_argv);
  fd_register_config("EXENAME",
                     _("the vector of args (before parsing) to the application (no configs)"),
                     fd_lconfig_get,NULL,&exec_arg);



  fd_register_config("LOGCMD",_("Whether to display command line args on entry and exit"),
                     fd_boolconfig_get,fd_boolconfig_set,&fd_logcmd);



  fd_register_config("SESSIONID",_("unique session identifier"),
                     config_getsessionid,config_setsessionid,NULL);
  fd_register_config("FASTEXIT",_("whether to recycle session state on exit"),
                     fd_boolconfig_get,fd_boolconfig_set,&fd_fast_exit);
  fd_register_config("EXIT:FAST",_("whether to recycle session state on exit"),
                     fd_boolconfig_get,fd_boolconfig_set,&fd_fast_exit);
  fd_register_config("RUNUSER",_("Set the user ID for this process"),
                     config_getuser,config_setuser,NULL);
  fd_register_config("RUNGROUP",_("Set the group ID for this process"),
                     config_getgroup,config_setgroup,NULL);


  fd_register_config("RUNBASE",_("Path prefix for program state files"),
                     config_getrunbase,config_setrunbase,NULL);

  fd_register_config("STDOUT",_("Redirect standard output to file"),
                     fd_sconfig_get,stdout_config_set,&stdout_filename);
  fd_register_config("STDERR",_("Redirect standard output to file"),
                     fd_sconfig_get,stderr_config_set,&stderr_filename);
  fd_register_config("STDIN",_("Redirect standard input to file"),
                     fd_sconfig_get,stdin_config_set,&stdin_filename);
  fd_register_config("PIDFILE",_("Write PID to file, delete on exit"),
                     fd_sconfig_get,pidfile_config_set,&pid_filename);

#if HAVE_SYS_RESOURCE_H
#ifdef RLIMIT_CPU
  fd_register_config("MAXCPU",_("Max CPU execution time limit"),
                     fd_config_rlimit_get,fd_config_rlimit_set,
                     (void *)&MAXCPU);
#endif
#ifdef RLIMIT_RSS
  fd_register_config("MAXRSS",_("Max resident set (RSS) size"),
                     fd_config_rlimit_get,fd_config_rlimit_set,
                     (void *)&MAXRSS);
#endif
#ifdef RLIMIT_CORE
  fd_register_config("MAXCORE",_("Max core dump size"),
                     fd_config_rlimit_get,fd_config_rlimit_set,
                     (void *)&MAXCORE);
#endif
#ifdef RLIMIT_NPROC
  fd_register_config("MAXNPROC",_("Max number of subprocesses"),
                     fd_config_rlimit_get,fd_config_rlimit_set,
                     (void *)&MAXNPROC);
#endif
#ifdef RLIMIT_NOFILE
  fd_register_config("MAXFILES",_("Max number of open file descriptors"),
                     fd_config_rlimit_get,fd_config_rlimit_set,
                     (void *)&MAXFILES);
#endif
#ifdef RLIMIT_STACK
  fd_register_config("MAXSTACK",_("Max stack depth"),
                     fd_config_rlimit_get,fd_config_rlimit_set,
                     (void *)&MAXSTACK);
#endif

#endif


#if HAVE_SYS_RESOURCE_H
#ifdef RLIMIT_CPU
  fd_register_config("MAXCPU",_("Max CPU execution time limit"),
                     fd_config_rlimit_get,fd_config_rlimit_set,
                     (void *)&MAXCPU);
#endif

#ifdef RLIMIT_RSS
  fd_register_config("MAXRSS",_("Max resident set (RSS) size"),
                     fd_config_rlimit_get,fd_config_rlimit_set,
                     (void *)&MAXRSS);
#endif
#ifdef RLIMIT_CORE
  fd_register_config("MAXCORE",_("Max core dump size"),
                     fd_config_rlimit_get,fd_config_rlimit_set,
                     (void *)&MAXCORE);
#endif
#ifdef RLIMIT_NPROC
  fd_register_config("MAXNPROC",_("Max number of subprocesses"),
                     fd_config_rlimit_get,fd_config_rlimit_set,
                     (void *)&MAXNPROC);
#endif
#ifdef RLIMIT_NOFILE
  fd_register_config("MAXFILES",_("Max number of open file descriptors"),
                     fd_config_rlimit_get,fd_config_rlimit_set,
                     (void *)&MAXFILES);
#endif
#ifdef RLIMIT_STACK
  fd_register_config("MAXSTACK",_("Max stack depth"),
                     fd_config_rlimit_get,fd_config_rlimit_set,
                     (void *)&MAXSTACK);
#endif

#endif

  if (!(logdir)) logdir = u8_strdup(FD_LOG_DIR);
  fd_register_config("LOGDIR",_("Root FramerD logging directories"),
                     fd_sconfig_get,fd_sconfig_set,&logdir);

  if (!(sharedir)) sharedir = u8_strdup(FD_SHARE_DIR);
  fd_register_config("SHAREDIR",_("Shared config/data directory for FramerD"),
                     fd_sconfig_get,fd_sconfig_set,&sharedir);

  if (!(datadir)) datadir = u8_strdup(FD_DATA_DIR);
  fd_register_config("DATADIR",_("Data directory for FramerD"),
                     fd_sconfig_get,fd_sconfig_set,&datadir);

  fd_register_config("SOURCES",_("Registered source files"),
                     config_get_source_files,config_add_source_file,
                     &u8_log_show_procinfo);

  fd_register_config("U8:MMAPTHRESH",
                     _("Size at which u8_big_alloc starts using MMAP"),
                     fd_sizeconfig_get,fd_sizeconfig_set,
                     &u8_mmap_threshold);
  fd_register_config("HASH:BIGTHRESH",
                     _("Number of buckets at which hashtables use bigalloc"),
                     fd_sizeconfig_get,fd_sizeconfig_set,
                     &fd_hash_bigthresh);
  fd_register_config("BUFIO:BIGTHRESH",
                     _("Size of binary buffers to use bigalloc"),
                     fd_sizeconfig_get,fd_sizeconfig_set,
                     &fd_bigbuf_threshold);

  fd_register_config("ATEXIT",_("Procedures to call on exit"),
                     config_atexit_get,config_atexit_set,NULL);

  fd_register_config("EXITING",_("Whether this process is exiting"),
                     fd_boolconfig_get,fd_boolconfig_set,&fd_exiting);


  fd_register_config
    ("CORELIMIT",_("Set core size limit"),
     rlimit_get,rlimit_set,(void *)RLIMIT_CORE);
  fd_register_config
    ("CPULIMIT",_("Set cpu time limit (in seconds)"),
     rlimit_get,rlimit_set,((void *)RLIMIT_CPU));
  fd_register_config
    ("RSSLIMIT",_("Set resident memory limit"),
     rlimit_get,rlimit_set,(void *)RLIMIT_RSS);
  fd_register_config
    ("VMEMLIMIT",_("Set total VMEM limit"),
     rlimit_get,rlimit_set,(void *)RLIMIT_AS);
  fd_register_config
    ("HEAPLIMIT",_("Set total heap (DATA segment) limit"),
     rlimit_get,rlimit_set,(void *)RLIMIT_DATA);

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
