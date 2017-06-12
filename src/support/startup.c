/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
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

#if HAVE_UNISTD_H
#include <unistd.h>
#endif

#if HAVE_GRP_H
#include <grp.h>
#endif

u8_condition fd_ArgvConfig=_("Config (argv)");
u8_condition SetRLimit=_("SetRLimit");
fd_exception fd_ExitException=_("Unhandled exception at exit");

static u8_mutex atexit_handlers_lock;

int fd_exiting = 0;
int fd_exited = 0;

static u8_string logdir = NULL, sharedir = NULL, datadir = NULL;

int fd_be_vewy_quiet = 0;
static int boot_message_delivered = 0;

/* Processing argc,argv */

lispval *fd_argv = NULL;
int fd_argc = -1;

static void set_vector_length(lispval vector,int len);
static lispval exec_arg = FD_FALSE, lisp_argv = FD_FALSE, string_argv = FD_FALSE;
static lispval raw_argv = FD_FALSE, config_argv = FD_FALSE;
static int init_argc = 0;
static size_t app_argc;

/* This takes an argv, argc combination and processes the argv elements
   which are configs (var = value again) */
FD_EXPORT lispval *fd_handle_argv(int argc,char **argv,
                                 unsigned int arg_mask,
                                 size_t *arglen_ptr)
{
  if (argc>0) {
    u8_string exe_name = u8_fromlibc(argv[0]);
    lispval interp = fd_lispstring(exe_name);
    fd_set_config("INTERPRETER",interp);
    fd_set_config("EXE",interp);
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
      if (i==0) {
        i++; u8_free(arg); continue;} 
      else if ( ( n < 32 ) && ( ( (arg_mask) & (1<<i)) !=0 ) ) {
        i++; u8_free(arg); continue;}
      else i++;
      if ((eq!=NULL) && (eq>arg) && (*(eq-1)!='\\')) {
        int retval = (arg!=NULL) ? (fd_config_assignment(arg)) : (-1);
        FD_VECTOR_SET(config_args,config_i,lispval_string(arg)); config_i++;
        if (retval<0) {
          u8_log(LOGCRIT,"FailedConfig",
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
        return_args[n]=lisp_arg; fd_incref(lisp_arg);}
      _fd_argv[n]=lisp_arg; fd_incref(lisp_arg);
      FD_VECTOR_SET(lisp_args,n,lisp_arg);
      FD_VECTOR_SET(string_args,n,string_arg);
      u8_free(arg);
      n++;}
    set_vector_length(lisp_args,n);
    lisp_argv = lisp_args;
    set_vector_length(string_args,n);
    string_argv = string_args;
    set_vector_length(config_args,n);
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
  u8_log(LOGCRIT,"Internal/CmdArgInitVec","Not a vector! %q",
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
  struct FD_ATEXIT *exitfn_next;} *atexit_handlers = NULL;
static int n_atexit_handlers = 0;

static lispval config_atexit_get(lispval var,void *data)
{
  struct FD_ATEXIT *scan; int i = 0; lispval result;
  u8_lock_mutex(&atexit_handlers_lock);
  result = fd_make_vector(n_atexit_handlers,NULL);
  scan = atexit_handlers; while (scan) {
    lispval handler = scan->exitfn_handler; fd_incref(handler);
    FD_VECTOR_SET(result,i,handler);
    scan = scan->exitfn_next; i++;}
  u8_unlock_mutex(&atexit_handlers_lock);
  return result;
}

static int config_atexit_set(lispval var,lispval val,void *data)
{
  struct FD_ATEXIT *fresh = u8_malloc(sizeof(struct FD_ATEXIT));
  if (!(FD_APPLICABLEP(val))) {
    fd_type_error("applicable","config_atexit",val);
    return -1;}
  u8_lock_mutex(&atexit_handlers_lock);
  fresh->exitfn_next = atexit_handlers; 
  fresh->exitfn_handler = val;
  fd_incref(val);
  n_atexit_handlers++; atexit_handlers = fresh;
  u8_unlock_mutex(&atexit_handlers_lock);
  return 1;
}

FD_EXPORT void fd_doexit(lispval arg)
{
  struct FD_ATEXIT *scan, *tmp;
  if (fd_exited) return;
  if (fd_exiting) {
    u8_log(LOGWARN,"RecursiveExit","Recursive fd_doexit");
    return;}
  fd_exiting = 1;
  if (fd_argv) {
    int i = 0, n = fd_argc; while (i<n) {
      lispval elt = fd_argv[i++]; fd_decref(elt);}
    u8_free(fd_argv);
    fd_argv = NULL;
    fd_argc = -1;}
  if (!(atexit_handlers)) {
    u8_log(LOG_DEBUG,"fd_doexit","No FramerD exit handlers!");
    return;}
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
    tmp = scan;
    scan = scan->exitfn_next;
    u8_free(tmp);}
  fd_decref(exec_arg); exec_arg = FD_FALSE;
  fd_decref(lisp_argv); lisp_argv = FD_FALSE;
  fd_decref(string_argv); string_argv = FD_FALSE;
  fd_decref(raw_argv); raw_argv = FD_FALSE;
  fd_decref(config_argv); config_argv = FD_FALSE;
  fd_exited=1;
  fd_exiting=0;
}

static void doexit_atexit()
{
  if ( (fd_exited) || (fd_exiting))
    return;
  else fd_doexit(FD_FALSE);
}

FD_EXPORT void fd_signal_doexit(int sig)
{
  fd_doexit(FD_INT(sig));
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

static fd_exception TimeFailed="call to time() failed";

static long long randomseed = 0x327b23c6;

static lispval config_getrandomseed(lispval var,void *data)
{
  if (randomseed<FD_MAX_FIXNUM) 
    return FD_INT(randomseed);
  else return (lispval)fd_long_long_to_bigint(randomseed);
}

static int config_setrandomseed(lispval var,lispval val,void *data)
{
  if (((SYMBOLP(val)) && ((strcmp(FD_XSYMBOL_NAME(val),"TIME"))==0)) ||
      ((STRINGP(val)) && ((strcmp(CSTRING(val),"TIME"))==0))) {
    time_t tick = time(NULL);
    if (tick<0) {
      u8_graberr(-1,"time",NULL);
      fd_seterr2(TimeFailed,"setrandomseed");
      return -1;}
    else {
      randomseed = (unsigned int)tick;
      u8_randomize(randomseed);
      return 1;}}
  else if ((FIXNUMP(val))&&
	   (FIX2INT(val)>0)&&
	   (FIX2INT(val)<UINT_MAX)) {
    long long intval = FIX2INT(val);
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
  if (FIXNUMP(val)) return (gid_t)(FIX2INT(val));
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
      return (uid_t) -1;}
    else return uinfo->pw_uid;}
  else return fd_type_error("userid","resolve_uid",val);
#else
  return -1;
#endif
}

static int resolve_gid(lispval val)
{
  if (FIXNUMP(val)) return (gid_t)(FIX2INT(val));
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
      return (uid_t) -1;}
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
  uid_t gid = GETUIDFN(); int ival = (int)gid;
  return FD_INT(ival);
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
  uid_t cur_uid = GETUIDFN(); int uid = resolve_uid(val);
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
  gid_t gid = GETGIDFN(); int i = (int)gid;
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
  gid_t cur_gid = GETGIDFN(); int gid = resolve_gid(val);
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

FD_EXPORT int fd_boot_message()
{
  if (fd_be_vewy_quiet) return 0;
  if (boot_message_delivered) return 0;
  u8_message("Copyright (C) beingmeta 2004-2017, all rights reserved");
  u8_message("(%s:%lld) %s %s",
             u8_appid(),(unsigned long long)getpid(),
             fd_getrevision(),u8_getrevision());
  boot_message_delivered = 1;
  return 1;
}

void fd_init_startup_c()
{
  u8_register_source_file(_FILEINFO);

  u8_init_mutex(&atexit_handlers_lock);

  atexit(doexit_atexit);
  /* atexit(report_errors_atexit); */

  boot_config();

  fd_register_config
    ("QUIET",_("Avoid unneccessary verbiage"),
     fd_intconfig_get,fd_boolconfig_set,&fd_be_vewy_quiet);
  fd_register_config
    ("PID",_("system process ID (read-only)"),
     config_getpid,NULL,NULL);
  fd_register_config
    ("PPID",_("parent's process ID (read-only)"),
     config_getppid,NULL,NULL);

  fd_register_config
    ("UTF8WARN",_("warn on bad UTF-8 sequences"),
     config_getutf8warn,config_setutf8warn,NULL);
  fd_register_config
    ("UTF8ERR",_("fail (error) on bad UTF-8 sequences"),
     config_getutf8err,config_setutf8err,NULL);
  fd_register_config
    ("RANDOMSEED",_("random seed used for stochastic operations"),
     config_getrandomseed,config_setrandomseed,NULL);

  fd_register_config
    ("APPID",_("application ID used in messages and SESSIONID"),
     config_getappid,config_setappid,NULL);

  fd_register_config
    ("ARGV",
     _("the vector of args (before parsing) to the application (no configs)"),
     fd_lconfig_get,NULL,&raw_argv);
  fd_register_config
    ("RAWARGS",
     _("the vector of args (before parsing) to the application (no configs)"),
     fd_lconfig_get,NULL,&raw_argv);
  fd_register_config
    ("CMDARGS",_("the vector of parsed args to the application (no configs)"),
     fd_lconfig_get,NULL,&lisp_argv);
  fd_register_config
    ("ARGS",_("the vector of parsed args to the application (no configs)"),
     fd_lconfig_get,NULL,&lisp_argv);
  fd_register_config
    ("STRINGARGS",
     _("the vector of args (before parsing) to the application (no configs)"),
     fd_lconfig_get,NULL,&string_argv);
  fd_register_config
    ("CONFIGARGS",_("config arguments passed to the application (unparsed)"),
     fd_lconfig_get,NULL,&config_argv);
  
  fd_register_config
    ("SESSIONID",_("unique session identifier"),
     config_getsessionid,config_setsessionid,NULL);
  fd_register_config
    ("RUNUSER",_("Set the user ID for this process"),
     config_getuser,config_setuser,NULL);
  fd_register_config
    ("RUNGROUP",_("Set the group ID for this process"),
     config_getgroup,config_setgroup,NULL);


  fd_register_config("RUNBASE",_("Path prefix for program state files"),
                     config_getrunbase,config_setrunbase,NULL);

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
  fd_register_config
    ("LOGDIR",_("Root FramerD logging directories"),
     fd_sconfig_get,fd_sconfig_set,&logdir);

  if (!(sharedir)) sharedir = u8_strdup(FD_SHARE_DIR);
  fd_register_config
    ("SHAREDIR",_("Shared config/data directory for FramerD"),
     fd_sconfig_get,fd_sconfig_set,&sharedir);

  if (!(datadir)) datadir = u8_strdup(FD_DATA_DIR);
  fd_register_config
    ("DATADIR",_("Data directory for FramerD"),
     fd_sconfig_get,fd_sconfig_set,&datadir);

  fd_register_config
    ("SOURCES",_("Registered source files"),
     config_get_source_files,config_add_source_file,
     &u8_log_show_procinfo);

  fd_register_config("ATEXIT",_("Procedures to call on exit"),
                     config_atexit_get,config_atexit_set,NULL);
}
