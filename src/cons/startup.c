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

static u8_string logdir=NULL, sharedir=NULL, datadir=NULL;

/* Processing argc,argv */

fdtype *fd_argv=NULL;
int fd_argc=-1;

static void set_vector_length(fdtype vector,int len);
static fdtype exec_arg=FD_FALSE, lisp_argv=FD_FALSE, string_argv=FD_FALSE;
static fdtype raw_argv=FD_FALSE, config_argv=FD_FALSE;
static int init_argc=0;
static size_t app_argc;

/* This takes an argv, argc combination and processes the argv elements
   which are configs (var=value again) */
FD_EXPORT fdtype *fd_handle_argv(int argc,char **argv,
                                 unsigned int arg_mask,
                                 size_t *arglen_ptr)
{
  if (argc>0) {
    u8_string exe_name=u8_fromlibc(argv[0]);
    fdtype interp=fd_lispstring(exe_name);
    fd_set_config("INTERPRETER",interp);
    fd_set_config("EXE",interp);
    fd_decref(interp);}

  if (fd_argv!=NULL)  {
    if ((init_argc>0) && (argc != init_argc)) 
      u8_log(LOG_WARN,"InconsistentArgv/c",
             "Trying to reprocess argv with a different argc (%d) length != %d",
             argc,init_argc);
    if (arglen_ptr) *arglen_ptr=fd_argc;
    return fd_argv;}
  else if (argc<=0) {
    u8_log(LOG_CRIT,"fd_handle_arg(invalid argv)",
           _("The argc length %d is not valid (>0)"),argc);
    return NULL;}
  else if (argv==NULL) {
    u8_log(LOG_CRIT,"fd_handle_arg(invalid argv)",
           _("The argv argument cannot be NULL!"),argc);
    return NULL;}
  else {
    int i=0, n=0, config_i=0;
    fdtype string_args=fd_make_vector(argc-1,NULL), string_arg=FD_VOID;
    fdtype lisp_args=fd_make_vector(argc-1,NULL), lisp_arg=FD_VOID;
    fdtype config_args=fd_make_vector(argc-1,NULL);
    fdtype raw_args=fd_make_vector(argc,NULL);
    fdtype *return_args=(arglen_ptr) ? (u8_alloc_n(argc-1,fdtype)) : (NULL);
    fdtype *_fd_argv=u8_alloc_n(argc-1,fdtype);

    u8_threadcheck();

    init_argc=argc;

    while (i<argc) {
      char *carg=argv[i];
      u8_string arg=u8_fromlibc(carg), eq=strchr(arg,'=');
      FD_VECTOR_SET(raw_args,i,fdtype_string(arg));
      /* Don't include argv[0] in the arglists */
      if (i==0) {
        i++; u8_free(arg); continue;} 
      else if ( ( n < 32 ) && ( ( (arg_mask) & (1<<i)) !=0 ) ) {
        i++; u8_free(arg); continue;}
      else i++;
      if ((eq!=NULL) && (eq>arg) && (*(eq-1)!='\\')) {
        int retval=(arg!=NULL) ? (fd_config_assignment(arg)) : (-1);
        FD_VECTOR_SET(config_args,config_i,fdtype_string(arg)); config_i++;
        if (retval<0) {
          u8_log(LOGCRIT,"FailedConfig",
                 "Couldn't handle the config argument `%s`",
                 (arg==NULL) ? ((u8_string)carg) : (arg));
          u8_clear_errors(0);}
        else u8_log(LOG_INFO,fd_ArgvConfig,"   %s",arg);
        u8_free(arg);
        continue;}
      string_arg=fdtype_string(arg);
      /* Note that fd_parse_arg should always return at least a lisp
         string */
      lisp_arg=fd_parse_arg(arg);
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
    app_argc=n;
    fd_argv = _fd_argv;
    fd_argc = n;
    if (return_args) {
      if (arglen_ptr) *arglen_ptr=n;
      return return_args;}
    else return NULL;}
}

static void set_vector_length(fdtype vector,int len)
{
  if (FD_VECTORP(vector)) {
    struct FD_VECTOR *vec = (struct FD_VECTOR *) vector;
    if (len>=0) {
      vec->fd_veclen=len;
      return;}}
  u8_log(LOGCRIT,"Internal/CmdArgInitVec","Not a vector! %q",
         vector);
} 

/* Accessing source file registry */

static void add_source_file(u8_string s,void *vp)
{
  fdtype *valp=(fdtype *)vp;
  fdtype val=*valp;
  FD_ADD_TO_CHOICE(val,fdtype_string(s));
  *valp=val;
}

static fdtype config_get_source_files(fdtype var,void *data)
{
  fdtype result=FD_EMPTY_CHOICE;
  u8_for_source_files(add_source_file,&result);
  return result;
}

static int config_add_source_file(fdtype var,fdtype val,void *data)
{
  if (FD_STRINGP(val)) {
    u8_string stringval=u8_strdup(FD_STRDATA(val));
    u8_register_source_file(stringval);
    return 1;}
  else {
    fd_type_error(_("string"),"config_addsourcefile",val);
    return -1;}
}

/* Termination */

static struct FD_ATEXIT {
  fdtype fd_exit_handler;
  struct FD_ATEXIT *fd_next_atexit;} *atexit_handlers=NULL;
static int n_atexit_handlers=0;

static fdtype config_atexit_get(fdtype var,void *data)
{
  struct FD_ATEXIT *scan; int i=0; fdtype result;
  u8_lock_mutex(&atexit_handlers_lock);
  result=fd_make_vector(n_atexit_handlers,NULL);
  scan=atexit_handlers; while (scan) {
    fdtype handler=scan->fd_exit_handler; fd_incref(handler);
    FD_VECTOR_SET(result,i,handler);
    scan=scan->fd_next_atexit; i++;}
  u8_unlock_mutex(&atexit_handlers_lock);
  return result;
}

static int config_atexit_set(fdtype var,fdtype val,void *data)
{
  struct FD_ATEXIT *fresh=u8_malloc(sizeof(struct FD_ATEXIT));
  if (!(FD_APPLICABLEP(val))) {
    fd_type_error("applicable","config_atexit",val);
    return -1;}
  u8_lock_mutex(&atexit_handlers_lock);
  fresh->fd_next_atexit=atexit_handlers; fresh->fd_exit_handler=val;
  fd_incref(val);
  n_atexit_handlers++; atexit_handlers=fresh;
  u8_unlock_mutex(&atexit_handlers_lock);
  return 1;
}

FD_EXPORT void fd_doexit(fdtype arg)
{
  struct FD_ATEXIT *scan, *tmp;
  fd_exiting=1;
  if (fd_argv) {
    int i=0, n=fd_argc; while (i<n) {
      fdtype elt=fd_argv[i++]; fd_decref(elt);}
    u8_free(fd_argv);
    fd_argv=NULL;
    fd_argc=-1;}
  if (!(atexit_handlers)) {
    u8_log(LOG_DEBUG,"fd_doexit","No FramerD exit handlers!");
    return;}
  u8_lock_mutex(&atexit_handlers_lock);
  u8_log(LOG_NOTICE,"fd_doexit","Running %d FramerD exit handlers",
         n_atexit_handlers);
  scan=atexit_handlers; atexit_handlers=NULL;
  u8_unlock_mutex(&atexit_handlers_lock);
  while (scan) {
    fdtype handler=scan->fd_exit_handler, result=FD_VOID;
    u8_log(LOG_INFO,"fd_doexit","Running FramerD exit handler %q",handler);
    if ((FD_FUNCTIONP(handler))&&(FD_FUNCTION_ARITY(handler)))
      result=fd_apply(handler,1,&arg);
    else result=fd_apply(handler,0,NULL);
    if (FD_ABORTP(result)) {
      fd_clear_errors(1);}
    else fd_decref(result);
    fd_decref(handler);
    tmp=scan;
    scan=scan->fd_next_atexit;
    u8_free(tmp);}
  fd_decref(exec_arg); exec_arg=FD_FALSE;
  fd_decref(lisp_argv); lisp_argv=FD_FALSE;
  fd_decref(string_argv); string_argv=FD_FALSE;
  fd_decref(raw_argv); raw_argv=FD_FALSE;
  fd_decref(config_argv); config_argv=FD_FALSE;
}

static void doexit_atexit(){fd_doexit(FD_FALSE);}


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

FD_EXPORT fdtype fd_config_rlimit_get(fdtype ignored,void *vptr)
{
  struct rlimit rlim;
  struct NAMED_RLIMIT *nrl=(struct NAMED_RLIMIT *)vptr;
  int retval=getrlimit(nrl->code,&rlim);
  if (retval<0) {
    u8_condition cond=u8_strerror(errno);
    errno=0;
    return fd_err(cond,"rlimit_get",u8_strdup(nrl->name),FD_VOID);}
  else if (rlim.rlim_cur==RLIM_INFINITY)
    return FD_FALSE;
  else return FD_INT((long long)(rlim.rlim_cur));
}

FD_EXPORT int fd_config_rlimit_set(fdtype ignored,fdtype v,void *vptr)
{
  struct rlimit rlim;
  struct NAMED_RLIMIT *nrl=(struct NAMED_RLIMIT *)vptr;
  int retval=getrlimit(nrl->code,&rlim); rlim_t setval;
  if ((FD_FIXNUMP(v))||(FD_BIGINTP(v))) {
    long lval=fd_getint(v);
    if (lval<0) {
      fd_incref(v);
      fd_seterr(fd_TypeError,"fd_config_rlimit_set",
                u8_strdup("resource limit (integer)"),v);
      return -1;}
    else setval=lval;}
  else if (FD_FALSEP(v)) setval=(RLIM_INFINITY);
  else if ((FD_STRINGP(v))&&
           ((strcasecmp(FD_STRDATA(v),"unlimited")==0)||
            (strcasecmp(FD_STRDATA(v),"nolimit")==0)||
            (strcasecmp(FD_STRDATA(v),"infinity")==0)||
            (strcasecmp(FD_STRDATA(v),"infinite")==0)||
            (strcasecmp(FD_STRDATA(v),"false")==0)||
            (strcasecmp(FD_STRDATA(v),"none")==0)))
    setval=(RLIM_INFINITY);
  else {
    fd_incref(v);
    fd_seterr(fd_TypeError,"fd_config_rlimit_set",
              u8_strdup("resource limit (integer)"),v);
    return -1;}
  if (retval<0) {
    u8_condition cond=u8_strerror(errno); errno=0;
    return fd_err(cond,"rlimit_get",u8_strdup(nrl->name),FD_VOID);}
  else if (setval>rlim.rlim_max) {
    /* Should be more informative */
    fd_seterr(_("RLIMIT too high"),"set_rlimit",u8_strdup(nrl->name),FD_VOID);
    return -1;}
  if (setval==rlim.rlim_cur)
    u8_log(LOG_WARN,SetRLimit,
	   "Setting for %s did not need to change",
	   nrl->name);
  else if (setval==RLIM_INFINITY)
    u8_log(LOG_WARN,SetRLimit,
	   "Setting %s to unlimited from %d",
	   nrl->name,rlim.rlim_cur);
  else u8_log(LOG_WARN,SetRLimit,
	      "Setting %s to %lld from %lld",
	      nrl->name,
	      (long long)setval,
	      rlim.rlim_cur);
  rlim.rlim_cur=setval;
  retval=setrlimit(nrl->code,&rlim);
  if (retval<0) {
    u8_condition cond=u8_strerror(errno); errno=0;
    return fd_err(cond,"rlimit_set",u8_strdup(nrl->name),FD_VOID);}
  else return 1;
}
#endif

/* Configuration for session and app id info */

static fdtype config_getappid(fdtype var,void *data)
{
  return fdtype_string(u8_appid());
}

static int config_setappid(fdtype var,fdtype val,void *data)
{
  if (FD_STRINGP(val)) {
    u8_identify_application(FD_STRDATA(val));
    return 1;}
  else return -1;
}

static fdtype config_getpid(fdtype var,void *data)
{
  pid_t pid=getpid();
  return FD_INT(((unsigned long)pid));
}

static fdtype config_getppid(fdtype var,void *data)
{
  pid_t pid=getppid();
  return FD_INT(((unsigned long)pid));
}

static fdtype config_getsessionid(fdtype var,void *data)
{
  return fdtype_string(u8_sessionid());
}

static int config_setsessionid(fdtype var,fdtype val,void *data)
{
  if (FD_STRINGP(val)) {
    u8_identify_session(FD_STRDATA(val));
    return 1;}
  else return -1;
}

static fdtype config_getutf8warn(fdtype var,void *data)
{
  if (u8_config_utf8warn(-1))
    return FD_TRUE;
  else return FD_FALSE;
}

static int config_setutf8warn(fdtype var,fdtype val,void *data)
{
  if (FD_TRUEP(val))
    if (u8_config_utf8warn(1)) return 0;
    else return 1;
  else if (u8_config_utf8warn(0)) return 1;
  else return 0;
}

static fdtype config_getutf8err(fdtype var,void *data)
{
  if (u8_config_utf8err(-1))
    return FD_TRUE;
  else return FD_FALSE;
}

static int config_setutf8err(fdtype var,fdtype val,void *data)
{
  if (FD_TRUEP(val))
    if (u8_config_utf8err(1)) return 0;
    else return 1;
  else if (u8_config_utf8err(0)) return 1;
  else return 0;
}

/* Random seed initialization */

static fd_exception TimeFailed="call to time() failed";

static unsigned int randomseed=0x327b23c6;

static fdtype config_getrandomseed(fdtype var,void *data)
{
  if (randomseed<FD_MAX_FIXNUM) return FD_INT(randomseed);
  else return (fdtype)fd_ulong_to_bigint(randomseed);
}

static int config_setrandomseed(fdtype var,fdtype val,void *data)
{
  if (((FD_SYMBOLP(val)) && ((strcmp(FD_XSYMBOL_NAME(val),"TIME"))==0)) ||
      ((FD_STRINGP(val)) && ((strcmp(FD_STRDATA(val),"TIME"))==0))) {
    time_t tick=time(NULL);
    if (tick<0) {
      u8_graberr(-1,"time",NULL);
      fd_seterr(TimeFailed,"setrandomseed",NULL,FD_VOID);
      return -1;}
    else {
      randomseed=(unsigned int)tick;
      u8_randomize(randomseed);
      return 1;}}
  else if (FD_FIXNUMP(val)) {
    randomseed=FD_FIX2INT(val);
    u8_randomize(randomseed);
    return 1;}
  else if (FD_BIGINTP(val)) {
    randomseed=(unsigned int)(fd_bigint_to_long((fd_bigint)val));
    u8_randomize(randomseed);
    return 1;}
  else return -1;
}

/* RUNBASE */

static u8_string runbase_config=NULL, runbase=NULL;

FD_EXPORT u8_string fd_runbase_filename(u8_string suffix)
{
  if (runbase==NULL) {
    if (runbase_config==NULL) {
      u8_string wd=u8_getcwd(), appid=u8_string_subst(u8_appid(),"/",":");
      runbase=u8_mkpath(wd,appid);
      u8_free(appid);}
    else if (u8_directoryp(runbase_config)) {
      /* If the runbase is a directory, create files using the appid. */
      u8_string appid=u8_string_subst(u8_appid(),"/",":");
      runbase=u8_mkpath(runbase_config,appid);
      u8_free(appid);}
  /* Otherwise, use the configured name as the prefix */
    else runbase=u8_strdup(runbase_config);}
  if (suffix==NULL)
    return u8_strdup(runbase);
  else return u8_string_append(runbase,suffix,NULL);
}

static fdtype config_getrunbase(fdtype var,void *data)
{
  if (runbase==NULL) return FD_FALSE;
  else return fdtype_string(runbase);
}

static int config_setrunbase(fdtype var,fdtype val,void *data)
{
  if (runbase)
    return fd_err("Runbase already set and used","config_set_runbase",runbase,FD_VOID);
  else if (FD_STRINGP(val)) {
    runbase_config=u8_strdup(FD_STRDATA(val));
    return 1;}
  else return fd_type_error(_("string"),"config_setrunbase",val);
}

/* fd_setapp */

FD_EXPORT void fd_setapp(u8_string spec,u8_string statedir)
{
  if (strchr(spec,'/')) {
    u8_string fullpath=
      ((spec[0]=='/')?((u8_string)(u8_strdup(spec))):(u8_abspath(spec,NULL)));
    u8_string base=u8_basename(spec,"*");
    u8_identify_application(base);
    if (statedir) runbase=u8_mkpath(statedir,base);
    else {
      u8_string dir=u8_dirname(fullpath);
      runbase=u8_mkpath(dir,base);
      u8_free(dir);}
    u8_free(base); u8_free(fullpath);}
  else {
    u8_byte *atpos=strchr(spec,'@');
    u8_string appid=((atpos)?(u8_slice(spec,atpos)):
                     ((u8_string)(u8_strdup(spec))));
    u8_identify_application(appid);
    if (statedir)
      runbase=u8_mkpath(statedir,appid);
    else {
      u8_string wd=u8_getcwd();
      runbase=u8_mkpath(wd,appid);
      u8_free(wd);}
    u8_free(appid);}
}

/* UID/GID setting */

static int resolve_uid(fdtype val)
{
  if (FD_FIXNUMP(val)) return (gid_t)(FD_FIX2INT(val));
#if ((HAVE_GETPWNAM_R)||(HAVE_GETPWNAM))
  if (FD_STRINGP(val)) {
    struct passwd _uinfo, *uinfo; char buf[1024]; int retval;
#if HAVE_GETPWNAM_R
    retval=getpwnam_r(FD_STRDATA(val),&_uinfo,buf,1024,&uinfo);
#else
    uinfo=getpwnam(FD_STRDATA(val));
#endif
    if ((retval<0)||(uinfo==NULL)) {
      if (errno) u8_graberr(errno,"resolve_uid",NULL);
      fd_seterr("BadUser","resolve_uid",NULL,val);
      return (uid_t) -1;}
    else return uinfo->pw_uid;}
  else return fd_type_error("userid","resolve_uid",val);
#else
  return -1;
#endif
}

static int resolve_gid(fdtype val)
{
  if (FD_FIXNUMP(val)) return (gid_t)(FD_FIX2INT(val));
#if ((HAVE_GETGRNAM_R)||(HAVE_GETGRNAM))
  if (FD_STRINGP(val)) {
    struct group _ginfo, *ginfo; char buf[1024]; int retval;
#if HAVE_GETGRNAM_R
    retval=getgrnam_r(FD_STRDATA(val),&_ginfo,buf,1024,&ginfo);
#else
    ginfo=getgrnam(FD_STRDATA(val));
#endif
    if ((retval<0)||(ginfo==NULL)) {
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
static fdtype config_getuser(fdtype var,void *data)
{
  uid_t gid=GETUIDFN(); int ival=(int)gid;
  return FD_INT(ival);
}
#else
static fdtype config_getuser(fdtype var,void *fd_vecelts)
{
  return FD_FALSE;
}
#endif

#if ((HAVE_GETUID|HAVE_GETEUID)&((HAVE_SETUID|HAVE_SETEUID)))
static int config_setuser(fdtype var,fdtype val,void *data)
{
  uid_t cur_uid=GETUIDFN(); int uid=resolve_uid(val);
  if (uid<0) return -1;
  else if (cur_uid==uid) return 0;
  else {
    int rv=SETUIDFN(uid);
    if (rv<0) {
      u8_graberr(errno,"config_setuser",NULL);
      u8_log(LOG_CRIT,"Can't set user","Can't change user ID from %d",cur_uid);
      fd_seterr("CantSetUser","config_setuser",NULL,uid);
      return -1;}
    return 1;}
}
#else
static int config_setuser(fdtype var,fdtype val,void *fd_vecelts)
{
  u8_log(LOG_CRIT,"Can't set user","Can't change user ID in this OS");
  fd_seterr("SystemCantSetUser","config_setuser",NULL,val);
  return -1;
}
#endif

/* User config functions */

#if (HAVE_GETGID|HAVE_GETEGID)
static fdtype config_getgroup(fdtype var,void *data)
{
  gid_t gid=GETGIDFN(); int i=(int)gid;
  return FD_INT(i);
}
#else
static fdtype config_getgroup(fdtype var,void *fd_vecelts)
{
  return FD_FALSE;
}
#endif

#if (HAVE_SETGID|HAVE_SETEGID)
static int config_setgroup(fdtype var,fdtype val,void *data)
{
  gid_t cur_gid=GETGIDFN(); int gid=resolve_gid(val);
  if (gid<0) return -1;
  else if (cur_gid==gid) return 0;
  else {
    int rv=SETGIDFN(gid);
    if (rv<0) {
      u8_graberr(errno,"config_setgroup",NULL);
      u8_log(LOG_CRIT,"Can't set group","Can't change group ID from %d",cur_gid);
      fd_seterr("CantSetGroup","config_setgroup",NULL,gid);
      return -1;}
    return 1;}
}
#else
static int config_setgroup(fdtype var,fdtype val,void *fd_vecelts)
{
  u8_log(LOG_CRIT,"Can't set group","Can't change group ID in this OS");
  fd_seterr("SystemCantSetGroup","config_setgroup",NULL,val);
  return -1;
}
#endif

/* Initialization */

static int boot_config()
{
  u8_byte *config_string=(u8_byte *)u8_getenv("FD_BOOT_CONFIG");
  u8_byte *scan, *end; int count=0;
  if (config_string==NULL) config_string=u8_strdup(FD_BOOT_CONFIG);
  else config_string=u8_strdup(config_string);
  scan=config_string; end=strchr(scan,';');
  while (scan) {
    if (end==NULL) {
      fd_config_assignment(scan); count++;
      break;}
    *end='\0'; fd_config_assignment(scan); count++;
    scan=end+1; end=strchr(scan,';');}
  u8_free(config_string);
  return count;
}

/* Stack setup */

FD_EXPORT ssize_t fd_init_stack()
{
  if (fd_default_stack_limit > 0)
    fd_stack_limit_set( fd_default_stack_limit );
  return -1;
}

static int init_thread_stack_limit()
{
  fd_init_stack();
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

  if (!(logdir)) logdir=u8_strdup(FD_LOG_DIR);
  fd_register_config
    ("LOGDIR",_("Root FramerD logging directories"),
     fd_sconfig_get,fd_sconfig_set,&logdir);

  if (!(sharedir)) sharedir=u8_strdup(FD_SHARE_DIR);
  fd_register_config
    ("SHAREDIR",_("Shared config/data directory for FramerD"),
     fd_sconfig_get,fd_sconfig_set,&sharedir);

  if (!(datadir)) datadir=u8_strdup(FD_DATA_DIR);
  fd_register_config
    ("DATADIR",_("Data directory for FramerD"),
     fd_sconfig_get,fd_sconfig_set,&datadir);

  fd_register_config
    ("SOURCES",_("Registered source files"),
     config_get_source_files,config_add_source_file,
     &u8_log_show_procinfo);

  fd_register_config("ATEXIT",_("Procedures to call on exit"),
                     config_atexit_get,config_atexit_set,NULL);

  fd_register_config
    ("STACKLIMIT",_("Size of the stack (in bytes)"),
     fd_sizeconfig_get,fd_sizeconfig_set,&fd_default_stack_limit);

  u8_register_threadinit(init_thread_stack_limit);

}
