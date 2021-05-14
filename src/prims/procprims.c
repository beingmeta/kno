/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/numbers.h"
#include "kno/apply.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/frames.h"
#include "kno/eval.h"
#include "kno/ports.h"
#include "kno/fileprims.h"
#include "kno/procprims.h"
#include "kno/cprims.h"

#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8streamio.h>
#include "libu8/u8fileio.h"
#include <libu8/u8netfns.h>
#include <libu8/u8xfiles.h>
#include <libu8/u8rusage.h>

#include <stdlib.h>

#if HAVE_UNISTD_H
#include <unistd.h>
#endif

#if HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif

#if HAVE_SYS_WAIT_H
#include <sys/wait.h>
#endif

#include <ctype.h>

#if HAVE_SIGNAL_H
#include <signal.h>
#endif

#if (HAVE_SYS_RESOURCE_H)
#include <sys/resource.h>
#elif (HAVE_RESOURCE_H)
#include <resource.h>
#endif

#if HAVE_SYS_TIME_H
#include <sys/time.h>
#endif

#if ((HAVE_SYS_VFS_H)&&(HAVE_STATFS))
#include <sys/vfs.h>
#elif ((HAVE_SYS_FSTAT_H)&&(HAVE_STATFS))
#include <sys/statfs.h>

#endif

#define SUBPROCP(obj)   (KNO_TYPEP((obj),kno_subproc_type))
#define SUBPROC_PID(sp) (KNO_INT((sp->proc_pid)))

/* Defined in sysprims.c, used here */
void extract_rusage(lispval slotmap,
		    struct rusage *r,
		    struct rusage *reference);


static u8_condition RedirectFailed = "Redirect failed";
static u8_condition NotPID = "Not a process identifier (PID)";
static u8_condition ProcinfoError = "Error getting process info";

static struct KNO_HASHTABLE _proctable, *proctable = NULL;

static lispval id_symbol, stdin_symbol, stdout_symbol, stderr_symbol;

static lispval cons_subproc(pid_t pid,u8_string id,
			    char *cprogname,char **argv,char **envp,
			    lispval in,lispval out,lispval err,
			    lispval opts);
static int handle_procopts(lispval opts);

#define PIPE_FLAGS O_NONBLOCK
#define STDOUT_FILE_MODE (S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH)
#define STDERR_FILE_MODE (S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH)
#define PROC_EXEC_FLAGS KNO_DO_LOOKUP

static void free_stringvec(char **vec)
{
  if (vec) {
    char **scan = vec;
    while (*scan) {
      char *elt = *scan++;
      u8_free(elt);}
    u8_free(vec);}
}

static int stringvec_len(char **vec)
{
  if (vec==NULL) return 0;
  char **scan = vec;
  while (*scan) scan++;
  return scan-vec;
}

#define ARG_ESCAPE_SCHEME 1
#define ARG_ESCAPE_CONFIGS 2

static int needs_scheme_escapep(u8_string s,int escape_flags)
{
  if ( (escape_flags&ARG_ESCAPE_CONFIGS) && (strchr(s,'=')) )
    return 1;
  else if (escape_flags&ARG_ESCAPE_SCHEME)
    return ((strchr("@{#(\"",s[0])) || (isdigit(s[0])));
  else return 0;
}

#if ! HAVE_EXECVPE
extern char *const *environ;
static int execvpe(char *prog,char *const argv[],char *const envp[])
{
  environ = envp;
  return execvp(prog,(char **)argv);
}
#endif

DEF_KNOSYM(exited); DEF_KNOSYM(terminated); DEF_KNOSYM(stopped);
DEF_KNOSYM(outdir); DEF_KNOSYM(chdir);
DEF_KNOSYM(wait); DEF_KNOSYM(pid); DEF_KNOSYM(block);
DEF_KNOSYM(fork); DEF_KNOSYM(lookup); DEF_KNOSYM(knox);
DEF_KNOSYM(interpreter); DEF_KNOSYM(environment); DEF_KNOSYM(configs);
DEF_KNOSYM(procid); DEF_KNOSYM(progname); DEF_KNOSYM(started);
DEF_KNOSYM(finished); DEF_KNOSYM(runtime); DEF_KNOSYM(keepenv);
DEF_KNOSYM(keepconfigs); DEF_KNOSYM(file); DEF_KNOSYM(temp);
DEF_KNOSYM(onfinish); DEF_KNOSYM(deleted);

static int remove_output(u8_string file,u8_string kind)
{
  int rv = u8_removefile(file);
  if (rv<0) {
    int err = errno; errno=0;
    u8_log(LOGERR,"RemoveFailed",
	   "Couldn't remove (%s) %s file '%s'",
	   u8_strerror(err),kind,file);
    return 0;}
  else return 1;
}

void extract_pid_status(int status,lispval into)
{
  if (WIFEXITED(status)) {
    int details = WEXITSTATUS(status);
    kno_store(into,KNOSYM(exited),KNO_INT(details));}
  if (WIFSIGNALED(status)) {
    int details = WTERMSIG(status);
    kno_store(into,KNOSYM(terminated),KNO_INT(details));}
  if (WIFSTOPPED(status)) {
    int details = WSTOPSIG(status);
    kno_store(into,KNOSYM(stopped),KNO_INT(details));}
  if (WIFCONTINUED(status))
    kno_store(into,KNOSYM(stopped),KNO_INT(SIGCONT));
}

static void finish_subproc(struct KNO_SUBPROC *p)
{
  if (p->proc_finished>0) return;
  lispval pidval = KNO_INT((long long)(p->proc_pid));
  int rv = kno_hashtable_drop(proctable,pidval,KNO_VOID);
  if (rv) { /* if rv!=0, it was already deleted */ 
    p->proc_finished = u8_elapsed_time();
    int status = p->proc_status;
    if ( (WIFEXITED(status)) && (WEXITSTATUS(status)==0) ) {
      lispval opts = p->proc_opts;
      lispval out = p->proc_stdout, err = p->proc_stderr;
      u8_string outfilename = NULL, errfilename = NULL;
      if (KNO_STRINGP(out)) {
	if (kno_testopt(opts,KNOSYM(stdout),KNOSYM(temp))) {
	  if (remove_output(KNO_CSTRING(out),"stdout")) {
	    p->proc_stdout=KNOSYM(deleted);
	    kno_decref(out);}}
	else outfilename = KNO_CSTRING(out);}
      if (KNO_STRINGP(err)) {
	if (kno_testopt(opts,KNOSYM(stderr),KNOSYM(temp))) {
	  if (remove_output(KNO_CSTRING(err),"stderr")) {
	    p->proc_stderr=KNOSYM(deleted);
	    kno_decref(err);}}
	else errfilename = KNO_CSTRING(err);}
      int loglevel  = ( (outfilename) || (errfilename) ) ? (LOG_WARN) :
	(LOG_NOTICE);
      u8_log(loglevel,"SubprocFinished","PID=%lld %s%s%s%s%s",
	     (long long)(p->proc_pid),p->proc_id,
	     ((outfilename)?("\n\tsaved stdout="):("")),
	     ((outfilename)?(outfilename):(U8S(""))),
	     ((errfilename)?("\n\tsaved stderr="):("")),
	     ((errfilename)?(errfilename):(U8S(""))));}
    lispval onfinish = kno_get(p->annotations,KNOSYM(onfinish),KNO_VOID);
    if (!(KNO_VOIDP(onfinish))) {
      lispval sj = (lispval)p;
      DO_CHOICES(handler,onfinish) {
	lispval v = kno_call(NULL,handler,1,&sj);
	if (KNO_ABORTED(v)) {kno_clear_errors(1);}
	else kno_decref(v);}
      kno_decref(onfinish);}}
}

static int check_finished(struct KNO_SUBPROC *p)
{
  pid_t pid = p->proc_pid;
  if (p->proc_finished>=0) return 1;
  else if (pid<0) {
    p->proc_finished = u8_elapsed_time();
    return 1;}
  else {
    int status = 0;
    int rv = waitpid(pid,&status,WNOHANG|WUNTRACED|WCONTINUED);
    if (rv<0) {
      int saved_errno = errno; errno = 0;
      u8_log(LOGERR,"FailedWait",
	     "waitpid for %lld unexpectedly failed with errno %d (%s)",
	     (long long)pid,saved_errno,u8_strerror(saved_errno));}
    else p->proc_status = status;
    if (rv<0) return 0;
    else if (rv!=pid) return 0;
    else if (!( (WIFEXITED(status)) || (WIFSIGNALED(status)) ))
      return 0;
    finish_subproc(p);
    return 1;}
}

/* PID functions */

DEFC_PRIM("pid?",ispid_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns #t if it's argument is a current and "
	  "valid process ID.",
	  {"pid_arg",kno_fixnum_type,KNO_VOID})
static lispval ispid_prim(lispval pid_arg)
{
  pid_t pid = FIX2INT(pid_arg);
  int rv = kill(pid,0);
  if (rv<0) {
    errno = 0; return KNO_FALSE;}
  else return KNO_TRUE;
}

DEFC_PRIM("pid/kill!",pid_kill_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "sends *signal* (default is ) to *process*.",
	  {"pid_arg",kno_any_type,KNO_VOID},
	  {"sig_arg",kno_any_type,KNO_VOID})
static lispval pid_kill_prim(lispval pid_arg,lispval sig_arg)
{
  pid_t pid = FIX2INT(pid_arg);
  int sig = FIX2INT(sig_arg);
  if ((sig>0)&&(sig<256)) {
    int rv = kill(pid,sig);
    if (rv<0) {
      char buf[128]; sprintf(buf,"pid=%ld;sig=%d",(long int)pid,sig);
      u8_graberrno("os_kill_prim",u8_strdup(buf));
      return KNO_ERROR;}
    else return KNO_TRUE;}
  else return kno_type_error("signal","pid_kill_prim",sig_arg);
}

/* Generic rlimits */

lispval kno_rlimit_codes = KNO_EMPTY;

DEFC_PRIM("getrlimit",getrlimit_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "gets the *resource* resource limit for the "
	  "current process. If *getmax* is true, gets the "
	  "maximum resource limit.",
	  {"resname",kno_symbol_type,KNO_VOID},
	  {"which",kno_any_type,KNO_VOID})
static lispval getrlimit_prim(lispval resname,lispval which)
{
  struct rlimit lim;
  long long resnum = -1;
  lispval rescode = kno_get(kno_rlimit_codes,resname,KNO_VOID);
  if (KNO_UINTP(rescode))
    resnum = KNO_FIX2INT(rescode);
  else return kno_err(_("BadRLIMITKey"),"setrlimit_prim",NULL,resname);
  int rv = getrlimit(resnum,&lim);
  if (rv < 0) {
    u8_string errstring = u8_strerror(errno); errno=0;
    return kno_err("RLimitFailed","getrlimit_prim",errstring,resname);}
  if (KNO_TRUEP(which)) {
    long long curlim = (long long) lim.rlim_cur;
    long long maxlim = (long long) lim.rlim_max;
    return kno_init_pair(NULL,
			 ( (curlim<0) ? (KNO_FALSE) : (KNO_INT(curlim)) ),
			 ( (maxlim<0) ? (KNO_FALSE) : (KNO_INT(maxlim)) ));}
  else {
    long long curlim = (long long) lim.rlim_cur;
    if (curlim < 0)
      return KNO_FALSE;
    else return KNO_INT(curlim);}
}

DEFC_PRIM("setrlimit!",setrlimit_prim,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "sets the resource limit *resource* (symbol) to "
	  "*value* for the current process. If *setmax* is "
	  "true, sets the maximium resource value if allowed.",
	  {"resname",kno_symbol_type,KNO_VOID},
	  {"limit_val",kno_any_type,KNO_VOID},
	  {"setmax_arg",kno_any_type,KNO_VOID})
static lispval setrlimit_prim(lispval resname,lispval limit_val,
			      lispval setmax_arg)
{

  struct rlimit lim;
  long long resnum = -1;
  long long resval = -1;
  lispval rescode = kno_get(kno_rlimit_codes,resname,KNO_VOID);
  int setmax = (KNO_TRUEP(setmax_arg));
  if (KNO_UINTP(rescode))
    resnum = KNO_FIX2INT(rescode);
  else return kno_err(_("BadRLIMITKey"),"setrlimit_prim",NULL,resname);
  if (KNO_UINTP(limit_val))
    resval = KNO_FIX2INT(limit_val);
  else if ( (KNO_FIXNUMP(limit_val)) || (KNO_FALSEP(limit_val)) )
    resval = RLIM_INFINITY;
  else return kno_err(kno_TypeError,"setrlimit_prim",NULL,limit_val);
  int rv = getrlimit(KNO_FIX2INT(rescode),&lim);
  if (rv < 0) {
    u8_string errstring = u8_strerror(errno); errno=0;
    return kno_err("RLimitFailed","setrlimit_prim/getrlimit",errstring,resname);}
  if (setmax)
    lim.rlim_max = resval;
  else lim.rlim_cur = resval;
  rv = setrlimit(resnum,&lim);
  if (rv < 0) {
    u8_string errstring = u8_strerror(errno); errno=0;
    return kno_err(_("SetRLIMITFailed"),"setrlimit_prim",errstring,resname);}
  return KNO_TRUE;
}

/* RLIMIT tables */

#define DECL_RLIMIT(name)					\
  kno_store(kno_rlimit_codes,kno_intern(# name),KNO_INT(name))
#define DECL_RLIMIT_ALIAS(alias,code)				\
  kno_store(kno_rlimit_codes,kno_intern(alias),KNO_INT(code))

static void init_rlimit_codes()
{
  kno_rlimit_codes = kno_empty_slotmap();
  DECL_RLIMIT(RLIMIT_AS);
  DECL_RLIMIT_ALIAS("VMEM",RLIMIT_AS);
  DECL_RLIMIT(RLIMIT_CORE);
  DECL_RLIMIT_ALIAS("COREFILE",RLIMIT_CORE);
  DECL_RLIMIT(RLIMIT_CPU);
  DECL_RLIMIT_ALIAS("CPU",RLIMIT_CPU);
  DECL_RLIMIT(RLIMIT_DATA);
  DECL_RLIMIT_ALIAS("HEAP",RLIMIT_DATA);
  DECL_RLIMIT(RLIMIT_FSIZE);
  DECL_RLIMIT_ALIAS("FILESIZE",RLIMIT_FSIZE);
#ifdef RLIMIT_LOCKS
  DECL_RLIMIT(RLIMIT_LOCKS);
#endif
#ifdef RLIMIT_MEMLOCK
  DECL_RLIMIT(RLIMIT_MEMLOCK);
#endif
#ifdef RLIMIT_MSGQUEUE
  DECL_RLIMIT(RLIMIT_MSGQUEUE);
#endif
#ifdef RLIMIT_NICE
  DECL_RLIMIT(RLIMIT_NICE);
#endif
  DECL_RLIMIT(RLIMIT_NOFILE);
  DECL_RLIMIT_ALIAS("MAXFILES",RLIMIT_NOFILE);
  DECL_RLIMIT(RLIMIT_NPROC);
  DECL_RLIMIT_ALIAS("MAXPROCS",RLIMIT_NPROC);
  DECL_RLIMIT(RLIMIT_RSS);
  DECL_RLIMIT_ALIAS("RESIDENT",RLIMIT_RSS);
#ifdef RLIMIT_RTPRIO
  DECL_RLIMIT(RLIMIT_RTPRIO);
#endif
#ifdef RLIMIT_RTIME
  DECL_RLIMIT(RLIMIT_RTIME);
#endif
#ifdef RLIMIT_SIGPENDING
  DECL_RLIMIT(RLIMIT_SIGPENDING);
#endif
  DECL_RLIMIT(RLIMIT_STACK);
}

/* The nice prim */

static lispval nice_symbol = KNO_VOID;

DEFC_PRIM("nice",nice_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	  "Returns or adjusts the priority for the current process",
	  {"delta_arg",kno_fixnum_type,KNO_VOID})
static lispval nice_prim(lispval delta_arg)
{
  int delta = (!(KNO_FIXNUMP(delta_arg))) ? (0) : (KNO_FIX2INT(delta_arg));
  errno = 0;
  int rv = nice(delta);
  if (errno) {
    u8_string errstring = u8_strerror(errno); errno=0;
    return kno_err("NiceFailed","nice_prim",errstring,delta);}
  else return KNO_INT(rv);
}

/* THe core exec routine */

#define KNO_IS_SCHEME 0x01
#define KNO_NO_FORK 0x02
#define KNO_DO_WAIT 0x04
#define KNO_DO_LOOKUP 0x08
#define KNO_DONT_BLOCK 0x10

static int dodup(int from,int to,u8_string stream,u8_string id);
static int setup_pipe(int fds[2]);
static int handle_procopts(lispval opts);

static pid_t doexec(int flags,char *progname,char *cwd,
		    int *in,int *out,int *err,
		    char *const argv[],char *const envp[],
		    lispval procopts)
{
  int dolookup = flags&(KNO_DO_LOOKUP);
  int doblock = (!(flags&(KNO_DONT_BLOCK)));
  /* We're in the proc now */
  int rv = handle_procopts(procopts);
  /* Change directory if specified */
  if (rv>=0) rv = (cwd) ? (chdir(cwd)) : (0);
  /* Close the sockets the parent is using */
  /* pair[0] is the read end, pair[1] is the write end */
  if (in) {
    if (in[1]>0) close(in[1]);
    if ((doblock) && (in[0]>0)) u8_set_blocking(in[0],1);}
  if (out) {
    if (out[0]>0) close(out[0]);
    if ((doblock) && (out[1]>0)) u8_set_blocking(out[1],1);}
  if (err) {
    if (err[0]>0) close(err[0]);
    /* Setup err[1] here */}
  if ( (rv>=0) && (in) && (in[0]>=0) )
    rv = dodup(in[0],STDIN_FILENO,"stdin",progname);
  if ( (rv>=0) && (out) && ( (rv>=0) && (out[1]>0) ) )
    rv = dodup(out[1],STDOUT_FILENO,"stdout",progname);
  if ( (rv>=0) && (err) && ( (rv>=0) && (err[1]>0) ) )
    rv = dodup(err[1],STDERR_FILENO,"stderr",progname);
  if (rv<0) {
    int saved_errno = errno; errno=0;
    u8_log(LOGCRIT,"ForkExecFailed","errno=%d (%s)",
	   saved_errno,u8_strerror(saved_errno));
    exit(1);}
  else if ( (dolookup) && (envp) )
    rv = execvpe(progname,argv,envp);
  else if (dolookup)
    rv = execvp(progname,argv);
  else if (envp)
    rv = execve(progname,argv,envp);
  else rv = execv(progname,argv);
  return rv;
}

#if HAVE_PIPE2
static int setup_pipe(int fds[2])
{
  return pipe2(fds,PIPE_FLAGS);
}
#else
static int setup_pipe(int fds[2])
{
  int rv = pipe(fds);
  return rv;
}
#endif

static int dodup(int from,int to,u8_string stream,u8_string id)
{
  int rv = dup2(from,to);
  if (rv<0) {
    int err = errno; errno=0;
    u8_log(LOGCRIT,RedirectFailed,
	   "Couldn't redirect %s for job %s (%d:%s)",
	   stream,id,err,u8_strerror(err));}
  close(from);
  return rv;
}

static int handle_procopts(lispval opts)
{
  if ( (KNO_FALSEP(opts)) || (KNO_VOIDP(opts)) ) return 0;
  lispval limit_keys = kno_getkeys(kno_rlimit_codes);
  DO_CHOICES(opt,limit_keys) {
    lispval optval = kno_getopt(opts,opt,KNO_VOID);
    if (KNO_VOIDP(optval)) continue;
    else {
      lispval setval = setrlimit_prim(opt,optval,KNO_FALSE);
      if (KNO_ABORTP(setval)) {
	u8_log(LOGWARN,"RlimitFailed","Couldn't set %q",opt);
	kno_clear_errors(1);}
      else u8_log(LOGNOTICE,"RlimitChanged","Set %q to %q",opt,optval);
      kno_decref(optval);}}
  kno_decref(limit_keys);
  lispval niceval = kno_getopt(opts,nice_symbol,KNO_VOID);
  if (KNO_FIXNUMP(niceval)) {
    int nv = KNO_FIX2INT(niceval); errno = 0;
    int rv = nice(nv);
    if (errno) {
      u8_string errstring = u8_strerror(errno); errno=0;
      u8_log(LOGCRIT,"ReNiceFailed","With %s, setting to %d",
	     errstring,KNO_FIX2INT(niceval));}
    else u8_log(LOGNOTICE,"Renice","To %d for %d",rv,KNO_FIX2INT(niceval));
    return 0;}
  else if (!(KNO_VOIDP(niceval))) {
    u8_log(LOGWARN,"BadNiceValue","handle_procopts %q",niceval);
    return -1;}
  else return 0;
}

/* Run */

static void handle_config_spec(kno_stackvec c,lispval spec,lispval keep);
static void handle_env_spec(kno_stackvec e,lispval spec,lispval keep);
static int handle_arg_spec(kno_stackvec a,lispval arg,int flags);
static void add_configs(kno_stackvec configs,lispval value,u8_string name);
static u8_string mkidstring(u8_string progname,int n,kno_argvec args);
static lispval makeout(int fd,u8_string id);
static lispval makein(int fd,u8_string id);
static u8_string get_outdir(lispval opts);

DEFC_PRIMNx("proc/open",proc_open_prim,
	    KNO_VAR_ARGS|KNO_MIN_ARGS(1),
	    "invokes *progname*, usually by forking a new child "
	    "process, and passes the remaining arguments to "
	    "the program. If the *opts* arg is a string, it is "
	    "taken as a first argument (instead of specifying options)",
	    2,
	    {"progname",kno_string_type,KNO_VOID},
	    {"opts",kno_any_type,KNO_VOID})
static lispval proc_open_prim(int n,kno_argvec args)
{
  u8_string progname = NULL;
  int args_start = 2, flags = 0, exec_mod = 0;
  lispval command = args[0], opts = (n>1) ? (args[1]) : (KNO_FALSE), result=VOID;
  if (KNO_STRINGP(command))
    progname=KNO_CSTRING(command);
  else if (KNO_SYMBOLP(command)) {
    if (strchr(KNO_SYMBOL_NAME(command),'/')) {
      lispval mod = kno_find_module(command,0);
      if (!(KNO_VOIDP(mod))) {
	exec_mod = 1;
	progname=KNO_SYMBOL_NAME(command);
	kno_decref(mod);}}
    if (!exec_mod) {
      lispval config_val = kno_config_get(KNO_SYMBOL_NAME(command));
      if (KNO_STRINGP(config_val))
	progname=KNO_CSTRING(config_val);
      else progname=KNO_SYMBOL_NAME(command);
      kno_decref(config_val);}}
  else return kno_err("BadCommand","runproc_prim",NULL,command);

  /* Handle the case with no options */
  if ( (KNO_STRINGP(opts)) || (KNO_NUMBERP(opts)) ) {
    opts = KNO_FALSE; args_start = 1;}
  else if (kno_testopt(opts,KNOSYM(fork),KNO_FALSE))
    flags |= KNO_NO_FORK;
  else NO_ELSE;

  if (kno_testopt(opts,KNOSYM(block),KNO_FALSE))
    flags |= KNO_DONT_BLOCK;

  if ( (kno_testopt(opts,KNOSYM(lookup),KNO_VOID)) ||
       (!(u8_file_existsp(progname))) )
    flags |= KNO_DO_LOOKUP;

  u8_string idstring = NULL;
  {lispval idopt = kno_getopt(opts,id_symbol,KNO_VOID);
    if (STRINGP(idopt)) idstring=u8_strdup(CSTRING(idopt));
    kno_decref(idopt);}
  if (idstring==NULL)
    idstring=mkidstring(progname,n-args_start,args+args_start);

  lispval knox_opt = kno_getopt(opts,KNOSYM(knox),KNO_VOID);
  lispval interpreter_opt = kno_getopt(opts,KNOSYM(interpreter),KNO_VOID);
  u8_string interpreter =
    ( (!(exec_mod)) && (KNO_FALSEP(interpreter_opt)) ) ? (NULL) :
    (KNO_STRINGP(interpreter_opt)) ? (KNO_CSTRING(interpreter_opt)) :
    (KNO_STRINGP(knox_opt)) ? (KNO_CSTRING(knox_opt)) :
    ((!(KNO_VOIDP(knox_opt))) && (!(KNO_FALSEP(knox_opt)))) ? (U8S(KNO_EXEC)) :
    (KNO_VOIDP(interpreter_opt)) ?
    ((u8_has_suffix(progname,".scm",1)) ? (U8S(KNO_EXEC)) : (NULL)) :
    (U8S(KNO_EXEC));
  int kno_exec = (exec_mod) ? (exec_mod) :
    (KNO_FALSEP(knox_opt)) ? (0) :
    (!(KNO_VOIDP(knox_opt))) ? (1) :
    ( (interpreter) && (strcmp(interpreter,KNO_EXEC)==0) );

  if (kno_exec) flags |= KNO_IS_SCHEME;

  KNO_DECL_STACKVEC(exec_args,32);
  KNO_DECL_STACKVEC(environment,32);
  KNO_DECL_STACKVEC(configs,32);
  char *cprogname = NULL;

  if (interpreter) {
    kno_stackvec_push(exec_args,knostring(interpreter));
    cprogname = u8_tolibc(interpreter);}
  else cprogname = u8_tolibc(progname);
  kno_stackvec_push(exec_args,knostring(progname));

  kno_argvec scan = args+args_start, limit = args+n;
  while (scan<limit) {
    lispval arg = *scan++;
    int arg_flags = (kno_exec) ? (ARG_ESCAPE_SCHEME|ARG_ESCAPE_CONFIGS) :
      (0);
    int rv = handle_arg_spec(exec_args,arg,arg_flags);
    if (rv<0) {
      kno_free_stackvec(exec_args);
      if (cprogname) u8_free(cprogname);
      return KNO_ERROR;}}

  lispval env_spec = kno_getopt(opts,KNOSYM(environment),KNO_VOID);
  lispval keep_env = kno_getopt(opts,KNOSYM(keepenv),KNO_VOID);
  if ( (!(KNO_VOIDP(env_spec))) || (!(KNO_VOIDP(keep_env))) )
    handle_env_spec(environment,env_spec,keep_env);

  lispval config_spec = kno_getopt(opts,KNOSYM(configs),VOID);
  if ( (!(KNO_VOIDP(config_spec))) && (kno_exec) ) {
    lispval keep_configs = kno_getopt(opts,KNOSYM(keepconfigs),KNO_VOID);
    if ( (!(KNO_VOIDP(config_spec))) || (!(KNO_VOIDP(keep_configs))) )
      handle_config_spec(configs,config_spec,keep_configs);
    else NO_ELSE;
    kno_decref(keep_configs);}
  else if (!(KNO_VOIDP(config_spec)))
    u8_log(LOGWARN,"IgnoredConfigs",
	   "Ignoring the provided configs because the program "
	   "is not a KNO application.");
  else NO_ELSE;

  int use_argc = ((interpreter==NULL)?(0):(1)) +
    exec_args->count + configs->count + 1;
  char **argv = u8_alloc_n(use_argc,char *);
  int arg_write = 0;

  lispval *scan_args = exec_args->elts;
  int arg_i = 0, arg_max = exec_args->count;
  while (arg_i<arg_max) {
    lispval arg = scan_args[arg_i++];
    if (KNO_STRINGP(arg))
      argv[arg_write++]=u8_tolibc(CSTRING(arg));}

  lispval *config_args = configs->elts;
  int config_i = 0, config_max = configs->count;
  while (config_i<config_max) {
    lispval arg = config_args[config_i++];
    if (KNO_STRINGP(arg))
      argv[arg_write++]=u8_tolibc(CSTRING(arg));}
  argv[arg_write++]=NULL;

  int use_envc = environment->count+1;
  char **envp = (use_envc) ? (u8_alloc_n(use_envc,char *)) : (NULL);
  int env_i=0, env_max = environment->count;
  lispval *elts = environment->elts;
  while (env_i<env_max) {
    lispval spec = elts[env_i];
    envp[env_i]=u8_tolibc(CSTRING(spec));
    env_i++;}
  envp[env_i++]=NULL;

  lispval dir_opt = kno_getopt(opts,KNOSYM(chdir),KNO_EMPTY);
  char *cwd = (KNO_STRINGP(dir_opt)) ? (u8_tolibc(KNO_CSTRING(dir_opt))) :
    (NULL);

  /* We have these default to empty because we may store
     them in the record and we don't want to store VOID values. */
  lispval infile   = kno_getopt(opts,stdin_symbol,KNO_EMPTY);
  lispval outfile  = kno_getopt(opts,stdout_symbol,KNO_EMPTY);
  lispval errfile  = kno_getopt(opts,stderr_symbol,KNO_EMPTY);
  int create_outfile = 0, create_errfile = 0;
  int *in=NULL, *out=NULL, *err=NULL;
  int inpipe[2]={-1,-1}, outpipe[2]={-1,-1}, errpipe[2]={-1,-1};
  u8_string out_dir = get_outdir(opts);

  if (infile == stdin_symbol) {}
  else if (KNO_STRINGP(infile)) {
    u8_string use_file = u8_abspath(KNO_CSTRING(infile),NULL);
    kno_decref(infile); infile = kno_init_string(NULL,-1,use_file);
    int new_stdin = u8_open_fd(use_file,O_RDONLY,0);
    if (new_stdin<0) {
      u8_exception ex = u8_current_exception;
      u8_log(LOGCRIT,RedirectFailed,
	     "Couldn't open %s as stdin (%s:%s)",
	     KNO_CSTRING(infile),ex->u8x_cond,ex->u8x_details);
      result=KNO_ERROR;
      goto do_exit;}
    else {
      inpipe[0]=new_stdin;
      in=inpipe;}}
  else {
    if (setup_pipe(inpipe)<0) {}
    else in = inpipe;}

  if (outfile == stdout_symbol) {}
  else if (KNO_STRINGP(outfile)) {
    u8_string use_file = u8_abspath(KNO_CSTRING(outfile),out_dir);
    kno_decref(outfile); outfile = kno_init_string(NULL,-1,use_file);
    int new_stdout = u8_open_fd(use_file,O_WRONLY|O_CREAT,STDOUT_FILE_MODE);
    if (new_stdout<0) {
      u8_exception ex = u8_current_exception;
      u8_log(LOGCRIT,"Couldn't open %s as stdout (%s:%s)",
	     KNO_CSTRING(outfile),ex->u8x_cond,ex->u8x_details);
      result=KNO_ERROR;
      goto do_exit;}
    else {
      outpipe[1]=new_stdout;
      out=outpipe;}}
  else if ( (outfile == KNOSYM(temp)) ||
	    (outfile == KNOSYM(file)) )
    create_outfile = 1;
  else {
    if (setup_pipe(outpipe)<0) {}
    else out = outpipe;}

  if ( (KNO_EMPTYP(errfile)) && (!(KNO_EMPTYP(outfile))) ) {
    errpipe[1]=outpipe[1];
    err=errpipe;}
  else if (KNO_EMPTYP(errfile)) {}
  else if (errfile == stderr_symbol) {}
  else if (KNO_STRINGP(errfile)) {
    u8_string use_file = u8_abspath(KNO_CSTRING(errfile),out_dir);
    kno_decref(errfile); errfile = kno_init_string(NULL,-1,use_file);
    int new_stderr = u8_open_fd(use_file,O_WRONLY|O_CREAT,STDERR_FILE_MODE);
    if (new_stderr<0) {
      u8_exception ex = u8_current_exception;
      u8_log(LOGCRIT,"Couldn't open %s as stderr (%s:%s)",
	     KNO_CSTRING(errfile),
	     ex->u8x_cond,ex->u8x_details);
      result=KNO_ERROR;
      goto do_exit;}
    else {
      errpipe[1]=new_stderr;
      err=errpipe;}}
  else if ( (errfile == KNOSYM(temp)) ||
	    (errfile == KNOSYM(file)) )
    create_errfile = 1;
  else {
    if (setup_pipe(errpipe)<0) {}
    else out = errpipe;}

  lispval wait_opt = kno_getopt(opts,KNOSYM(wait),KNO_FALSE);
  int nofork = flags&(KNO_NO_FORK);
  pid_t pid = (nofork) ? (0) : (fork());

  /* Once we have a PID, we can generate stdout/stderr files with the
     PID in the filename. */
  pid_t child_pid = (pid == 0) ? (getpid()) : (pid);
  if (create_outfile) {
    u8_string name =
      u8_mkstring("%s_%lld.stdout",progname,(long long)child_pid);
    u8_string use_file = (out_dir) ? (u8_mkpath(out_dir,name)) :
      (u8_abspath(name,NULL));
    int new_stdout = u8_open_fd(use_file,O_WRONLY|O_CREAT,STDOUT_FILE_MODE);
    if (new_stdout<0) {
      u8_exception ex = u8_current_exception;
      u8_log(LOGCRIT,"Couldn't open %s as stdout (%s:%s)",
	     KNO_CSTRING(errfile),
	     ex->u8x_cond,ex->u8x_details);
      result=KNO_ERROR;
      goto do_exit;}
    else if (pid)
      outfile=kno_wrapstring(use_file);
    else {
      outpipe[1]=new_stdout;
      out=outpipe;}}
  if (create_errfile) {
    u8_string name =
      u8_mkstring("%s_%lld.stderr",progname,(long long)child_pid);
    u8_string use_file = (out_dir) ? (u8_mkpath(out_dir,name)) :
      (u8_abspath(name,NULL));
    int new_stderr = u8_open_fd(use_file,O_WRONLY|O_CREAT,STDERR_FILE_MODE);
    if (new_stderr<0) {
      u8_exception ex = u8_current_exception;
      u8_log(LOGCRIT,"Couldn't open %s as stderr (%s:%s)",
	     KNO_CSTRING(errfile),
	     ex->u8x_cond,ex->u8x_details);
      result=KNO_ERROR;
      goto do_exit;}
    else if (pid)
      errfile=kno_wrapstring(use_file);
    else {
      errpipe[1]=new_stderr;
      err=errpipe;}}

  if (pid == 0) {
    doexec(flags,cprogname,cwd,in,out,err,argv,envp,opts);
    result=kno_seterr("ExecFailed","proc_open",progname,VOID);}
  else if (pid>0) {
    result = cons_subproc
      (pid,idstring,cprogname,argv,envp,
       (((in==NULL) || (in[1]<0)) ? (kno_incref(infile)) :
	(makeout(in[1],u8_mkstring("(stdin)%s",idstring)))),
       (((out==NULL) || (out[0]<0)) ? (kno_incref(outfile)) :
	(makein(out[0],u8_mkstring("(stdout)%s",idstring)))),
       (((err==NULL) || (err[0]<0)) ? (kno_incref(errfile)) :
	(makein(err[0],u8_mkstring("(stderr)%s",idstring)))),
       kno_incref(opts));
    struct KNO_SUBPROC *subproc = (kno_subproc) result;
    if (!(KNO_FALSEP(wait_opt))) {
      int status = -1;
      int rv = waitpid(pid,&status,0);
      if (rv>=0) subproc->proc_status = status;
      if (rv<0) {
	lispval err = kno_err("WaitFailed","proc_open",progname,result);
	kno_decref(result);
	result=err;}
      else if ( (WIFEXITED(status)) || (WIFSIGNALED(status)) )
	finish_subproc(subproc);
      else NO_ELSE;}
    errno = 0;}
  else {
    int err = errno; errno=0;
    result = kno_err(u8_strerror(err),"proc_open/wait",progname,KNO_VOID);}

  /* Clean up any sockets we created */
  /* pair[0] is the read end, pair[1] is the write end */
  if (in) {
    /* Child's stdin, we use the write end [1] and close the read end [0]. */
    if (in[1]>=0) {
      if (KNO_ABORTED(result)) close(in[1]);
      else {u8_set_blocking(in[1],1);}}
    if (in[0]>=0) close(in[0]);}
  if (out) {
    /* Child's stdout, we use the read end [0] and close the write end [1]. */
    if (out[0]>=0) {
      if (KNO_ABORTED(result)) close(out[0]);
      else u8_set_blocking(out[0],1);}
    if (out[1]>=0) close(out[1]);}
  if (err) {
    /* Child's stderr, we use the read end [0] and close the write end [1]. */
    if (err[0]>=0) {
      if (KNO_ABORTED(result)) close(err[0]);
      else u8_set_blocking(err[0],1);}
    if (err[1]>=0) close(err[1]);}

 do_exit:
  if (KNO_ABORTED(result)) {
    if (cprogname) u8_free(cprogname);}
  if (out_dir) u8_free(out_dir);
  /* free_stringvec(argv); free_stringvec(envp); */
  kno_decref(infile);
  kno_decref(outfile);
  kno_decref(errfile);
  kno_decref(dir_opt);
  kno_decref(knox_opt);
  kno_decref(interpreter_opt);
  kno_decref(config_spec);
  kno_decref(env_spec);
  kno_decref(keep_env);
  kno_free_stackvec(environment);
  kno_free_stackvec(configs);
  return result;
}

static lispval makeout(int fd,u8_string id)
{
  u8_output out = (u8_output) u8_open_xoutput(fd,NULL);
  out->u8_streaminfo |= U8_STREAM_OWNS_SOCKET;
  return kno_make_port(NULL,out,id);
}
static lispval makein(int fd,u8_string id)
{
  u8_input in = (u8_input) u8_open_xinput(fd,NULL);
  in->u8_streaminfo |= U8_STREAM_OWNS_SOCKET;
  return kno_make_port(in,NULL,id);
}

static u8_string get_outdir(lispval opts)
{
  lispval outdir_opt = kno_getopt(opts,KNOSYM(outdir),KNO_VOID);
  lispval outdir_conf = KNO_VOID;
  u8_string outdir = NULL;
  if (KNO_STRINGP(outdir_opt)) {
    outdir = KNO_CSTRING(outdir_opt);
    goto cleanup;}
  else if (KNO_SYMBOLP(outdir_opt)) {
    lispval outdir_conf = kno_config_get(KNO_SYMBOL_NAME(outdir_opt));
    if (KNO_STRINGP(outdir_conf)) {
      outdir = KNO_CSTRING(outdir_conf);
      goto cleanup;}
    else if (KNO_UNSPECIFIEDP(outdir_conf)) {}
    else u8_log(LOGWARN,"BadOutdir",
		"Value of OUTDIR for proc/run config option was %q",
		outdir_conf);}
  else if (KNO_UNSPECIFIEDP(outdir_opt)) {}
  else u8_log(LOGWARN,"BadOutdir","Outdir option (for proc/run) was %q",
	      outdir_opt);

  if (outdir == NULL) {}
  else if (!(u8_directoryp(outdir))) {
    u8_log(LOGERR,"BadOutputDirectory",
	   "The path '%s' is not a directory",outdir);
    outdir = NULL;}
  else if (!(u8_file_writablep(outdir))) {
    u8_log(LOGERR,"BadOutputDirectory",
	   "The directory '%s' is not writable",outdir);
    outdir = NULL; }
  else NO_ELSE;

 cleanup:
  if (outdir) outdir = u8_strdup(outdir);
  kno_decref(outdir_conf);
  kno_decref(outdir_opt);
  return outdir;
}

static int handle_arg_spec(kno_stackvec args,lispval spec,
			   int escape_flags)
{
  if (KNO_EMPTY_LISTP(spec))
    return 0;
  else if (KNO_STRINGP(spec)) {
    u8_string s = CSTRING(spec);
    if ( (escape_flags) && (needs_scheme_escapep(s,escape_flags)) ) {
      U8_STATIC_OUTPUT(arg,100);
      u8_putc(argout,'\\');
      u8_puts(argout,KNO_CSTRING(spec));
      kno_stackvec_push
	(args,kno_make_string(NULL,u8_outlen(argout),u8_outstring(argout)));
      u8_close_output(argout);
      return 1;}
    else {
      kno_incref(spec);
      kno_stackvec_push(args,spec);
      return 1;}}
  else if (KNO_PACKETP(spec)) {
    kno_incref(spec);
    kno_stackvec_push(args,spec);
    return 1;}
  else if (KNO_VECTORP(spec)) {
    lispval *elts = KNO_VECTOR_ELTS(spec), *scan=elts;
    lispval *limit = scan + KNO_VECTOR_LENGTH(spec);
    while (scan<limit) {
      lispval elt = *scan++;
      int rv = handle_arg_spec(args,elt,escape_flags);
      if (rv<0) return rv;}
    return limit-elts;}
  else if (KNO_PAIRP(spec)) {
    lispval scan = spec; int count = 0;
    while (PAIRP(scan)) {
      int rv = handle_arg_spec(args,KNO_CAR(scan),escape_flags);
      if (rv<0) return rv;
      scan = KNO_CDR(scan);
      count++;}
    return count;}
  else {
    U8_STATIC_OUTPUT(arg,100);
    if (OIDP(spec)) {
      KNO_OID addr = KNO_OID_ADDR(spec);
      u8_printf(argout,"@%x/%x",KNO_OID_HI(addr),KNO_OID_LO(addr));}
    else if ( (KNO_FIXNUMP(spec)) || (KNO_BIGINTP(spec)) || (KNO_FLONUMP(spec)) )
      kno_unparse(argout,spec);
    else {
      u8_putc(argout,':');
      kno_unparse(argout,spec);}
    kno_stackvec_push
      (args,kno_make_string(NULL,u8_outlen(argout),u8_outstring(argout)));
    u8_close_output(argout);
    return 1;}
}

static void handle_config_spec(kno_stackvec configs,lispval config_spec,lispval keep_spec)
{
  if (KNO_VOIDP(keep_spec)) {}
  else if ( (KNO_FALSEP(keep_spec)) && (KNO_VOIDP(config_spec)) ) {
    kno_stackvec_push(configs,knostring("PROCPRIMS_CONFIG_RESET=yes"));
    return;}
  else {
    lispval config_args = kno_config_get("CONFIGARGS");
    if (KNO_VECTORP(config_args)) {
      int len = KNO_VECTOR_LENGTH(config_args);
      lispval *elts = KNO_VECTOR_ELTS(config_args);
      int i = 0; while (i<len) {
	lispval config = elts[i]; kno_incref(config);
	kno_stackvec_push(configs,config);
	i++;}}
    kno_decref(config_args);}
  if (!(KNO_VOIDP(config_spec))) {
    KNO_DO_CHOICES(conf,config_spec) {
      if (KNO_STRINGP(conf)) {
	if (strchr(CSTRING(conf),'='))
	  kno_stackvec_push(configs,conf);
	else {
	  lispval cval = kno_config_get(KNO_SYMBOL_NAME(conf));
	  add_configs(configs,cval,CSTRING(conf));
	  kno_decref(cval);}}
      else if (KNO_SYMBOLP(conf)) {
	lispval cval = kno_config_get(KNO_SYMBOL_NAME(conf));
	add_configs(configs,cval,KNO_SYMBOL_NAME(conf));
	kno_decref(cval);}
      else if (KNO_TABLEP(conf)) {
	lispval keys = kno_getkeys(conf);
	KNO_DO_CHOICES(key,keys) {
	  if (KNO_SYMBOLP(key)) {
	    lispval cval = kno_get(conf,key,KNO_VOID);
	    add_configs(configs,cval,KNO_SYMBOL_NAME(key));
	    kno_decref(cval);}}
	kno_decref(keys);}
      else NO_ELSE;}}
}

static void handle_env_spec(kno_stackvec environment,
			    lispval env_spec,
			    lispval keep_env)
{
  if (KNO_VOIDP(keep_env)) {}
  else if ( (KNO_FALSEP(keep_env)) && (KNO_VOIDP(env_spec)) ) {
    kno_stackvec_push(environment,knostring("PROCPRIMS_ENV_RESET=yes"));
    return;}
  else {
    char *const *scan = environ;
    while (*scan) {
      u8_string use_string = u8_fromlibc(*scan);
      lispval stringval = kno_init_string(NULL,-1,use_string);
      kno_stackvec_push(environment,stringval);
      scan++;}}
  if (!(KNO_VOIDP(env_spec))) {
    KNO_DO_CHOICES(spec,env_spec) {
      if (KNO_STRINGP(spec)) {
	if (strchr(CSTRING(spec),'=')) {
	  kno_stackvec_push(environment,spec); kno_incref(spec);}
	else u8_log(LOGWARN,"BadEnvEntry","Couldn't use string %q",spec);}
      else if (KNO_TABLEP(spec)) {
	lispval keys = kno_getkeys(spec);
	KNO_DO_CHOICES(key,keys) {
	  u8_string env_var = (SYMBOLP(key)) ? (SYMBOL_NAME(key)) :
	    (STRINGP(key)) ? (CSTRING(key)) : (NULL);
	  if (env_var) {
	    lispval val = kno_get(spec,key,KNO_VOID);
	    u8_string spec = NULL;
	    if (KNO_STRINGP(val))
	      spec = u8_mkstring("%s=%s",env_var,CSTRING(val));
	    else if (KNO_NUMBERP(val))
	      spec = u8_mkstring("%s=%q",env_var,val);
	    else if (KNO_OIDP(val))
	      spec = u8_mkstring("%s=%q",env_var,val);
	    else spec = u8_mkstring("%s=:%q",env_var,val);
	    lispval as_lisp = kno_init_string(NULL,-1,spec);
	    kno_stackvec_push(environment,as_lisp);}}}
      else NO_ELSE;}}
}

static void add_configs(kno_stackvec configs,lispval value,u8_string name)
{
  if (KNO_CHOICEP(value)) {
    KNO_DO_CHOICES(v,value) { add_configs(configs,v,name); }}
  else {
    u8_string conf_spec = u8_mkstring("%s=:%q",name,value);
    lispval as_lisp = kno_init_string(NULL,-1,conf_spec);
    kno_stackvec_push(configs,as_lisp);}
}

static u8_string mkidstring(u8_string progname,int n,kno_argvec args)
{
  struct U8_OUTPUT idout; U8_INIT_OUTPUT(&idout,200);
  u8_puts(&idout,progname);
  int i = 0; while (i<n) {
    lispval v = args[i];
    u8_putc(&idout,' ');
    if (KNO_STRINGP(v)) {
      u8_string s = KNO_CSTRING(v);
      if (strchr(s,' '))
	kno_unparse(&idout,v);
      else u8_puts(&idout,s);}
    else u8_printf(&idout,"'%q'",v);
    i++;}
  return u8_outstring(&idout);
}

/* Procedures on subproc objects */

DEFC_PRIM("subproc?",subprocp_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the numeric process ID for the subproc *proc*",
	  {"obj",kno_any_type,KNO_VOID})
static lispval subprocp_prim(lispval obj)
{
  if (KNO_TYPEP(obj,kno_subproc_type))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("proc-pid",proc_pid,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the numeric process ID for the subproc *proc*",
	  {"proc",kno_subproc_type,KNO_VOID})
static lispval proc_pid(lispval proc)
{
  struct KNO_SUBPROC *sj = (kno_subproc) proc;
  return KNO_INT(sj->proc_pid);
}

DEFC_PRIM("proc-id",proc_idstring,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the id string for the subproc *proc*",
	  {"proc",kno_subproc_type,KNO_VOID})
static lispval proc_idstring(lispval proc)
{
  struct KNO_SUBPROC *sj = (kno_subproc) proc;
  return knostring(sj->proc_id);
}

DEFC_PRIM("proc-progname",proc_progname,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the program name for the subproc *proc*",
	  {"proc",kno_subproc_type,KNO_VOID})
static lispval proc_progname(lispval proc)
{
  struct KNO_SUBPROC *sj = (kno_subproc) proc;
  return kno_init_string(NULL,-1,u8_fromlibc((char *)(sj->proc_progname)));
}

DEFC_PRIM("proc-argv",proc_argv,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the program name for the subproc *proc*",
	  {"proc",kno_subproc_type,KNO_VOID})
static lispval proc_argv(lispval proc)
{
  struct KNO_SUBPROC *sj = (kno_subproc) proc;
  char **argv = sj->proc_argv;
  int argc = stringvec_len(argv);
  lispval vec = kno_make_vector(argc,NULL);
  lispval *elts = KNO_VECTOR_ELTS(vec);
  int i = 0; while (i<argc) {
    char *arg = argv[i];
    elts[i]=kno_init_string(NULL,-1,u8_fromlibc((char *)arg));
    i++;}
  return vec;
}

DEFC_PRIM("proc-env",proc_envp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the environment initialization for the subproc *proc*",
	  {"proc",kno_subproc_type,KNO_VOID})
static lispval proc_envp(lispval proc)
{
  struct KNO_SUBPROC *sj = (kno_subproc) proc;
  char **argv = sj->proc_envp;
  int envc = stringvec_len(argv);
  lispval vec = kno_make_vector(envc,NULL);
  lispval *elts = KNO_VECTOR_ELTS(vec);
  int i = 0; while (i<envc) {
    char *arg = argv[i];
    elts[i]=kno_init_string(NULL,-1,u8_fromlibc((char *)arg));
    i++;}
  return vec;
}

DEFC_PRIM("proc-stdin",proc_stdin,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns an output port for sending to the proc.",
	  {"proc",kno_subproc_type,KNO_VOID})
static lispval proc_stdin(lispval proc)
{
  struct KNO_SUBPROC *sj = (kno_subproc) proc;
  return kno_incref(sj->proc_stdin);
}

DEFC_PRIM("proc-stdout",proc_stdout,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns an input port for reading the output of "
	  "proc.",
	  {"proc",kno_subproc_type,KNO_VOID})
static lispval proc_stdout(lispval proc)
{
  struct KNO_SUBPROC *sj = (kno_subproc) proc;
  return kno_incref(sj->proc_stdout);
}

DEFC_PRIM("proc-stderr",proc_stderr,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns an input port for reading the error "
	  "output (stderr)  of proc.",
	  {"proc",kno_subproc_type,KNO_VOID})
static lispval proc_stderr(lispval proc)
{
  struct KNO_SUBPROC *sj = (kno_subproc) proc;
  return kno_incref(sj->proc_stderr);
}

DEFC_PRIM("proc-started",proc_started,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the time the process was started (relative to this "
	  "process's startup).",
	  {"proc",kno_subproc_type,KNO_VOID})
static lispval proc_started(lispval proc)
{
  struct KNO_SUBPROC *sj = (kno_subproc) proc;
  return kno_make_flonum(sj->proc_started);
}

DEFC_PRIM("proc-finished",proc_finished,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the time the process terminated (relative to this "
	  "process's startup), or #f if it hasn't finished.",
	  {"proc",kno_subproc_type,KNO_VOID})
static lispval proc_finished(lispval proc)
{
  struct KNO_SUBPROC *sj = (kno_subproc) proc;
  if (sj->proc_finished>0)
    return kno_make_flonum(sj->proc_finished);
  else return KNO_FALSE;
}

DEFC_PRIM("proc-runtime",proc_runtime,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the runtime of the process, either total or to date.",
	  {"proc",kno_subproc_type,KNO_VOID})
static lispval proc_runtime(lispval proc)
{
  struct KNO_SUBPROC *sj = (kno_subproc) proc;
  if (sj->proc_finished>0)
    return kno_make_flonum(sj->proc_finished-sj->proc_started);
  else if (check_finished(sj))
    return kno_make_flonum(sj->proc_finished-sj->proc_started);
  else {
    double now = u8_elapsed_time();
    return kno_make_flonum(now-sj->proc_started);}
}

DEFC_PRIM("proc/signal",proc_signal,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "sends the number *signal* to the process "
	  "executing *proc*.",
	  {"proc",kno_any_type,KNO_VOID},
	  {"sigval",kno_any_type,KNO_VOID})
static lispval proc_signal(lispval proc,lispval sigval)
{
  struct KNO_SUBPROC *sj = (kno_subproc) proc;
  int sig = KNO_INT(sigval);
  int pid = sj->proc_pid;
  if ((sig>0)&&(sig<256)) {
    int rv = kill(pid,sig);
    if (rv<0) {
      char buf[128]; sprintf(buf,"pid=%ld;sig=%d",(long int)pid,sig);
      u8_graberrno("proc_signal",u8_strdup(buf));
      return KNO_ERROR;}
    else return KNO_TRUE;}
  else return kno_type_error("signal","proc_signal",sigval);
}

/* EXIT functions */

DEFC_PRIM("exit",exit_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	  "exits the current process with a return code of "
	  "*retval* (defaults to 0)",
	  {"arg",kno_any_type,KNO_VOID})
static lispval exit_prim(lispval arg)
{
  pid_t main_thread = getpid();
  int rv = kill(main_thread,SIGTERM);
  if (rv<0)
    return kno_err("ExitFailed","exit_prim",NULL,VOID);
  else return VOID;
}

DEFC_PRIM("exit/fast",fast_exit_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	  "exits the current process expeditiously without, "
	  "for example, freeing memory which will just be "
	  "returned to the OS after exit.",
	  {"arg",kno_any_type,KNO_VOID})
static lispval fast_exit_prim(lispval arg)
{
  kno_fast_exit=1;
  pid_t main_thread = getpid();
  int rv = kill(main_thread,SIGTERM);
  if (rv<0)
    return kno_err("ExitFailed","exit_prim",NULL,VOID);
  else return VOID;
}

/* Getting info from PIDs or PROCS */

static pid_t pid_arg(lispval x,u8_context caller,int err)
{
  if (KNO_FIXNUMP(x)) {
    long long ival = KNO_FIX2INT(x);
    if (ival<0) {
      if (err) {
	kno_seterr(NotPID,caller,NULL,x);
	return -1;}
      else return 0;}
    int rv = kill((pid_t)ival,0); errno = 0;
    if (rv<0) {
      if (err) {
	kno_seterr(NotPID,caller,NULL,x);
	return -1;}
      else return 0;}
    else return (pid_t) ival;}
  else if (KNO_TYPEP(x,kno_subproc_type)) {
    struct KNO_SUBPROC *sj = (struct KNO_SUBPROC *)x;
    return sj->proc_pid;}
  else if (err) {
    kno_seterr(NotPID,caller,NULL,x);
    return -1;}
  else return 0;
}

static lispval pid_error(lispval arg,u8_context caller)
{
  int saved_errno = errno; errno = 0;
  kno_seterr(ProcinfoError,caller,u8_strerror(saved_errno),arg);
  return KNO_ERROR;
}

static void init_proc_result(lispval result,lispval arg,pid_t pid)
{
  kno_store(result,KNOSYM(pid),KNO_INT(pid));
  if (KNO_TYPEP(arg,kno_subproc_type)) {
    struct KNO_SUBPROC *sp = (kno_subproc) arg;
    lispval idstring = (sp->proc_id>0) ?
      (kno_make_string(NULL,-1,sp->proc_id)) :
      (KNO_FALSE);
    lispval progname =
      (kno_init_string(NULL,-1,u8_fromlibc((char *)(sp->proc_progname))));
    lispval started = kno_make_flonum(sp->proc_started);
    lispval finished = (sp->proc_finished>0) ?
      (kno_make_flonum(sp->proc_finished)) : (KNO_VOID);
    lispval runtime = (sp->proc_finished>0) ?
      (kno_make_flonum((sp->proc_finished)-(sp->proc_started))) :
      (kno_make_flonum(u8_elapsed_time()-(sp->proc_started)));
    kno_store(result,KNOSYM(procid),idstring);
    kno_store(result,KNOSYM(progname),progname);
    kno_store(result,KNOSYM(started),started);
    kno_store(result,KNOSYM(runtime),runtime);
    if (!(KNO_VOIDP(finished))) kno_store(result,KNOSYM(finished),finished);
    kno_decref(idstring);
    kno_decref(progname);
    kno_decref(runtime);
    kno_decref(finished);
    kno_decref(started);}
}

/* Getting PID information */
static lispval pid_status(lispval arg)
{
  pid_t pid = pid_arg(arg,"proc_status",1);
  if (pid<0) return KNO_ERROR;
  int status = -1;
  int rv = waitpid(pid,&status,WNOHANG|WUNTRACED|WCONTINUED);
  if (rv<0) return pid_error(arg,"proc_status");
  if ( (WIFEXITED(status)) && (WIFSIGNALED(status)) ) {
    if (KNO_TYPEP(arg,kno_subproc_type)) {
      struct KNO_SUBPROC *sp = (kno_subproc) arg;
      if (sp->proc_finished<0) sp->proc_finished = u8_elapsed_time();}}
  lispval result = kno_make_slotmap(5,0,NULL);
  init_proc_result(result,arg,pid);
  extract_pid_status(status,result);
  return result;
}

static lispval pid_wait(lispval arg)
{
  pid_t pid = pid_arg(arg,"proc_wait",1);
  if (pid<0) return KNO_ERROR;
  int status = -1;
  int rv = waitpid(pid,&status,0);
  if (rv<0) return pid_error(arg,"proc_wait");
  if ( (WIFEXITED(status)) && (WIFSIGNALED(status)) ) {
    if (KNO_TYPEP(arg,kno_subproc_type)) {
      struct KNO_SUBPROC *sp = (kno_subproc) arg;
      if (sp->proc_finished<0) sp->proc_finished = u8_elapsed_time();}}
  lispval result = kno_make_slotmap(5,0,NULL);
  init_proc_result(result,arg,pid);
  extract_pid_status(status,result);
  return result;
}

static lispval pid_info(lispval arg)
{
  pid_t pid = pid_arg(arg,"proc_info",1);
  if (pid<0) return KNO_ERROR;
  int status = -1;
#if HAVE_WAIT4
  struct rusage usage = { 0 };
  int rv = wait4(pid,&status,WNOHANG|WUNTRACED|WCONTINUED,&usage);
#else
  int rv = waitpid(pid,&status,WNOHANG|WUNTRACED|WCONTINUED);
#endif
  if (rv<0) return pid_error(arg,"proc_info");
  if ( (WIFEXITED(status)) && (WIFSIGNALED(status)) ) {
    if (KNO_TYPEP(arg,kno_subproc_type)) {
      struct KNO_SUBPROC *sp = (kno_subproc) arg;
      if (sp->proc_finished<0) sp->proc_finished = u8_elapsed_time();}}
  lispval result = kno_make_slotmap(4,0,NULL);
  init_proc_result(result,arg,pid);
  extract_pid_status(status,result);
#if HAVE_WAIT4
  extract_rusage(result,&usage,NULL);
#endif
  return result;
}

static lispval pid_livep(lispval arg)
{
  pid_t pid = pid_arg(arg,"pid_livep",1);
  if (pid<0) return KNO_ERROR;
  int status = -1;
  int rv = waitpid(pid,&status,WNOHANG|WUNTRACED|WCONTINUED);
  if (rv<0) return pid_error(arg,"pid_livep");
  if ( (WIFEXITED(status)) || (WIFSIGNALED(status)) )
    return KNO_FALSE;
  else return KNO_TRUE;
}

static lispval pid_runningp(lispval arg)
{
  pid_t pid = pid_arg(arg,"pid_runningp",1);
  if (pid<0) return KNO_ERROR;
  int status = -1;
  int rv = waitpid(pid,&status,WNOHANG|WUNTRACED|WCONTINUED);
  if (rv<0) return pid_error(arg,"pid_runningp");
  if ( (WIFEXITED(status)) || (WIFSIGNALED(status)) )
    return KNO_FALSE;
  else if ( (WIFSTOPPED(status)) && (! (WIFCONTINUED(status)) ) )
    return KNO_FALSE;
  else return KNO_TRUE;
}

static lispval pid_stoppedp(lispval arg)
{
  pid_t pid = pid_arg(arg,"proc_stoppedp",1);
  if (pid<0) return KNO_ERROR;
  int status = -1;
  int rv = waitpid(pid,&status,WNOHANG|WUNTRACED|WCONTINUED);
  if (rv<0) return pid_error(arg,"proc_stoppedp");
  if (WIFSTOPPED(status))
    return KNO_TRUE;
  else return KNO_FALSE;
}

/* Generic PROC primitives */

DEFC_PRIM("proc/status",proc_status,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns a table of information about *proc*, "
	  "which can be a numeric PID or a proc object.",
	  {"proc",kno_any_type,KNO_VOID})
static lispval proc_status(lispval arg)
{
  if (KNO_INTEGERP(arg))
    return pid_info(arg);
  else if (SUBPROCP(arg)) {
    kno_subproc sp = (kno_subproc)arg;
    if (check_finished(sp)) {
      int status = sp->proc_status;
      lispval result = kno_make_slotmap(5,0,NULL);
      init_proc_result(result,arg,sp->proc_pid);
      extract_pid_status(status,result);
      return result;}
    else return pid_status(SUBPROC_PID(sp));}
  else return kno_err("NotAProc","proc/status",NULL,arg);
}

DEFC_PRIM("proc/wait",proc_wait,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Waits for *proc* to exit and returns a table of information "
	  "about how it exited.",
	  {"proc",kno_any_type,KNO_VOID})
static lispval proc_wait(lispval arg)
{
  if (KNO_INTEGERP(arg))
    return pid_info(arg);
  else if (SUBPROCP(arg)) {
    kno_subproc sp = (kno_subproc)arg;
    if (check_finished(sp)) {
      int status = sp->proc_status;
      lispval result = kno_make_slotmap(5,0,NULL);
      init_proc_result(result,arg,sp->proc_pid);
      extract_pid_status(status,result);
      return result;}
    else return pid_wait(SUBPROC_PID(sp));}
  else return kno_err("NotAProc","proc/wait",NULL,arg);
}

DEFC_PRIM("proc/info",proc_info,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns a detailed table of information about *proc*, "
	  "which can be a numeric PID or a proc object. Unlike proc/status "
	  "this returns rusage data if available.",
	  {"proc",kno_any_type,KNO_VOID})
static lispval proc_info(lispval arg)
{
  if (KNO_INTEGERP(arg))
    return pid_info(arg);
  else if (SUBPROCP(arg)) {
    kno_subproc sp = (kno_subproc)arg;
    if (check_finished(sp)) {
      int status = sp->proc_status;
      lispval result = kno_make_slotmap(5,0,NULL);
      init_proc_result(result,arg,sp->proc_pid);
      extract_pid_status(status,result);
      return result;}
    else return pid_info(SUBPROC_PID(sp));}
  else return kno_err("NotAProc","proc/info",NULL,arg);
}

DEFC_PRIM("proc/live?",proc_livep,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns true if *proc* hasn't exited or been terminated",
	  {"proc",kno_any_type,KNO_VOID})
static lispval proc_livep(lispval arg)
{
  if (KNO_INTEGERP(arg))
    return pid_livep(arg);
  else if (SUBPROCP(arg)) {
    kno_subproc sp = (kno_subproc)arg;
    if (check_finished(sp)) {
      int status = sp->proc_status;
      if ( (WIFEXITED(status)) || (WIFSIGNALED(status)) )
	return KNO_FALSE;
      return KNO_TRUE;}
    else return pid_livep(SUBPROC_PID(sp));}
  else return kno_err("NotAProc","proc/live?",NULL,arg);
}

DEFC_PRIM("proc/running?",proc_runningp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns true if *proc* hasn't exited or been terminated",
	  {"proc",kno_any_type,KNO_VOID})
static lispval proc_runningp(lispval arg)
{
  if (KNO_INTEGERP(arg))
    return pid_runningp(arg);
  else if (SUBPROCP(arg)) {
    kno_subproc sp = (kno_subproc)arg;
    if (check_finished(sp)) {
      int status = sp->proc_status;
      if ( (WIFEXITED(status)) || (WIFSIGNALED(status)) )
	return KNO_FALSE;
      else if ( (WIFSTOPPED(status)) && (! (WIFCONTINUED(status)) ) )
	return KNO_FALSE;
      else return KNO_TRUE;}
    else return pid_runningp(SUBPROC_PID(sp));}
  else return kno_err("NotAProc","proc/running?",NULL,arg);
}

DEFC_PRIM("proc/stopped?",proc_stoppedp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns true if *proc* isn't running but exited",
	  {"proc",kno_any_type,KNO_VOID})
static lispval proc_stoppedp(lispval arg)
{
  if (KNO_INTEGERP(arg))
    return pid_stoppedp(arg);
  else if (SUBPROCP(arg)) {
    kno_subproc sp = (kno_subproc)arg;
    if (check_finished(sp))
      return KNO_FALSE;
    else return pid_runningp(SUBPROC_PID(sp));}
  else return kno_err("NotAProc","proc/running?",NULL,arg);
}

/* SUBPROC object */

static lispval cons_subproc(pid_t pid,u8_string id,
			    char *cprogname,char **argv,char **envp,
			    lispval in,lispval out,lispval err,
			    lispval opts)
{
  // TODO: Have a global list/array of subprocesses
  struct KNO_SUBPROC *proc = u8_alloc(struct KNO_SUBPROC);
  KNO_INIT_CONS(proc,kno_subproc_type);
  proc->annotations = KNO_EMPTY;
  proc->proc_started = u8_elapsed_time();
  proc->proc_pid = pid;
  proc->proc_id = id;
  proc->proc_stdin = in;
  proc->proc_stdout = out;
  proc->proc_stderr = err;
  proc->proc_opts = opts;
  proc->proc_progname = cprogname;
  proc->proc_argv = argv;
  proc->proc_envp = envp;
  proc->proc_finished = -1;
  lispval pid_val = KNO_INT((long long)pid);
  lispval procval = (lispval) proc;
  int rv = kno_hashtable_store(proctable,pid_val,procval);
  if (rv<0) {
    kno_decref(pid_val);
    kno_seterr("FailedRegisterProc","cons_subproc",NULL,procval);
    kno_decref(procval);
    return KNO_ERROR;}
  else return (lispval) proc;
}

static int unparse_subproc(u8_output out,lispval x)
{
  struct KNO_SUBPROC *sj = (struct KNO_SUBPROC *)x;
  int argc = stringvec_len(sj->proc_argv);
  int envc = stringvec_len(sj->proc_envp);
  u8_string status = (sj->proc_finished>0) ? (" (finished)") :
    (sj->proc_pid<0) ? (" (finishing)") :
    (" (running)");
  if (envc)
    u8_printf(out,"#<SUBPROC %lld '%s' %s%d... env=%d...>",
	      (long long)(sj->proc_pid),sj->proc_id,status,argc,envc);
  else u8_printf(out,"#<SUBPROC %lld '%s' $s%d..>",
		 (long long)sj->proc_pid,sj->proc_id,status,argc);
  return 1;
}

static void recycle_subproc(struct KNO_RAW_CONS *c)
{
  struct KNO_SUBPROC *sp = (struct KNO_SUBPROC *)c;
  kno_decref(sp->proc_stdin); sp->proc_stdin=KNO_VOID;
  kno_decref(sp->proc_stdout); sp->proc_stdout=KNO_VOID;
  kno_decref(sp->proc_stderr); sp->proc_stderr=KNO_VOID;
  kno_decref(sp->proc_opts); sp->proc_opts=KNO_VOID;
  if (sp->proc_progname) u8_free(sp->proc_progname);
  free_stringvec(sp->proc_argv);
  free_stringvec(sp->proc_envp);
  if (sp->proc_id) u8_free(sp->proc_id);
  if (sp->annotations) kno_decref(sp->annotations);
  if (!(KNO_STATIC_CONSP(c))) u8_free(c);
}

/* Handle sigchld */


static void handle_sigchld(int sig)
{
  int status;
  pid_t pid = waitpid(-1, &status, WNOHANG);
  if ( (pid>0) && ( (WIFEXITED(status)) || (WIFSIGNALED(status)) ) ) {
    lispval pidval = KNO_INT(pid);
    lispval proc = kno_hashtable_get(proctable,pidval,KNO_VOID);
    if (!(KNO_VOIDP(proc))) {
      finish_subproc((kno_subproc)proc);
      kno_decref(proc);}
    kno_decref(pidval);}
}

/* The init function */

static int scheme_procprims_initialized = 0;

static lispval procprims_module;

KNO_EXPORT void kno_init_procprims_c()
{
  if (scheme_procprims_initialized) return;
  scheme_procprims_initialized = 1;
  kno_init_scheme();
  procprims_module = kno_new_cmodule
    ("procprims",(KNO_MODULE_DEFAULT),kno_init_procprims_c);
  u8_register_source_file(_FILEINFO);

  KNO_INIT_STATIC_CONS(&_proctable,kno_hashtable_type);
  u8_init_rwlock(&(_proctable.table_rwlock));
  kno_make_hashtable(&_proctable,73);
  proctable = &_proctable;

  init_rlimit_codes();
  link_local_cprims();

  kno_unparsers[kno_subproc_type] = unparse_subproc;
  kno_recyclers[kno_subproc_type] = recycle_subproc;

  kno_tablefns[kno_subproc_type] = kno_annotated_tablefns;

  id_symbol = kno_intern("id");
  stdin_symbol = kno_intern("stdin");
  stdout_symbol = kno_intern("stdout");
  stderr_symbol = kno_intern("stderr");
  nice_symbol = kno_intern("nice");

  kno_finish_cmodule(procprims_module);

  sigset(SIGCHLD,handle_sigchld);
}

static void link_local_cprims()
{
  KNO_LINK_CPRIMN("proc/open",proc_open_prim,procprims_module);
  KNO_LINK_CPRIM("proc-pid",proc_pid,1,procprims_module);
  KNO_LINK_CPRIM("proc-id",proc_idstring,1,procprims_module);
  KNO_LINK_CPRIM("proc-progname",proc_progname,1,procprims_module);
  KNO_LINK_CPRIM("proc-argv",proc_argv,1,procprims_module);
  KNO_LINK_CPRIM("proc-envp",proc_envp,1,procprims_module);
  KNO_LINK_CPRIM("proc-stdin",proc_stdin,1,procprims_module);
  KNO_LINK_CPRIM("proc-stdout",proc_stdout,1,procprims_module);
  KNO_LINK_CPRIM("proc-stderr",proc_stderr,1,procprims_module);
  KNO_LINK_CPRIM("proc-started",proc_started,1,procprims_module);
  KNO_LINK_CPRIM("proc-finished",proc_finished,1,procprims_module);
  KNO_LINK_CPRIM("proc-runtime",proc_runtime,1,procprims_module);

  KNO_LINK_CPRIM("subproc?",subprocp_prim,1,procprims_module);
  KNO_LINK_ALIAS("proc?",subprocp_prim,procprims_module);

  KNO_LINK_CPRIM("nice",nice_prim,1,procprims_module);
  KNO_LINK_CPRIM("setrlimit!",setrlimit_prim,3,procprims_module);
  KNO_LINK_CPRIM("getrlimit",getrlimit_prim,2,procprims_module);
  KNO_LINK_CPRIM("pid/kill!",pid_kill_prim,2,procprims_module);
  KNO_LINK_CPRIM("pid?",ispid_prim,1,procprims_module);
  KNO_LINK_CPRIM("exit/fast",fast_exit_prim,1,procprims_module);
  KNO_LINK_CPRIM("exit",exit_prim,1,procprims_module);
  KNO_LINK_CPRIM("proc/signal",proc_signal,2,procprims_module);

  KNO_LINK_CPRIM("proc/live?",proc_livep,1,procprims_module);
  KNO_LINK_CPRIM("proc/running?",proc_runningp,1,procprims_module);
  KNO_LINK_CPRIM("proc/stopped?",proc_stoppedp,1,procprims_module);
  KNO_LINK_CPRIM("proc/status",proc_status,1,procprims_module);
  KNO_LINK_CPRIM("proc/info",proc_info,1,procprims_module);
  KNO_LINK_CPRIM("proc/wait",proc_wait,1,procprims_module);
}
