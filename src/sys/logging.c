/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/numbers.h"
#include "kno/apply.h"

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
#if KNO_FILECONFIG_ENABLED
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

/* Log functions */

static lispval kno_logfns = EMPTY;
static lispval kno_logfn = VOID;
static int using_kno_logger = 0;
static u8_mutex log_lock;

static int req_loglevel = -1, req_logonly = -1;

#define MAX_LOGLEVEL 8
static char *loglevel_names[]=
  {"Emergency","Alert","Critical","Error","Warning","Notice","Info",
   "Debug","Detail","Deluge"};

U8_EXPORT int u8_default_logger(int loglevel,u8_condition c,u8_string message);

static int default_log_error(void);

static int kno_logger(int loglevel,u8_condition c,u8_string message)
{
  int local_log = (loglevel<0);
  int abs_loglevel = ((loglevel<0)?(-loglevel):(loglevel));
  lispval ll = KNO_INT(loglevel);
  lispval csym = ((c == NULL)?(KNO_FALSE):(kno_intern((u8_string)c)));
  struct U8_OUTPUT *reqout = ((req_loglevel>=loglevel)?(kno_try_reqlog()):(NULL));
  lispval mstring = kno_make_string(NULL,-1,message);
  lispval args[3]={ll,csym,mstring};
  char *level = ((abs_loglevel>MAX_LOGLEVEL)?(NULL):
                 (loglevel_names[abs_loglevel]));
  if (reqout) {
    struct U8_XTIME xt;
    u8_local_xtime(&xt,-1,u8_nanosecond,0);
    if (local_log)
      u8_printf(reqout,"<logentry level='%d' scope='local'>",
                abs_loglevel);
    else u8_printf(reqout,"<logentry level='%d'>",abs_loglevel);
    if (level) u8_printf(reqout,"\n\t<level>%s</level>",level);
    u8_printf(reqout,"\n\t<datetime tick='%ld' nsecs='%d'>%Xlt</datetime>",
              xt.u8_tick,xt.u8_nsecs,&xt);
    if (c) u8_printf(reqout,"\n\t<condition>%s</condition>",c);
    u8_printf(reqout,"\n\t<message>\n%s\n\t</message>",message);
    u8_printf(reqout,"\n</logentry>\n");}
  if ((reqout!=NULL)&&
      (!(local_log))&&
      (req_logonly>=0)&&
      (abs_loglevel>=LOG_NOTIFY)&&
      (abs_loglevel>=req_logonly)) {
    kno_decref(mstring);
    return 0;}
  else if (VOIDP(kno_logfn))
    u8_default_logger(loglevel,c,message);
  else {
    lispval logfn = kno_incref(kno_logfn);
    lispval v = kno_apply(logfn,3,args);
    if (KNO_ABORTP(v)) {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,1024);
      u8_default_logger(loglevel,c,message);
      u8_default_logger(LOG_CRIT,"Log Error","Kno log call failed");
      default_log_error();
      kno_logfn = VOID;
      kno_decref(logfn);
      kno_decref(logfn);}
    else {
      kno_decref(logfn);
      kno_decref(v);}}
  lispval logfns = kno_make_simple_choice(kno_logfns); {
    DO_CHOICES(logfn,logfns) {
      lispval v = kno_apply(logfn,3,args);
      if (KNO_ABORTP(v)) {
        u8_default_logger(LOG_CRIT,"Log Error","Kno log call failed");
        default_log_error();}
      kno_decref(v);}}
  kno_decref(mstring);
  kno_decref(ll);
  kno_decref(logfns);
  return 1;
}

static int default_log_error()
{
  u8_exception ex = u8_erreify(), scan = ex; int n_errs = 0;
  while (scan) {
    u8_string sum = kno_errstring(scan);
    u8_logger(LOG_ERR,scan->u8x_cond,sum);
    u8_free(sum);
    scan = scan->u8x_prev;
    n_errs++;}
  u8_free_exception(ex,1);
  return n_errs;
}

static void use_kno_logger()
{
  if (using_kno_logger) return;
  else {
    u8_lock_mutex(&log_lock);
    if (!(using_kno_logger)) {
      u8_set_logfn(kno_logger);
      using_kno_logger = 1;}
    u8_unlock_mutex(&log_lock);}
}

static lispval config_get_logfn(lispval var,void *data)
{
  if (VOIDP(kno_logfn)) return KNO_FALSE;
  else return kno_incref(kno_logfn);
}

static int config_set_logfn(lispval var,lispval val,void *data)
{
  if (VOIDP(kno_logfn)) {
    kno_logfn = val;
    kno_incref(val);
    use_kno_logger();
    return 1;}
  else if (kno_logfn == val)
    return 0;
  else {
    lispval oldfn = kno_logfn;
    kno_logfn = val;
    kno_incref(val);
    kno_decref(oldfn);
    return 1;}
}

static lispval config_get_logfns(lispval var,void *data)
{
  return kno_incref(kno_logfns);
}

static int config_add_logfn(lispval var,lispval val,void *data)
{
  lispval arity = -1;
  if (KNO_FUNCTIONP(val)) arity = KNO_FUNCTION_ARITY(val);
  if (arity!=3)
    return KNO_ERR(-1,kno_TypeError,"config_add_logfn","log function",val);
  use_kno_logger(); kno_incref(val);
  u8_lock_mutex(&log_lock);
  CHOICE_ADD(kno_logfns,val);
  kno_logfns = kno_simplify_choice(kno_logfns);
  u8_unlock_mutex(&log_lock);
  return 1;
}

/* Request logging */

static lispval config_get_reqloglevel(lispval var,void *data)
{
  if ((using_kno_logger)&&(req_loglevel>=0))
    return KNO_INT(req_loglevel);
  else return KNO_FALSE;
}

static int config_set_reqloglevel(lispval var,lispval val,void *data)
{
  if (FALSEP(val)) {
    if (req_loglevel>=0) {req_loglevel = -1; return 1;}
    else return 0;}
  else if (FIXNUMP(val)) {
    long long level = FIX2INT(val);
    if ((level>INT_MAX)||(level<INT_MIN))
      return KNO_ERR(-1,"Invalid loglevel","config_set_reqloglevel",NULL,val);
    if (level == req_loglevel) return 0;
    else if (level>=0) use_kno_logger();
    else {}
    req_loglevel = level;
    return 1;}
  else if (req_loglevel>=0) return 0;
  else {
    use_kno_logger();
    req_loglevel = 5;
    return 1;}
}

static lispval config_get_reqlogonly(lispval var,void *data)
{
  if ((using_kno_logger)&&(req_logonly>=0))
    return KNO_INT(req_logonly);
  else return KNO_FALSE;
}

static int config_set_reqlogonly(lispval var,lispval val,void *data)
{
  if (FALSEP(val)) {
    if (req_logonly>=0) {req_logonly = -1; return 1;}
    else return 0;}
  else if (FIXNUMP(val)) {
    long long level = FIX2INT(val);
    if ((level>INT_MAX)||(level<INT_MIN))
      return KNO_ERR(-1,"Invalid loglevel","config_set_reqloglevel",NULL,val);
    if (level == req_logonly) return 0;
    else if (level>=0) use_kno_logger();
    else {}
    req_logonly = level;
    return 1;}
  else if (req_logonly>=0) return 0;
  else {
    use_kno_logger();
    req_logonly = 5;
    return 1;}
}

void setup_logging()
{
  u8_logprefix = u8_strdup(";; ");
  u8_logsuffix = u8_strdup("\n");
  u8_logindent = u8_strdup(";;     ");

  kno_register_config
    ("LOGLEVEL",_("Required priority for messages to be displayed"),
     kno_intconfig_get,kno_loglevelconfig_set,
     &u8_loglevel);
  kno_register_config
    ("LOGLEVEL:STDOUT",
     _("Required priority for messages to be displayed on stdout"),
     kno_intconfig_get,kno_loglevelconfig_set,
     &u8_stdout_loglevel);
  kno_register_config
    ("LOGLEVEL:STDERR",
     _("Required priority for messages to be displayed on stderr"),
     kno_intconfig_get,kno_loglevelconfig_set,
     &u8_stderr_loglevel);

  /* Default logindent for Kno */
  if (!(u8_logindent)) u8_logindent = u8_strdup("       ");
  kno_register_config
    ("LOGINDENT",_("String for indenting multi-line log messages"),
     kno_sconfig_get,kno_sconfig_set,&u8_logindent);
  kno_register_config
    ("LOGPREFIX",_("Prefix for log messages (default '[')"),
     kno_sconfig_get,kno_sconfig_set,&u8_logprefix);
  kno_register_config
    ("LOGSUFFIX",_("Suffix for log messages (default ']\\n')"),
     kno_sconfig_get,kno_sconfig_set,&u8_logsuffix);

  kno_register_config
    ("LOGDATE",_("Whether to show the date in log messages"),
     kno_boolconfig_get,kno_boolconfig_set,
     &u8_log_show_date);
  kno_register_config
    ("LOGPROCINFO",_("Whether to show PID/appid info in messages"),
     kno_boolconfig_get,kno_boolconfig_set,
     &u8_log_show_procinfo);
  kno_register_config
    ("LOGTHREADINFO",_("Whether to show thread id info in messages"),
     kno_boolconfig_get,kno_boolconfig_set,
     &u8_log_show_threadinfo);
  kno_register_config
    ("LOGELAPSED",_("Whether to show elapsed time in messages"),
     kno_boolconfig_get,kno_boolconfig_set,
     &u8_log_show_elapsed);

  kno_register_config
    ("LOG:DATE",_("Whether to show the date in log messages"),
     kno_boolconfig_get,kno_boolconfig_set,
     &u8_log_show_date);
  kno_register_config
    ("LOG:ELAPSED",_("Whether to show elapsed time in messages"),
     kno_boolconfig_get,kno_boolconfig_set,
     &u8_log_show_elapsed);
  kno_register_config
    ("LOG:THREADID",_("Whether to show thread id info in messages"),
     kno_boolconfig_get,kno_boolconfig_set,
     &u8_log_show_threadinfo);

#if U8_THREAD_DEBUG
  kno_register_config
    ("THREAD_DEBUG",_("Whether to output thread debug messages"),
     kno_boolconfig_get,kno_boolconfig_set,
     &u8_thread_log_enabled);
  kno_register_config
    ("THREAD_LOGLEVEL",_("Loglevel to use for thread debug messages"),
     kno_intconfig_get,kno_loglevelconfig_set,
     &u8_thread_debug_loglevel);
#else
  /* If THREAD_DEBUG isn't enabled, these config variables can't be
     set */
  kno_register_config
    ("THREAD_DEBUG",_("Whether to output thread debug messages"),
     kno_boolconfig_get,NULL,&u8_thread_log_enabled);
  kno_register_config
    ("THREAD_LOGLEVEL",_("Loglevel to use for thread debug messages"),
     kno_intconfig_get,NULL,&u8_thread_debug_loglevel);
#endif

}

void kno_init_logging_c()
{
  u8_register_source_file(_FILEINFO);

  u8_init_mutex(&log_lock);

  setup_logging();

  kno_register_config
    ("LOGFN",_("the default log function"),
     config_get_logfn,config_set_logfn,NULL);
  kno_register_config
    ("LOGFNS",_("additional log functions"),
     config_get_logfns,config_add_logfn,NULL);
  kno_register_config
    ("REQ:LOGLEVEL",_("whether to use Kno per-request logging"),
     config_get_reqloglevel,config_set_reqloglevel,NULL);
  kno_register_config
    ("REQ:LOGONLY",
     _("only use per-request logging (when available) for loglevels >= this"),
     config_get_reqlogonly,config_set_reqlogonly,NULL);
}

