/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/dtype.h"
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
#include <libu8/u8elapsed.h>
#include <libu8/u8netfns.h>
#include <libu8/u8printf.h>
#if KNO_FILECONFIG_ENABLED
#include <libu8/u8filefns.h>
#include <libu8/libu8io.h>
#endif

/* Converting signals to exceptions */

struct sigaction sigaction_raise, sigaction_abort;
struct sigaction sigaction_exit, sigaction_default;
static sigset_t sigcatch_set, sigexit_set, sigdefault_set, sigabort_set;

static sigset_t default_sigmask;
sigset_t *kno_default_sigmask = &default_sigmask;

struct sigaction *kno_sigaction_raise = &sigaction_raise;
struct sigaction *kno_sigaction_abort = &sigaction_abort;
struct sigaction *kno_sigaction_exit = &sigaction_exit;
struct sigaction *kno_sigaction_default = &sigaction_default;

static void siginfo_raise(int signum,siginfo_t *info,void *stuff)
{
  u8_contour c = u8_dynamic_contour;
  u8_condition ex = u8_signal_name(signum);
  if (!(c)) {
    u8_log(LOG_CRIT,ex,"Unexpected signal");
    exit(1);}
  else {
    u8_raise(ex,c->u8c_label,NULL);}
  exit(1);
}

static void siginfo_exit(int signum,siginfo_t *info,void *stuff)
{
  kno_signal_doexit(signum);
  exit(0);
}

static void siginfo_abort(int signum,siginfo_t *info,void *stuff)
{
  abort();
}

/* Signal handling configs */

static lispval sigmask2dtype(sigset_t *mask)
{
  lispval result = EMPTY;
  int sig = 1; while (sig<32) {
    if (sigismember(mask,sig)) {
      CHOICE_ADD(result,kno_intern(u8_signal_name(sig)));}
    sig++;}
  return result;
}

static int arg2signum(lispval arg)
{
  long long sig = -1;
  if (FIXNUMP(arg))
    sig = FIX2INT(arg);
  else if (SYMBOLP(arg))
    sig = u8_name2signal(SYM_NAME(arg));
  else if (STRINGP(arg))
    sig = u8_name2signal(CSTRING(arg));
  else sig = -1;
  if ((sig>1)&&(sig<32))
    return sig;
  else {
    kno_seterr(kno_TypeError,"arg2signum",NULL,arg);
    return -1;}
}

static lispval sigconfig_getfn(lispval var,void *data)
{
  sigset_t *mask = (sigset_t *)data;
  return sigmask2dtype(mask);
}

static int sigconfig_setfn(lispval var,lispval val,
                           sigset_t *mask,
                           struct sigaction *action,
                           u8_string caller)
{
  int sig = arg2signum(val);
  if (sig<0) return sig;
  if (sigismember(mask,sig))
    return 0;
  else {
    sigaction(sig,action,NULL);
    sigaddset(mask,sig);
    if (mask!= &sigcatch_set) sigdelset(&sigcatch_set,sig);
    if (mask!= &sigexit_set) sigdelset(&sigexit_set,sig);
    if (mask!= &sigabort_set) sigdelset(&sigabort_set,sig);
    if (mask!= &sigdefault_set) sigdelset(&sigdefault_set,sig);
    return 1;}
}

static int sigconfig_catch_setfn(lispval var,lispval val,void *data)
{
  sigset_t *mask = (sigset_t *)data;
  return sigconfig_setfn(var,val,mask,&sigaction_raise,
                         "sigconfig_catch_setfn");
}

static int sigconfig_exit_setfn(lispval var,lispval val,void *data)
{
  sigset_t *mask = (sigset_t *)data;
  return sigconfig_setfn(var,val,mask,&sigaction_exit,
                         "sigconfig_exit_setfn");
}

static int sigconfig_abort_setfn(lispval var,lispval val,void *data)
{
  sigset_t *mask = (sigset_t *)data;
  return sigconfig_setfn(var,val,mask,&sigaction_abort,
                         "sigconfig_abort_setfn");
}
static int sigconfig_default_setfn(lispval var,lispval val,void *data)
{
  sigset_t *mask = (sigset_t *)data;
  return sigconfig_setfn(var,val,mask,&sigaction_default,
                         "sigconfig_default_setfn");
}

void kno_init_signals_c()
{
  u8_register_source_file(_FILEINFO);

  /* Setup sigaction handler */

  memset(&sigaction_raise,0,sizeof(struct sigaction));
  memset(&sigaction_abort,0,sizeof(struct sigaction));
  memset(&sigaction_exit,0,sizeof(struct sigaction));
  memset(&sigaction_default,0,sizeof(struct sigaction));

  sigemptyset(&sigcatch_set);
  sigemptyset(&sigexit_set);
  sigemptyset(&sigabort_set);
  sigemptyset(&sigdefault_set);

  /* Setup sigaction for converting signals to u8_raise (longjmp or exit) */
  sigaction_raise.sa_sigaction = siginfo_raise;
  sigaction_raise.sa_flags = SA_SIGINFO;
  sigemptyset(&(sigaction_raise.sa_mask));

  /* Setup sigaction for default action */
  sigaction_default.sa_handler = SIG_DFL;
  sigemptyset(&(sigaction_default.sa_mask));

  /* Setup sigaction for exit action */
  sigaction_exit.sa_sigaction = siginfo_exit;
  sigaction_exit.sa_flags = SA_SIGINFO;
  sigemptyset(&(sigaction_exit.sa_mask));

  /* Setup sigaction for exit action */
  sigaction_abort.sa_sigaction = siginfo_abort;
  sigaction_abort.sa_flags = SA_SIGINFO;
  sigemptyset(&(sigaction_abort.sa_mask));

  /* Default exit actions */

  sigaddset(&(sigaction_abort.sa_mask),SIGSEGV);
  sigaction(SIGSEGV,&(sigaction_abort),NULL);

  sigaddset(&(sigaction_abort.sa_mask),SIGILL);
  sigaction(SIGILL,&(sigaction_abort),NULL);

  sigaddset(&(sigaction_abort.sa_mask),SIGFPE);
  sigaction(SIGFPE,&(sigaction_abort),NULL);

  sigaddset(&(sigaction_exit.sa_mask),SIGTERM);
  sigaction(SIGTERM,&(sigaction_exit),NULL);

  sigaddset(&(sigaction_abort.sa_mask),SIGQUIT);
  sigaction(SIGQUIT,&(sigaction_abort),NULL);

  sigaddset(&(sigaction_abort.sa_mask),SIGINT);
  sigaction(SIGINT,&(sigaction_abort),NULL);

#ifdef SIGBUS
  sigaddset(&(sigaction_abort.sa_mask),SIGBUS);
  sigaction(SIGBUS,&(sigaction_abort),NULL);
#endif

  /* The default sigmask is masking everything but synchronous
     signals */
  sigfillset(&default_sigmask);
  sigdelset(&default_sigmask,SIGSEGV);
  sigdelset(&default_sigmask,SIGILL);
  sigdelset(&default_sigmask,SIGFPE);
#ifdef SIGBUS
  sigaddset(&default_sigmask,SIGBUS);
#endif

  kno_register_config
    ("SIGRAISE",_("SIGNALS to catch and return as errors"),
     sigconfig_getfn,sigconfig_catch_setfn,
     &sigcatch_set);
  kno_register_config
    ("SIGEXIT",_("Signals to trigger exits"),
     sigconfig_getfn,sigconfig_exit_setfn,
     &sigexit_set);
  kno_register_config
    ("SIGABORT",_("Signals to trigger exits"),
     sigconfig_getfn,sigconfig_abort_setfn,
     &sigabort_set);
  kno_register_config
    ("SIGDEFAULT",_("Signals to trigger exits"),
     sigconfig_getfn,sigconfig_default_setfn,
     &sigexit_set);

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
