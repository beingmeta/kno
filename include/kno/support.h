/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef KNO_SUPPORT_H
#define KNO_SUPPORT_H 1
#ifndef KNO_SUPPORT_H_INFO
#define KNO_SUPPORT_H_INFO "include/kno/support.h"
#endif

#include <signal.h>

KNO_EXPORT u8_condition kno_UnknownError, kno_OutOfMemory;
KNO_EXPORT int kno_thread_sigint;

KNO_EXPORT u8_string kno_logdir, kno_rundir, kno_sharedir, kno_datadir;

#define KNO_ARGV_PARSE  0
#define KNO_ARGV_CONFIG 1
#define KNO_ARGV_IGNORE 2

KNO_EXPORT int kno_argv_config(int argc,char **argv) U8_DEPRECATED;
KNO_EXPORT lispval *kno_handle_argv(int argc,char **argv,
				  unsigned char *arg_mask,
				  size_t *arglen_ptr);

KNO_EXPORT lispval *kno_argv;
KNO_EXPORT int kno_argc;

/* Whether the executable is exiting or has exited */
KNO_EXPORT int kno_exiting;
KNO_EXPORT int kno_exited;

/* Whether to log command arguments */
KNO_EXPORT int kno_logcmd;

/* Whether to 'fast exit' by skipping some memory release, etc when
   exiting */
KNO_EXPORT int kno_fast_exit;
/* Whether to report an error conditions on exit */
KNO_EXPORT int kno_report_errors_atexit;

KNO_EXPORT u8_string kno_exe_name;

KNO_EXPORT int kno_boolstring(u8_string,int);

KNO_EXPORT void kno_doexit(lispval);
KNO_EXPORT void kno_signal_doexit(int);

KNO_EXPORT int kno_clear_errors(int);
KNO_EXPORT void kno_log_exception(u8_exception ex);
KNO_EXPORT int kno_pop_exceptions(u8_exception restore,int loglevel);

KNO_EXPORT lispval kno_thread_get(lispval var);
KNO_EXPORT int kno_thread_probe(lispval var);
KNO_EXPORT lispval kno_init_threadtable(lispval);
KNO_EXPORT void kno_reset_threadvars(void);
KNO_EXPORT int kno_thread_set(lispval var,lispval val);
KNO_EXPORT int kno_thread_add(lispval var,lispval val);

/* Request state */

typedef lispval (*kno_reqfn)(lispval);

KNO_EXPORT lispval kno_req(lispval var);
KNO_EXPORT lispval kno_req_get(lispval var,lispval dflt);
KNO_EXPORT int kno_req_store(lispval var,lispval val);
KNO_EXPORT int kno_req_test(lispval var,lispval val);
KNO_EXPORT int kno_req_add(lispval var,lispval val);
KNO_EXPORT int kno_req_drop(lispval var,lispval val);
KNO_EXPORT lispval kno_req_call(kno_reqfn reqfn);
KNO_EXPORT int kno_req_push(lispval var,lispval val);
KNO_EXPORT void kno_use_reqinfo(lispval reqinfo);
KNO_EXPORT lispval kno_push_reqinfo(lispval reqinfo);
KNO_EXPORT int kno_isreqlive(void);

KNO_EXPORT struct U8_OUTPUT *kno_reqlog(int force);
KNO_EXPORT int kno_reqlogger(u8_condition,u8_context,u8_string);

KNO_EXPORT struct U8_OUTPUT *kno_get_reqlog(void);
KNO_EXPORT struct U8_OUTPUT *kno_try_reqlog(void);

/* Sets the application identifier and runbase */

KNO_EXPORT void kno_setapp(u8_string spec,u8_string dir);

/* Runbase */

KNO_EXPORT u8_string kno_runbase_filename(u8_string suffix);

/* Handling options */

#define KNO_OPTIONSP(opts) \
  ( (KNO_SLOTMAPP(opts)) ||  (KNO_SCHEMAPP(opts)) || (KNO_PAIRP(opts)) )

KNO_EXPORT lispval kno_getopt(lispval opts,lispval key,lispval dflt);
KNO_EXPORT int kno_testopt(lispval opts,lispval key,lispval val);
KNO_EXPORT long long kno_getfixopt(lispval opts,u8_string name,long long dflt);

#if KNO_DEFINE_GETOPT
#define getopt(o,k,d) \
  ( (KNO_FALSEP(o)) ? (kno_incref(d)) : (kno_getopt(o,k,d)) )
#endif

KNO_EXPORT lispval kno_merge_opts(lispval head,lispval tail);

#define kno_hasopt(opts,key) (kno_testopt((opts),(key),(KNO_VOID)))

/* Resources sensors */

typedef lispval (*kno_resource_sensor)(void);

KNO_EXPORT int kno_add_sensor(lispval name,kno_resource_sensor fn);
KNO_EXPORT lispval kno_read_sensor(lispval name);
KNO_EXPORT lispval kno_read_sensors(lispval into);

/* Getting the C config table */

KNO_EXPORT lispval kno_get_build_info(void);

/* Signalling */

KNO_EXPORT struct sigaction *kno_sigaction_raise;
KNO_EXPORT struct sigaction *kno_sigaction_exit;
KNO_EXPORT struct sigaction *kno_sigaction_abort;
KNO_EXPORT struct sigaction *kno_sigaction_default;
KNO_EXPORT struct sigaction *kno_sigaction_ignore;
KNO_EXPORT struct sigaction *kno_sigaction_status;
KNO_EXPORT struct sigaction *kno_sigaction_stack;

KNO_EXPORT sigset_t *kno_default_sigmask;

/* Sourcebase functions */

KNO_EXPORT u8_string kno_sourcebase();
KNO_EXPORT u8_string kno_get_component(u8_string spec);
KNO_EXPORT u8_string kno_bind_sourcebase(u8_string sourcebase);
KNO_EXPORT void kno_restore_sourcebase(u8_string sourcebase);

/* Checking errnos */

#if KNO_PROFILING
#define KNO_CHECK_ERRNO(action,cxt)
#define KNO_CHECK_ERRNO_OBJ(obj,cxt)
#else
#define KNO_CHECK_ERRNO(action,cxt)		\
  if (errno) {					\
    int errnum = errno; errno = 0;		 \
    u8_log(LOG_WARN,u8_UnexpectedErrno,		 \
	   "Dangling errno value %d (%s) %s %s", \
	   errnum,u8_strerror(errnum),cxt,action);}
#define KNO_CHECK_ERRNO_OBJ(obj,cxt)		 \
  if (errno) {					\
    int errnum = errno; errno = 0;		 \
    u8_log(LOG_WARN,u8_UnexpectedErrno,		 \
	   "Dangling errno value %d (%s) %s %q", \
	   errnum,u8_strerror(errnum),cxt,obj);}
#endif

#endif /* #ifndef KNO_SUPPORT_H */

