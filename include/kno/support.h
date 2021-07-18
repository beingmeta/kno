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

KNO_EXPORT u8_condition kno_UnknownError, kno_ConfigError, kno_OutOfMemory;
KNO_EXPORT u8_condition kno_ReadOnlyConfig;

KNO_EXPORT int kno_trace_config;

KNO_EXPORT int kno_thread_sigint;

#define KNO_CONFIG_ALREADY_MODIFIED 0x01
#define KNO_CONFIG_SINGLE_VALUE	    0x02
#define KNO_CONFIG_DELAYED	    0x04

KNO_EXPORT u8_string kno_logdir, kno_rundir, kno_sharedir, kno_datadir;

typedef lispval (*kno_config_getfn)(lispval var,void *data);
typedef int (*kno_config_setfn)(lispval var,lispval val,void *data);

typedef struct KNO_CONFIG_HANDLER {
  lispval configname;
  void *configdata;
  int configflags;
  u8_string configdoc;
  kno_config_getfn config_get_method;
  kno_config_setfn config_set_method;
  struct KNO_CONFIG_HANDLER *config_next;} KNO_CONFIG_HANDLER;
typedef struct KNO_CONFIG_HANDLER *kno_config_handler;

typedef struct KNO_CONFIG_FINDER {
  lispval (*config_lookup)(lispval var,void *data);
  void *config_lookup_data;
  struct KNO_CONFIG_FINDER *next_lookup;} KNO_CONFIG_FINDER;
typedef struct KNO_CONFIG_FINDER *kno_config_finders;

KNO_EXPORT lispval kno_config_get(u8_string var);
KNO_EXPORT int kno_set_config(u8_string var,lispval val);
KNO_EXPORT int kno_set_config_sym(lispval symbol,lispval val);
KNO_EXPORT int kno_default_config(u8_string var,lispval val);
KNO_EXPORT int kno_default_config_sym(lispval symbol,lispval val);
KNO_EXPORT int kno_set_config_consed(u8_string var,lispval val);
#define kno_config_set(var,val) kno_set_config(var,val)
#define kno_config_set_consed(var,val) kno_set_config_consed(var,val)
#define kno_config_default(var,val) kno_default_config(var,val)

KNO_EXPORT int kno_configs_initialized;

KNO_EXPORT int kno_readonly_config_set(lispval ignored,lispval v,void *p);

KNO_EXPORT lispval kno_interpret_config(lispval value_expr);

KNO_EXPORT int kno_lconfig_push(lispval,lispval v,void *lispp);
KNO_EXPORT int kno_lconfig_add(lispval,lispval v,void *lispp);
KNO_EXPORT int kno_lconfig_set(lispval,lispval v,void *lispp);
KNO_EXPORT lispval kno_lconfig_get(lispval,void *lispp);
KNO_EXPORT int kno_sconfig_set(lispval,lispval v,void *stringptr);
KNO_EXPORT lispval kno_sconfig_get(lispval,void *stringptr);
KNO_EXPORT int kno_intconfig_set(lispval,lispval v,void *intptr);
KNO_EXPORT lispval kno_longconfig_get(lispval,void *intptr);
KNO_EXPORT int kno_longconfig_set(lispval,lispval v,void *intptr);
KNO_EXPORT lispval kno_intconfig_get(lispval,void *intptr);
KNO_EXPORT int kno_sizeconfig_set(lispval,lispval v,void *intptr);
KNO_EXPORT lispval kno_sizeconfig_get(lispval,void *intptr);
KNO_EXPORT int kno_intboolconfig_set(lispval,lispval v,void *intptr);
KNO_EXPORT lispval kno_intboolconfig_get(lispval,void *intptr);
KNO_EXPORT int kno_boolconfig_set(lispval,lispval v,void *intptr);
KNO_EXPORT lispval kno_boolconfig_get(lispval,void *intptr);
KNO_EXPORT int kno_dblconfig_set(lispval,lispval v,void *dblptr);
KNO_EXPORT lispval kno_dblconfig_get(lispval,void *dblptr);
KNO_EXPORT int kno_loglevelconfig_set(lispval var,lispval val,void *data);

KNO_EXPORT int kno_symconfig_set(lispval,lispval v,void *stringptr);

KNO_EXPORT int kno_realpath_config_set(lispval,lispval v,void *stringptr);
KNO_EXPORT int kno_realdir_config_set(lispval,lispval v,void *stringptr);

KNO_EXPORT int kno_tblconfig_set(lispval var,lispval config_val,void *tblptr);
KNO_EXPORT lispval kno_tblconfig_get(lispval var,void *tblptr);

KNO_EXPORT int kno_config_rlimit_set(lispval ignored,lispval v,void *vptr);
KNO_EXPORT lispval kno_config_rlimit_get(lispval ignored,void *vptr);

KNO_EXPORT int kno_config_assignment(u8_string assign_expr);
KNO_EXPORT int kno_default_config_assignment(u8_string assign_expr);
KNO_EXPORT int kno_read_config(u8_input in);
KNO_EXPORT int kno_read_default_config(u8_input in);

KNO_EXPORT int kno_argv_config(int argc,char **argv) U8_DEPRECATED;
KNO_EXPORT lispval *kno_handle_argv(int argc,char **argv,
				  unsigned char *arg_mask,
				  size_t *arglen_ptr);

KNO_EXPORT lispval *kno_argv;
KNO_EXPORT int kno_argc;

KNO_EXPORT
void kno_register_config_lookup(lispval (*fn)(lispval,void *),void *);

KNO_EXPORT int kno_register_config
(u8_string var,u8_string doc,
   lispval (*getfn)(lispval,void *),
   int (*setfn)(lispval,lispval,void *),
   void *data);
KNO_EXPORT int kno_register_config_x
(u8_string var,u8_string doc,
 lispval (*getfn)(lispval,void *),
 int (*setfn)(lispval,lispval,void *),
 void *data,int flags,
 int (*reuse)(struct KNO_CONFIG_HANDLER *scan));
KNO_EXPORT lispval kno_all_configs(int with_docs);

KNO_EXPORT void kno_config_lock(int lock);

KNO_EXPORT int kno_boolstring(u8_string,int);

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

