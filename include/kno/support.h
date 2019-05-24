/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
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

#define KNO_CONFIG_ALREADY_MODIFIED 1

typedef struct KNO_CONFIG_HANDLER {
  lispval configname;
  void *configdata;
  int configflags;
  u8_string configdoc;
  lispval (*config_get_method)(lispval var,void *data);
  int (*config_set_method)(lispval var,lispval val,void *data);
  struct KNO_CONFIG_HANDLER *config_next;} KNO_CONFIG_HANDLER;
typedef struct KNO_CONFIG_HANDLER *kno_config_handler;

typedef struct KNO_CONFIG_FINDER {
  lispval (*config_lookup)(lispval var,void *data);
  void *config_lookup_data;
  struct KNO_CONFIG_FINDER *next_lookup;} KNO_CONFIG_FINDER;
typedef struct KNO_CONFIG_FINDER *kno_config_finders;

KNO_EXPORT lispval kno_config_get(u8_string var);
KNO_EXPORT int kno_set_config(u8_string var,lispval val);
KNO_EXPORT int kno_default_config(u8_string var,lispval val);
KNO_EXPORT int kno_set_config_consed(u8_string var,lispval val);
#define kno_config_set(var,val) kno_set_config(var,val)
#define kno_config_set_consed(var,val) kno_set_config_consed(var,val)
#define kno_config_default(var,val) kno_default_config(var,val)

KNO_EXPORT int kno_readonly_config_set(lispval ignored,lispval v,void *p);

KNO_EXPORT lispval kno_interpret_value(lispval value_expr);

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
   void *data,int (*reuse)(struct KNO_CONFIG_HANDLER *scan));
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

KNO_EXPORT void kno_doexit(lispval);
KNO_EXPORT void kno_signal_doexit(int);

KNO_EXPORT int kno_clear_errors(int);
KNO_EXPORT void kno_log_exception(u8_exception ex);

KNO_EXPORT lispval kno_thread_get(lispval var);
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

KNO_EXPORT sigset_t *kno_default_sigmask;

/* Sourcebase functions */

KNO_EXPORT u8_string kno_sourcebase();
KNO_EXPORT u8_string kno_get_component(u8_string spec);
KNO_EXPORT u8_string kno_bind_sourcebase(u8_string sourcebase);
KNO_EXPORT void kno_restore_sourcebase(u8_string sourcebase);

#endif /* #ifndef KNO_SUPPORT_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/