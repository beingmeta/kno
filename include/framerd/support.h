/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_SUPPORT_H
#define FRAMERD_SUPPORT_H 1
#ifndef FRAMERD_SUPPORT_H_INFO
#define FRAMERD_SUPPORT_H_INFO "include/framerd/support.h"
#endif

#include <signal.h>

FD_EXPORT u8_condition fd_UnknownError, fd_ConfigError, fd_OutOfMemory;
FD_EXPORT u8_condition fd_ReadOnlyConfig;

FD_EXPORT int fd_trace_config;

#define FD_CONFIG_ALREADY_MODIFIED 1

typedef struct FD_CONFIG_HANDLER {
  lispval configname;
  void *configdata;
  int configflags;
  u8_string configdoc;
  lispval (*config_get_method)(lispval var,void *data);
  int (*config_set_method)(lispval var,lispval val,void *data);
  struct FD_CONFIG_HANDLER *config_next;} FD_CONFIG_HANDLER;
typedef struct FD_CONFIG_HANDLER *fd_config_handler;

typedef struct FD_CONFIG_FINDER {
  lispval (*config_lookup)(lispval var,void *data);
  void *config_lookup_data;
  struct FD_CONFIG_FINDER *next_lookup;} FD_CONFIG_FINDER;
typedef struct FD_CONFIG_FINDER *fd_config_finders;

FD_EXPORT lispval fd_config_get(u8_string var);
FD_EXPORT int fd_set_config(u8_string var,lispval val);
FD_EXPORT int fd_default_config(u8_string var,lispval val);
FD_EXPORT int fd_set_config_consed(u8_string var,lispval val);
#define fd_config_set(var,val) fd_set_config(var,val)
#define fd_config_set_consed(var,val) fd_set_config_consed(var,val)
#define fd_config_default(var,val) fd_default_config(var,val)

FD_EXPORT int fd_readonly_config_set(lispval ignored,lispval v,void *p);

FD_EXPORT lispval fd_interpret_value(lispval value_expr);

FD_EXPORT int fd_lconfig_push(lispval,lispval v,void *lispp);
FD_EXPORT int fd_lconfig_add(lispval,lispval v,void *lispp);
FD_EXPORT int fd_lconfig_set(lispval,lispval v,void *lispp);
FD_EXPORT lispval fd_lconfig_get(lispval,void *lispp);
FD_EXPORT int fd_sconfig_set(lispval,lispval v,void *stringptr);
FD_EXPORT lispval fd_sconfig_get(lispval,void *stringptr);
FD_EXPORT int fd_intconfig_set(lispval,lispval v,void *intptr);
FD_EXPORT lispval fd_longconfig_get(lispval,void *intptr);
FD_EXPORT int fd_longconfig_set(lispval,lispval v,void *intptr);
FD_EXPORT lispval fd_intconfig_get(lispval,void *intptr);
FD_EXPORT int fd_sizeconfig_set(lispval,lispval v,void *intptr);
FD_EXPORT lispval fd_sizeconfig_get(lispval,void *intptr);
FD_EXPORT int fd_boolconfig_set(lispval,lispval v,void *intptr);
FD_EXPORT lispval fd_boolconfig_get(lispval,void *intptr);
FD_EXPORT int fd_dblconfig_set(lispval,lispval v,void *dblptr);
FD_EXPORT lispval fd_dblconfig_get(lispval,void *dblptr);
FD_EXPORT int fd_loglevelconfig_set(lispval var,lispval val,void *data);

FD_EXPORT int fd_symconfig_set(lispval,lispval v,void *stringptr);

FD_EXPORT int fd_realpath_config_set(lispval,lispval v,void *stringptr);
FD_EXPORT int fd_realdir_config_set(lispval,lispval v,void *stringptr);

FD_EXPORT int fd_tblconfig_set(lispval var,lispval config_val,void *tblptr);
FD_EXPORT lispval fd_tblconfig_get(lispval var,void *tblptr);

FD_EXPORT int fd_config_rlimit_set(lispval ignored,lispval v,void *vptr);
FD_EXPORT lispval fd_config_rlimit_get(lispval ignored,void *vptr);

FD_EXPORT int fd_config_assignment(u8_string assign_expr);
FD_EXPORT int fd_default_config_assignment(u8_string assign_expr);
FD_EXPORT int fd_read_config(u8_input in);
FD_EXPORT int fd_read_default_config(u8_input in);

FD_EXPORT int fd_argv_config(int argc,char **argv) U8_DEPRECATED;
FD_EXPORT lispval *fd_handle_argv(int argc,char **argv,
                                  unsigned char *arg_mask,
                                  size_t *arglen_ptr);

FD_EXPORT lispval *fd_argv;
FD_EXPORT int fd_argc;

FD_EXPORT
void fd_register_config_lookup(lispval (*fn)(lispval,void *),void *);

FD_EXPORT int fd_register_config
(u8_string var,u8_string doc,
   lispval (*getfn)(lispval,void *),
   int (*setfn)(lispval,lispval,void *),
   void *data);
FD_EXPORT int fd_register_config_x
(u8_string var,u8_string doc,
   lispval (*getfn)(lispval,void *),
   int (*setfn)(lispval,lispval,void *),
   void *data,int (*reuse)(struct FD_CONFIG_HANDLER *scan));
FD_EXPORT lispval fd_all_configs(int with_docs);

FD_EXPORT void fd_config_lock(int lock);

FD_EXPORT int fd_boolstring(u8_string,int);

/* Whether the executable is exiting or has exited */
FD_EXPORT int fd_exiting;
FD_EXPORT int fd_exited;

/* Whether to log command arguments */
FD_EXPORT int fd_logcmd;

/* Whether to 'fast exit' by skipping some memory release, etc when
   exiting */
FD_EXPORT int fd_fast_exit;
/* Whether to report an error conditions on exit */
FD_EXPORT int fd_report_errors_atexit;

FD_EXPORT void fd_doexit(lispval);
FD_EXPORT void fd_signal_doexit(int);

FD_EXPORT int fd_clear_errors(int);
FD_EXPORT void fd_log_exception(u8_exception ex);

FD_EXPORT lispval fd_thread_get(lispval var);
FD_EXPORT lispval fd_init_threadtable(lispval);
FD_EXPORT void fd_reset_threadvars(void);
FD_EXPORT int fd_thread_set(lispval var,lispval val);
FD_EXPORT int fd_thread_add(lispval var,lispval val);

/* Request state */

typedef lispval (*fd_reqfn)(lispval);

FD_EXPORT lispval fd_req(lispval var);
FD_EXPORT lispval fd_req_get(lispval var,lispval dflt);
FD_EXPORT int fd_req_store(lispval var,lispval val);
FD_EXPORT int fd_req_test(lispval var,lispval val);
FD_EXPORT int fd_req_add(lispval var,lispval val);
FD_EXPORT int fd_req_drop(lispval var,lispval val);
FD_EXPORT lispval fd_req_call(fd_reqfn reqfn);
FD_EXPORT int fd_req_push(lispval var,lispval val);
FD_EXPORT void fd_use_reqinfo(lispval reqinfo);
FD_EXPORT lispval fd_push_reqinfo(lispval reqinfo);
FD_EXPORT int fd_isreqlive(void);

FD_EXPORT struct U8_OUTPUT *fd_reqlog(int force);
FD_EXPORT int fd_reqlogger(u8_condition,u8_context,u8_string);

FD_EXPORT struct U8_OUTPUT *fd_get_reqlog(void);
FD_EXPORT struct U8_OUTPUT *fd_try_reqlog(void);

/* Sets the application identifier and runbase */

FD_EXPORT void fd_setapp(u8_string spec,u8_string dir);

/* Runbase */

FD_EXPORT u8_string fd_runbase_filename(u8_string suffix);

/* Handling options */

#define FD_OPTIONSP(opts) \
  ( (FD_SLOTMAPP(opts)) ||  (FD_SCHEMAPP(opts)) || (FD_PAIRP(opts)) )

FD_EXPORT lispval fd_getopt(lispval opts,lispval key,lispval dflt);
FD_EXPORT int fd_testopt(lispval opts,lispval key,lispval val);
FD_EXPORT long long fd_getfixopt(lispval opts,u8_string name,long long dflt);

/* Resources sensors */

typedef lispval (*fd_resource_sensor)(void);

FD_EXPORT int fd_add_sensor(lispval name,fd_resource_sensor fn);
FD_EXPORT lispval fd_read_sensor(lispval name);
FD_EXPORT lispval fd_read_sensors(lispval into);

/* Getting the C config table */

FD_EXPORT lispval fd_get_build_info(void);

/* Signalling */

FD_EXPORT struct sigaction *fd_sigaction_raise;
FD_EXPORT struct sigaction *fd_sigaction_exit;
FD_EXPORT struct sigaction *fd_sigaction_abort;
FD_EXPORT struct sigaction *fd_sigaction_default;

FD_EXPORT sigset_t *fd_default_sigmask;

/* Sourcebase functions */

FD_EXPORT u8_string fd_sourcebase();
FD_EXPORT u8_string fd_get_component(u8_string spec);
FD_EXPORT u8_string fd_bind_sourcebase(u8_string sourcebase);
FD_EXPORT void fd_restore_sourcebase(u8_string sourcebase);

#endif /* #ifndef FRAMERD_SUPPORT_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
