/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_SUPPORT_H
#define FRAMERD_SUPPORT_H 1
#ifndef FRAMERD_SUPPORT_H_INFO
#define FRAMERD_SUPPORT_H_INFO "include/framerd/support.h"
#endif

#include <signal.h>

FD_EXPORT fd_exception fd_UnknownError, fd_ConfigError, fd_OutOfMemory;
FD_EXPORT fd_exception fd_ReadOnlyConfig;

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
#define fd_default_config(var,val) fd_default_config(var,val)

FD_EXPORT int fd_readonly_config_set(lispval ignored,lispval v,void *p);

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

FD_EXPORT int fd_config_rlimit_set(lispval ignored,lispval v,void *vptr);
FD_EXPORT lispval fd_config_rlimit_get(lispval ignored,void *vptr);

FD_EXPORT int fd_config_assignment(u8_string assign_expr);
FD_EXPORT int fd_default_config_assignment(u8_string assign_expr);
FD_EXPORT int fd_read_config(u8_input in);
FD_EXPORT int fd_read_default_config(u8_input in);

FD_EXPORT int fd_argv_config(int argc,char **argv) U8_DEPRECATED;
FD_EXPORT lispval *fd_handle_argv(int argc,char **argv,
				 unsigned int parse_mask,
				 size_t *arglen_ptr);

FD_EXPORT lispval *fd_argv;
FD_EXPORT int fd_argc;

FD_EXPORT void fd_set_thread_config(lispval table);

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

/* Error handling */

FD_EXPORT void fd_free_exception_xdata(void *ptr);

FD_EXPORT lispval fd_err(fd_exception,u8_context,u8_string,lispval);
FD_EXPORT void fd_graberr(int,u8_context cxt,u8_string details);

FD_EXPORT lispval fd_type_error(u8_string,u8_context,lispval);

FD_EXPORT void fd_sum_exception(U8_OUTPUT *out,u8_exception e);
FD_EXPORT u8_string fd_errstring(u8_exception e);
FD_EXPORT lispval fd_exception_xdata(u8_exception e);

FD_EXPORT void fd_print_exception(U8_OUTPUT *out,u8_exception e);
FD_EXPORT void fd_log_exception(u8_exception ex);
FD_EXPORT void fd_output_exception(u8_output out,u8_exception ex);
FD_EXPORT void fd_output_errstack(u8_output out,u8_exception ex);
FD_EXPORT void fd_log_errstack(u8_exception ex,int loglevel,int w_irritant);
FD_EXPORT lispval fd_exception_backtrace(u8_exception ex);

FD_EXPORT U8_NOINLINE void fd_seterr
  (u8_condition c,u8_context cxt,u8_string details,lispval irritant);
FD_EXPORT U8_NOINLINE void fd_pusherr
  (u8_condition c,u8_context cxt,u8_string details,lispval irritant);
FD_EXPORT U8_NOINLINE void fd_raise
  (u8_condition c,u8_context cxt,u8_string details,lispval irritant);

FD_EXPORT lispval fd_get_irritant(u8_exception ex);

FD_EXPORT int fd_stacktracep(lispval rep);

#define fd_seterr3(c,cxt,details) \
   fd_seterr(c,cxt,details,FD_VOID)
#define fd_seterr2(c,cxt) \
   fd_seterr(c,cxt,NULL,FD_VOID)
#define fd_seterr1(c) \
   fd_seterr(c,NULL,NULL,FD_VOID)

FD_EXPORT void fd_set_type_error(u8_string type_name,lispval irritant);

FD_EXPORT int fd_geterr
  (u8_condition *c,u8_context *cxt,u8_string *details,lispval *irritant);
FD_EXPORT int fd_poperr
  (u8_condition *c,u8_context *cxt,u8_string *details,lispval *irritant);


FD_EXPORT int fd_reterr
  (u8_condition c,u8_context cxt,u8_string details,lispval irritant);
FD_EXPORT int fd_interr(lispval x);
FD_EXPORT lispval fd_erreify(void);

FD_EXPORT fd_exception fd_retcode_to_exception(lispval err);

FD_EXPORT lispval fd_exception_backtrace(u8_exception ex);

/* Whether the executable is exiting */
FD_EXPORT int fd_exiting;
FD_EXPORT int fd_exited;
FD_EXPORT int fd_report_errors_atexit;
FD_EXPORT int fd_clear_errors(int);
FD_EXPORT void fd_log_exception(u8_exception ex);
FD_EXPORT void fd_doexit(lispval);
FD_EXPORT void fd_signal_doexit(int);

/* Thread vars */

FD_EXPORT lispval fd_thread_get(lispval var);
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

FD_EXPORT lispval fd_getopt(lispval opts,lispval key,lispval dflt);
FD_EXPORT int fd_testopt(lispval opts,lispval key,lispval val);
FD_EXPORT long long fd_fixopt(lispval opts,u8_string name,int dflt);

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

#endif /* #ifndef FRAMERD_SUPPORT_H */

