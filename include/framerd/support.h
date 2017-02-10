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
  fdtype fd_configname; 
  void *fd_configdata; 
  int fd_configflags; 
  u8_string fd_configdoc;
  fdtype (*fd_config_get_method)(fdtype var,void *data);
  int (*fd_config_set_method)(fdtype var,fdtype val,void *data);
  struct FD_CONFIG_HANDLER *fd_nextconfig;} FD_CONFIG_HANDLER;
typedef struct FD_CONFIG_HANDLER *fd_config_handler;

typedef struct FD_CONFIG_FINDER {
  fdtype (*fdcfg_lookup)(fdtype var,void *data);
  void *fdcfg_lookup_data;
  struct FD_CONFIG_FINDER *fd_next_finder;} FD_CONFIG_FINDER;
typedef struct FD_CONFIG_FINDER *fd_config_finders;

FD_EXPORT fdtype fd_config_get(u8_string var);
FD_EXPORT int fd_set_config(u8_string var,fdtype val);
FD_EXPORT int fd_default_config(u8_string var,fdtype val);
FD_EXPORT int fd_set_config_consed(u8_string var,fdtype val);
#define fd_config_set(var,val) fd_set_config(var,val)
#define fd_config_set_consed(var,val) fd_set_config_consed(var,val)
#define fd_default_config(var,val) fd_default_config(var,val)

FD_EXPORT int fd_readonly_config_set(fdtype ignored,fdtype v,void *p);

FD_EXPORT int fd_lconfig_push(fdtype,fdtype v,void *lispp);
FD_EXPORT int fd_lconfig_add(fdtype,fdtype v,void *lispp);
FD_EXPORT int fd_lconfig_set(fdtype,fdtype v,void *lispp);
FD_EXPORT fdtype fd_lconfig_get(fdtype,void *lispp);
FD_EXPORT int fd_sconfig_set(fdtype,fdtype v,void *stringptr);
FD_EXPORT fdtype fd_sconfig_get(fdtype,void *stringptr);
FD_EXPORT int fd_intconfig_set(fdtype,fdtype v,void *intptr);
FD_EXPORT fdtype fd_intconfig_get(fdtype,void *intptr);
FD_EXPORT int fd_sizeconfig_set(fdtype,fdtype v,void *intptr);
FD_EXPORT fdtype fd_sizeconfig_get(fdtype,void *intptr);
FD_EXPORT int fd_boolconfig_set(fdtype,fdtype v,void *intptr);
FD_EXPORT fdtype fd_boolconfig_get(fdtype,void *intptr);
FD_EXPORT int fd_dblconfig_set(fdtype,fdtype v,void *dblptr);
FD_EXPORT fdtype fd_dblconfig_get(fdtype,void *dblptr);
FD_EXPORT int fd_loglevelconfig_set(fdtype var,fdtype val,void *data);

FD_EXPORT int fd_config_assignment(u8_string assign_expr);
FD_EXPORT int fd_default_config_assignment(u8_string assign_expr);
FD_EXPORT int fd_read_config(u8_input in);
FD_EXPORT int fd_read_default_config(u8_input in);

FD_EXPORT int fd_argv_config(int argc,char **argv) U8_DEPRECATED;
FD_EXPORT fdtype *fd_handle_argv(int argc,char **argv,
				 unsigned int parse_mask,
				 size_t *arglen_ptr);

FD_EXPORT fdtype *fd_argv;
FD_EXPORT int fd_argc;

FD_EXPORT void fd_set_thread_config(fdtype table);

FD_EXPORT
void fd_register_config_lookup(fdtype (*fn)(fdtype,void *),void *);

FD_EXPORT int fd_register_config
(u8_string var,u8_string doc,
   fdtype (*getfn)(fdtype,void *),
   int (*setfn)(fdtype,fdtype,void *),
   void *data);
FD_EXPORT int fd_register_config_x
(u8_string var,u8_string doc,
   fdtype (*getfn)(fdtype,void *),
   int (*setfn)(fdtype,fdtype,void *),
   void *data,int (*reuse)(struct FD_CONFIG_HANDLER *scan));
FD_EXPORT fdtype fd_all_configs(int with_docs);

FD_EXPORT void fd_config_lock(int lock);

FD_EXPORT int fd_boolstring(u8_string,int);

/* Error handling */

FD_EXPORT void fd_free_exception_xdata(void *ptr);

FD_EXPORT fdtype fd_err(fd_exception,u8_context,u8_string,fdtype);
FD_EXPORT void fd_push_error_context(u8_context cxt,fdtype data);

FD_EXPORT fdtype fd_type_error(u8_string,u8_context,fdtype);

FD_EXPORT void fd_print_exception(U8_OUTPUT *out,u8_exception e);
FD_EXPORT void fd_sum_exception(U8_OUTPUT *out,u8_exception e);
FD_EXPORT u8_string fd_errstring(u8_exception e);
FD_EXPORT fdtype fd_exception_xdata(u8_exception e);

FD_EXPORT U8_NOINLINE void fd_seterr
  (u8_condition c,u8_context cxt,u8_string details,fdtype irritant);

#define fd_seterr3(c,cxt,details) \
   fd_seterr(c,cxt,details,FD_VOID)
#define fd_seterr2(c,cxt) \
   fd_seterr(c,cxt,NULL,FD_VOID)
#define fd_seterr1(c) \
   fd_seterr(c,NULL,NULL,FD_VOID)

FD_EXPORT void fd_set_type_error(u8_string type_name,fdtype irritant);

FD_EXPORT int fd_geterr
  (u8_condition *c,u8_context *cxt,u8_string *details,fdtype *irritant);
FD_EXPORT int fd_poperr
  (u8_condition *c,u8_context *cxt,u8_string *details,fdtype *irritant);


FD_EXPORT int fd_reterr
  (u8_condition c,u8_context cxt,u8_string details,fdtype irritant);
FD_EXPORT int fd_interr(fdtype x);
FD_EXPORT fdtype fd_erreify(void);

FD_EXPORT fd_exception fd_retcode_to_exception(fdtype err);

FD_EXPORT fdtype fd_exception_backtrace(u8_exception ex);

/* Whether the executable is exiting */
FD_EXPORT int fd_exiting;
FD_EXPORT int fd_report_errors_atexit;
FD_EXPORT int fd_clear_errors(int);
FD_EXPORT void fd_log_exception(u8_exception ex);
FD_EXPORT void fd_doexit(fdtype);

/* Thread vars */

FD_EXPORT fdtype fd_thread_get(fdtype var);
FD_EXPORT void fd_reset_threadvars(void);
FD_EXPORT int fd_thread_set(fdtype var,fdtype val);
FD_EXPORT int fd_thread_add(fdtype var,fdtype val);

/* Request state */

typedef fdtype (*fd_reqfn)(fdtype);

FD_EXPORT fdtype fd_req(fdtype var);
FD_EXPORT fdtype fd_req_get(fdtype var,fdtype dflt);
FD_EXPORT int fd_req_store(fdtype var,fdtype val);
FD_EXPORT int fd_req_test(fdtype var,fdtype val);
FD_EXPORT int fd_req_add(fdtype var,fdtype val);
FD_EXPORT int fd_req_drop(fdtype var,fdtype val);
FD_EXPORT fdtype fd_req_call(fd_reqfn reqfn);
FD_EXPORT int fd_req_push(fdtype var,fdtype val);
FD_EXPORT void fd_use_reqinfo(fdtype reqinfo);
FD_EXPORT fdtype fd_push_reqinfo(fdtype reqinfo);
FD_EXPORT int fd_isreqlive(void);

FD_EXPORT struct U8_OUTPUT *fd_reqlog(int force);
FD_EXPORT int fd_reqlogger(u8_condition,u8_context,u8_string);

/* Sets the application identifier and runbase */

FD_EXPORT void fd_setapp(u8_string spec,u8_string dir);

/* Runbase */

FD_EXPORT u8_string fd_runbase_filename(u8_string suffix);

/* Handling options */

FD_EXPORT fdtype fd_getopt(fdtype opts,fdtype key,fdtype dflt);
FD_EXPORT int fd_testopt(fdtype opts,fdtype key,fdtype val);

/* Initializing the stack */

FD_EXPORT ssize_t fd_default_stack_limit;
FD_EXPORT ssize_t fd_init_stack(void);

#define FD_INIT_STACK() \
  U8_SET_STACK_BASE();	\
  fd_init_stack()

/* Signalling */

FD_EXPORT struct sigaction *fd_sigaction_catch;
FD_EXPORT struct sigaction *fd_sigaction_exit;
FD_EXPORT struct sigaction *fd_sigaction_default;

FD_EXPORT sigset_t *fd_default_sigmask;

#endif /* #ifndef FRAMERD_SUPPORT_H */

