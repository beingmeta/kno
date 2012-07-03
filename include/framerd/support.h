/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2012 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FDB_SUPPORT_H
#define FDB_SUPPORT_H 1
#ifndef FDB_SUPPORT_H_INFO
#define FDB_SUPPORT_H_INFO "include/framerd/support.h"
#endif

FD_EXPORT fd_exception fd_UnknownError, fd_ConfigError, fd_OutOfMemory;

#define FD_CONFIG_ALREADY_MODIFIED 1

typedef struct FD_CONFIG_HANDLER {
  fdtype var; void *data; int flags; u8_string doc;
  fdtype (*config_get_method)(fdtype var,void *data);
  int (*config_set_method)(fdtype var,fdtype val,void *data);
  struct FD_CONFIG_HANDLER *next;} FD_CONFIG_HANDLER;
typedef struct FD_CONFIG_HANDLER *fd_config_handler;
  
typedef struct FD_CONFIG_LOOKUPS {
  fdtype (*fdcfg_lookup)(fdtype var,void *data);
  void *fdcfg_lookup_data;
  struct FD_CONFIG_LOOKUPS *next;} FD_CONFIG_LOOKUPS;
typedef struct FD_CONFIG_LOOKUPS *fd_config_lookups;

FD_EXPORT fdtype fd_config_get(u8_string var);
FD_EXPORT int fd_config_set(u8_string var,fdtype val);
FD_EXPORT int fd_config_default(u8_string var,fdtype val);
FD_EXPORT int fd_config_set_consed(u8_string var,fdtype val);

FD_EXPORT int fd_lconfig_push(fdtype ignored,fdtype v,void *lispp);
FD_EXPORT int fd_lconfig_add(fdtype ignored,fdtype v,void *lispp);
FD_EXPORT int fd_lconfig_set(fdtype ignored,fdtype v,void *lispp);
FD_EXPORT fdtype fd_lconfig_get(fdtype ignored,void *lispp);
FD_EXPORT int fd_sconfig_set(fdtype ignored,fdtype v,void *stringptr);
FD_EXPORT fdtype fd_sconfig_get(fdtype ignored,void *stringptr);
FD_EXPORT int fd_intconfig_set(fdtype ignored,fdtype v,void *intptr);
FD_EXPORT fdtype fd_intconfig_get(fdtype ignored,void *intptr);
FD_EXPORT int fd_boolconfig_set(fdtype ignored,fdtype v,void *intptr);
FD_EXPORT fdtype fd_boolconfig_get(fdtype ignored,void *intptr);
FD_EXPORT int fd_dblconfig_set(fdtype ignored,fdtype v,void *dblptr);
FD_EXPORT fdtype fd_dblconfig_get(fdtype ignored,void *dblptr);

FD_EXPORT int fd_config_assignment(u8_string assign_expr);
FD_EXPORT int fd_argv_config(int argc,char **argv);
FD_EXPORT int fd_read_config(u8_input in);

FD_EXPORT void fd_set_thread_config(fdtype table);

FD_EXPORT
void fd_register_config_lookup(fdtype (*fn)(fdtype,void *),void *);

FD_EXPORT int fd_register_config
(u8_string var,u8_string doc,
   fdtype (*getfn)(fdtype,void *),
   int (*setfn)(fdtype,fdtype,void *),
   void *data);


FD_EXPORT void fd_config_lock(int lock);

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

FD_EXPORT int fd_report_errors_atexit;
FD_EXPORT int fd_clear_errors(int);

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

/* Runbase */

FD_EXPORT u8_string fd_runbase_filename(u8_string suffix);

/* File and module recording */

typedef struct FD_SOURCE_FILE_RECORD {
  u8_string filename;
  struct FD_SOURCE_FILE_RECORD *next;} FD_SOURCE_FILE_RECORD;
typedef struct FD_SOURCE_FILE_RECORD *fd_source_file_record;

FD_EXPORT void fd_register_source_file(u8_string s);
FD_EXPORT void fd_for_source_files(void (*f)(u8_string s,void *),void *data);

/* Handling options */

FD_EXPORT fdtype fd_getopt(fdtype opts,fdtype key,fdtype dflt);
FD_EXPORT int fd_testopt(fdtype opts,fdtype key,fdtype val);


#endif /* #ifndef FDB_SUPPORT_H */

