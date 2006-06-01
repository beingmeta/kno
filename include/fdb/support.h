/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FDB_SUPPORT_H
#define FDB_SUPPORT_H 1
#define FDB_SUPPORT_H_VERSION "$Id: support.h,v 1.22 2006/01/26 14:44:32 haase Exp $"

FD_EXPORT fd_exception fd_UnknownError, fd_ConfigError, fd_OutOfMemory;

typedef struct FD_CONFIG_HANDLER {
  fdtype var; void *data;
  fdtype (*config_get_method)(fdtype var,void *data);
  int (*config_set_method)(fdtype var,fdtype val,void *data);
  struct FD_CONFIG_HANDLER *next;} FD_CONFIG_HANDLER;
typedef struct FD_CONFIG_HANDLER *fd_config_handler;
  
typedef struct FD_CONFIG_LOOKUPS {
  fdtype (*lookup)(fdtype var);
  struct FD_CONFIG_LOOKUPS *next;} FD_CONFIG_LOOKUPS;
typedef struct FD_CONFIG_LOOKUPS *fd_config_lookups;

FD_EXPORT fdtype fd_config_get(u8_string var);
FD_EXPORT int fd_config_set(u8_string var,fdtype val);
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

FD_EXPORT int fd_config_assignment(u8_string assign_expr);
FD_EXPORT int fd_read_config(u8_input);

FD_EXPORT void fd_set_thread_config(fdtype table);

FD_EXPORT
void fd_register_config_lookup(fdtype (*fn)(fdtype));

/* Error handling */

typedef struct FD_ERRDATA {
  u8_condition cond;
  u8_context cxt;
  u8_string details;
  fdtype irritant;
  struct FD_ERRDATA *next;} FD_ERRDATA;
typedef struct FD_ERRDATA *fd_errdata;

FD_EXPORT void fd_seterr
  (u8_condition c,u8_context cxt,u8_string details,fdtype irritant);
#define fd_seterr3(c,cxt,details) \
   fd_seterr(c,cxt,details,FD_VOID)
#define fd_seterr2(c,cxt) \
   fd_seterr(c,cxt,NULL,FD_VOID)
#define fd_seterr1(c) \
   fd_seterr(c,NULL,NULL,FD_VOID)

FD_EXPORT int fd_geterr
  (u8_condition *c,u8_context *cxt,u8_string *details,fdtype *irritant);
FD_EXPORT int fd_poperr
  (u8_condition *c,u8_context *cxt,u8_string *details,fdtype *irritant);

FD_EXPORT u8_string fd_errstring(struct FD_ERRDATA *);
FD_EXPORT int fd_errout(U8_OUTPUT *,struct FD_ERRDATA *);

FD_EXPORT int fd_reterr
  (u8_condition c,u8_context cxt,u8_string details,fdtype irritant);
FD_EXPORT int fd_interr(fdtype x);
FD_EXPORT fdtype fd_erreify(void);

FD_EXPORT fd_exception fd_retcode_to_exception(fdtype err);

FD_EXPORT int fd_report_errors_atexit;
FD_EXPORT int fd_clear_errors(int);

FD_EXPORT void fd_raise_error(void);

/* Thread vars */

FD_EXPORT fdtype fd_thread_get(fdtype var);
FD_EXPORT fdtype fd_thread_set(fdtype var,fdtype val);

/* File and module recording */

typedef struct FD_SOURCE_FILE_RECORD {
  u8_string filename;
  struct FD_SOURCE_FILE_RECORD *next;} FD_SOURCE_FILE_RECORD;
typedef struct FD_SOURCE_FILE_RECORD *fd_source_file_record;

FD_EXPORT void fd_register_source_file(u8_string s);
FD_EXPORT void fd_for_source_files(void (*f)(u8_string s,void *),void *data);

#endif /* #ifndef FDB_SUPPORT_H */

