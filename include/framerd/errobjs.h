/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_ERROBJS_H
#define FRAMERD_ERROBJS_H 1
#ifndef FRAMERD_ERROBJS_H_INFO
#define FRAMERD_ERROBJS_H_INFO "include/framerd/errobjs.h"
#endif

/* Error handling */

FD_EXPORT lispval fd_wrap_exception(u8_exception ex);

FD_EXPORT void fd_decref_u8x_xdata(void *ptr);
FD_EXPORT void fd_decref_embedded_exception(void *ptr);

FD_EXPORT lispval fd_raw_irritant(u8_exception ex);
FD_EXPORT lispval fd_get_irritant(u8_exception ex);
FD_EXPORT lispval fd_get_exception(u8_exception ex);
FD_EXPORT struct FD_EXCEPTION *fd_exception_object(u8_exception ex);

FD_EXPORT lispval fd_err(u8_condition,u8_context,u8_string,lispval);

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

FD_EXPORT void fd_compact_backtrace(u8_output out,lispval stack,int limit);

FD_EXPORT U8_NOINLINE void fd_seterr
  (u8_condition c,u8_context cxt,u8_string details,lispval irritant);
FD_EXPORT U8_NOINLINE void fd_raise
  (u8_condition c,u8_context cxt,u8_string details,lispval irritant);

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

FD_EXPORT u8_condition fd_retcode_to_exception(lispval err);

FD_EXPORT lispval fd_exception_backtrace(u8_exception ex);

#endif /* ndef FRAMERD_ERROBJS_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
