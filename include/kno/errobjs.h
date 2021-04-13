/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef KNO_ERROBJS_H
#define KNO_ERROBJS_H 1
#ifndef KNO_ERROBJS_H_INFO
#define KNO_ERROBJS_H_INFO "include/kno/errobjs.h"
#endif

KNO_EXPORT int kno_capture_stack;

/* Error handling */

KNO_EXPORT struct KNO_EXCEPTION *kno_exception_object(u8_exception ex);

KNO_EXPORT lispval kno_wrap_exception(u8_exception ex);
KNO_EXPORT void kno_restore_exception(struct KNO_EXCEPTION *exo);

KNO_EXPORT void kno_decref_u8x_xdata(void *ptr);
KNO_EXPORT void kno_decref_embedded_exception(void *ptr);

KNO_EXPORT lispval kno_raw_irritant(u8_exception ex);
KNO_EXPORT lispval kno_get_irritant(u8_exception ex);
KNO_EXPORT lispval kno_get_exception(u8_exception ex);
KNO_EXPORT lispval kno_simple_exception(u8_exception ex);
KNO_EXPORT void kno_simplify_exception(u8_exception ex);

KNO_EXPORT lispval kno_type_error(u8_string,u8_context,lispval);
KNO_EXPORT void kno_undeclared_error
(u8_context caller,u8_string details,lispval irritant);

KNO_EXPORT void kno_sum_exception(U8_OUTPUT *out,u8_exception e);
KNO_EXPORT u8_string kno_errstring(u8_exception e);
KNO_EXPORT lispval kno_exception_xdata(u8_exception e);

KNO_EXPORT void kno_print_exception(U8_OUTPUT *out,u8_exception e);
KNO_EXPORT void kno_log_error(u8_condition c,u8_context caller,
			      u8_string details,lispval irritant);
KNO_EXPORT void kno_log_exception(u8_exception ex);
KNO_EXPORT void kno_output_exception(u8_output out,u8_exception ex);
KNO_EXPORT void kno_output_errstack(u8_output out,u8_exception ex);
KNO_EXPORT void kno_log_errstack(u8_exception ex,int loglevel,int w_irritant);

KNO_EXPORT lispval kno_exception_backtrace(u8_exception ex);
KNO_EXPORT void kno_compact_backtrace(u8_output out,lispval stack,int limit);
KNO_EXPORT void kno_output_backtrace(u8_output out,lispval stack,int limit);

KNO_EXPORT U8_NOINLINE lispval kno_mkerr
(u8_condition c,u8_context cxt,u8_string details,lispval irritant,
 u8_exception *push);

#define kno_seterr3(c,cxt,details) \
   kno_seterr(c,cxt,details,KNO_VOID)
#define kno_seterr2(c,cxt) \
   kno_seterr(c,cxt,NULL,KNO_VOID)
#define kno_seterr1(c) \
   kno_seterr(c,NULL,NULL,KNO_VOID)

#define kno_err3(c,cxt,details) \
  ( (kno_seterr(c,cxt,details,KNO_VOID)) , (KNO_ERROR_VALUE) )
#define kno_err2(c,cxt) \
  ( (kno_seterr(c,cxt,NULL,KNO_VOID)) , (KNO_ERROR_VALUE) )
#define kno_err1(c) \
  ( (kno_seterr(c,NULL,NULL,KNO_VOID)) , (KNO_ERROR_VALUE) )

#define KNO_ERR(rv,cond,cxt,details,irritant) \
  ( (kno_seterr(cond,cxt,(details),(irritant))) , (rv) )
#define KNO_ERR3(rv,cond,cxt,details) \
  ( (kno_seterr(cond,cxt,(details),(KNO_VOID))) , (rv) )
#define KNO_ERR2(rv,cond,cxt) \
  ( (kno_seterr(cond,cxt,(NULL),(KNO_VOID))) , (rv) )
#define KNO_ERR1(rv,cond) \
  ( (kno_seterr(cond,NULL,(NULL),(KNO_VOID))) , (rv) )

KNO_EXPORT void kno_set_type_error(u8_string type_name,lispval irritant);

KNO_EXPORT int kno_geterr
  (u8_condition *c,u8_context *cxt,u8_string *details,lispval *irritant);
KNO_EXPORT int kno_poperr
  (u8_condition *c,u8_context *cxt,u8_string *details,lispval *irritant);


KNO_EXPORT int kno_reterr
  (u8_condition c,u8_context cxt,u8_string details,lispval irritant);
KNO_EXPORT int kno_interr(lispval x);
KNO_EXPORT lispval kno_erreify(void);

KNO_EXPORT u8_condition kno_retcode_to_exception(lispval err);

KNO_EXPORT lispval kno_exception_backtrace(u8_exception ex);

KNO_EXPORT lispval kno_lisp_error(u8_exception ex);

#endif /* ndef KNO_ERROBJS_H */

