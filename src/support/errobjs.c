/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/numbers.h"
#include "kno/apply.h"

#include <libu8/u8signals.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8printf.h>
#include <libu8/u8logging.h>

#include <signal.h>
#include <sys/types.h>
#include <pwd.h>

#include <libu8/libu8.h>
#include <libu8/u8elapsed.h>
#include <libu8/u8netfns.h>
#include <libu8/u8printf.h>
#if KNO_FILECONFIG_ENABLED
#include <libu8/u8filefns.h>
#include <libu8/libu8io.h>
#endif

static int max_irritant_len=256;

static lispval stack_entry_symbol, exception_stack_symbol;

int kno_log_stack_max = 500;
int kno_sum_stack_max = 200;

/* Managing error data */

KNO_EXPORT void kno_decref_u8x_xdata(void *ptr)
{
  lispval v = (lispval)ptr;
  kno_decref(v);
}

KNO_EXPORT void kno_decref_embedded_exception(void *ptr)
{
  lispval v = (lispval)ptr;
  kno_decref(v);
}

static lispval get_exception_context(u8_exception ex)
{
  if (ex == NULL)
    return KNO_VOID;
  else {
    int depth = 0; u8_exception scan = ex;
    while (scan) {
      scan = scan->u8x_prev;
      depth++;}
    lispval stack_vec = kno_make_vector(depth,NULL);
    int i=0; scan = ex; while (scan) {
      lispval irritant = kno_get_irritant(scan);
      lispval entry = kno_init_exception
        (NULL,scan->u8x_cond,scan->u8x_context,
         u8_strdup(scan->u8x_details),kno_incref(irritant),
         KNO_FALSE,KNO_VOID,
         NULL,-1,-1,-1);
      KNO_VECTOR_SET(stack_vec,i,entry);
      scan = scan->u8x_prev;
      i++;}
    struct KNO_KEYVAL init_kv = { exception_stack_symbol, stack_vec};
    return kno_make_slotmap(4,1,&init_kv);}
}

static u8_exception mkerr(u8_condition c,u8_context caller,
                          u8_string details,
                          lispval irritant)
{
  u8_exception ex = u8_current_exception;
  u8_condition condition = (c) ? (c) : (ex) ? (ex->u8x_cond) :
    ((u8_condition)"Unknown (NULL) error");
  /* Don't grab contexts or backtraces for thread termination */
  if (c == kno_ThreadTerminated)
    return u8_new_exception(condition,caller,NULL,NULL,NULL);
  struct KNO_EXCEPTION *exo = (ex) ? (kno_exception_object(ex)) : (NULL);
  lispval backtrace = ( (exo) && (KNO_CONSP(exo->ex_stack)) ) ?
    (kno_incref(exo->ex_stack)) :
    (kno_get_backtrace(kno_stackptr));
  lispval context = (ex) ? (get_exception_context(ex)) : (KNO_VOID);
  lispval exception = kno_init_exception
    (NULL,condition,caller,u8_strdup(details),
     irritant,backtrace,context,
     u8_sessionid(),
     u8_elapsed_time(),
     u8_elapsed_base(),
     u8_threadid());
  kno_incref(irritant);
  return u8_new_exception(condition,caller,u8_strdup(details),
                          (void *)exception,kno_decref_embedded_exception);
}

KNO_EXPORT u8_exception kno_mkerr
  (u8_condition c,u8_context caller,u8_string details,lispval irritant)
{
  return mkerr(c,caller,details,irritant);
}

KNO_EXPORT void kno_seterr
  (u8_condition c,u8_context caller,u8_string details,lispval irritant)
{
  u8_exception ex = mkerr(c,caller,details,irritant);
  u8_expush(ex);
}

KNO_EXPORT void kno_restore_exception(struct KNO_EXCEPTION *exo)
{
  kno_incref((lispval)exo);
  u8_push_exception(exo->ex_condition,exo->ex_caller,
                    u8_strdup(exo->ex_details),
                    (void *)exo,
                    kno_decref_embedded_exception);
}

KNO_EXPORT lispval kno_wrap_exception(u8_exception ex)
{
  if (ex == NULL) ex = u8_current_exception;
  lispval exception = kno_get_exception(ex);
  if (!(KNO_VOIDP(exception)))
    return kno_incref(exception);
  else {
    u8_condition condition = (ex) ? (ex->u8x_cond) :
      ((u8_condition)"missingCondition");
    u8_context caller = (ex) ? (ex->u8x_context) : (NULL);
    u8_string details = (ex) ? (ex->u8x_details) : (NULL);
    lispval irritant = (ex) ? (kno_get_irritant(ex)) : (KNO_VOID);
    lispval backtrace = kno_get_backtrace(kno_stackptr);
    return kno_init_exception(NULL,condition,caller,
                             u8_strdup(details),
                             kno_incref(irritant),
                             backtrace,KNO_VOID,
                             u8_sessionid(),
                             ex->u8x_moment,
                             u8_elapsed_base(),
                             ex->u8x_thread);}
}

KNO_EXPORT lispval kno_simple_exception(u8_exception ex)
{
  if (ex == NULL) ex = u8_current_exception;
  u8_condition condition = (ex) ? (ex->u8x_cond) :
    ((u8_condition)"missingCondition");
  u8_context caller = (ex) ? (ex->u8x_context) : (NULL);
  u8_string details = (ex) ? (ex->u8x_details) : (NULL);
  return kno_init_exception(NULL,condition,caller,
                            u8_strdup(details),
                            KNO_VOID,KNO_VOID,KNO_VOID,
                            u8_sessionid(),
                            ex->u8x_moment,
                            u8_elapsed_base(),
                            ex->u8x_thread);
}

/* This gets the 'actual' irritant from a u8_exception, extracting it
   from the underlying u8_condition (if that's an irritant) */
KNO_EXPORT lispval kno_get_irritant(u8_exception ex)
{
  if (ex->u8x_free_xdata == kno_decref_embedded_exception) {
    lispval irritant = (lispval) ex->u8x_xdata;
    if (KNO_EXCEPTIONP(irritant)) {
      struct KNO_EXCEPTION *exo = (kno_exception) irritant;
      return exo->ex_irritant;}
    else return irritant;}
  else if (ex->u8x_free_xdata == kno_decref_u8x_xdata)
    return (lispval) ex->u8x_xdata;
  else return KNO_VOID;
}

/* This gets the exception object (if there is one) from a u8_exception, extracting it from the
   underlying u8_condition (if that's an irritant) */
KNO_EXPORT lispval kno_get_exception(u8_exception ex)
{
  if (ex->u8x_free_xdata == kno_decref_embedded_exception) {
    lispval irritant = (lispval) ex->u8x_xdata;
    if (KNO_EXCEPTIONP(irritant))
      return irritant;
    else return KNO_VOID;}
  else return KNO_VOID;
}

/* This gets the 'actual' irritant from a u8_exception, extracting it
   from the underlying u8_condition (if that's an irritant) */
KNO_EXPORT struct KNO_EXCEPTION *kno_exception_object(u8_exception ex)
{
  if (ex->u8x_free_xdata == kno_decref_embedded_exception) {
    lispval irritant = (lispval) ex->u8x_xdata;
    if (KNO_EXCEPTIONP(irritant))
      return (kno_exception) irritant;
    else return NULL;}
  else return NULL;
}

KNO_EXPORT void kno_raise
  (u8_condition c,u8_context cxt,u8_string details,lispval irritant)
{
  u8_exception ex = kno_mkerr(c,cxt,details,irritant);
  u8_raise_exception(ex);
}

KNO_EXPORT int kno_poperr
  (u8_condition *c,u8_context *cxt,u8_string *details,lispval *irritant)
{
  u8_exception current = u8_current_exception;
  if (current == NULL) return 0;
  if (c) *c = current->u8x_cond;
  if (cxt) *cxt = current->u8x_context;
  if (details) {
    /* If we're hanging onto the details, clear it from
       the structure before popping. */
    *details = current->u8x_details;
    current->u8x_details = NULL;}
  if (irritant) {
    if ((current->u8x_xdata) &&
        (current->u8x_free_xdata == kno_decref_u8x_xdata)) {
      /* Likewise for the irritant */
      *irritant = (lispval)(current->u8x_xdata);
      current->u8x_xdata = NULL;
      current->u8x_free_xdata = NULL;}
    else *irritant = VOID;}
  u8_pop_exception();
  return 1;
}

KNO_EXPORT lispval kno_exception_xdata(u8_exception ex)
{
  if ((ex->u8x_xdata) && (KNO_XDATA_ISLISP(ex)))
    return (lispval)(ex->u8x_xdata);
  else return VOID;
}

KNO_EXPORT int kno_reterr
  (u8_condition c,u8_context cxt,u8_string details,lispval irritant)
{
  kno_seterr(c,cxt,details,irritant);
  return -1;
}

KNO_EXPORT int kno_interr(lispval x)
{
  return -1;
}

KNO_EXPORT lispval kno_err
  (u8_condition ex,u8_context cxt,u8_string details,lispval irritant)
{
  if (KNO_CHECK_PTR(irritant)) {
    if (details)
      kno_seterr(ex,cxt,details,irritant);
    else kno_seterr(ex,cxt,NULL,irritant);}
  else if (details)
    kno_seterr(ex,cxt,details,VOID);
  else kno_seterr(ex,cxt,NULL,VOID);
  return KNO_ERROR;
}

KNO_EXPORT lispval kno_type_error(u8_string type_name,u8_context cxt,lispval irritant)
{
  u8_byte buf[512];
  u8_string msg = u8_sprintf(buf,512,_("object is not a %m"),type_name);
  return KNO_ERR(KNO_TYPE_ERROR,kno_TypeError,cxt,msg,irritant);
}

KNO_EXPORT void kno_set_type_error(u8_string type_name,lispval irritant)
{
  u8_byte buf[512];
  u8_string msg = u8_sprintf(buf,512,_("object is not a %m"),type_name);
  kno_seterr(kno_TypeError,NULL,msg,irritant);
}

KNO_EXPORT
void kno_print_exception(U8_OUTPUT *out,u8_exception ex)
{
  u8_printf(out,";;(ERROR %m)",ex->u8x_cond);
  if (ex->u8x_details) u8_printf(out," %m",ex->u8x_details);
  if (ex->u8x_context) u8_printf(out," (%s)",ex->u8x_context);
  u8_printf(out,"\n");
  if (ex->u8x_xdata) {
    lispval irritant = kno_exception_xdata(ex);
    u8_puts(out,";; ");
    kno_pprint(out,irritant,";; ",0,3,100);}
}

KNO_EXPORT
void kno_log_exception(u8_exception ex)
{
  if (ex->u8x_xdata) {
    lispval irritant = kno_get_irritant(ex);
    u8_log(LOG_WARN,ex->u8x_cond,"%m (%m)\n\t%Q",
           (U8ALT((ex->u8x_details),((U8S0())))),
           (U8ALT((ex->u8x_context),((U8S0())))),
           irritant);}
  else u8_log(LOG_WARN,ex->u8x_cond,"%m (%m)",
              (U8ALT((ex->u8x_details),((U8S0())))),
              (U8ALT((ex->u8x_context),((U8S0())))));
}

KNO_EXPORT
void kno_output_exception(u8_output out,u8_exception ex)
{
  u8_puts(out,";; !! ");
  u8_puts(out,ex->u8x_cond);
  if (ex->u8x_context) {
    u8_puts(out," <");
    u8_puts(out,ex->u8x_context);
    u8_puts(out,">");}
  if (ex->u8x_details) {
    u8_puts(out," (");
    u8_puts(out,ex->u8x_details);
    u8_puts(out,")");}
  if (ex->u8x_free_xdata == kno_decref_u8x_xdata) {
    lispval irritant=kno_get_irritant(ex);
    if (VOIDP(irritant)) {}
    else if ( (PAIRP(irritant)) ||
              (VECTORP(irritant)) ||
              (SLOTMAPP(irritant)) ||
              (SCHEMAPP(irritant)) ) {
      u8_puts(out," irritant:\n    ");
      kno_pprint(out,irritant,"    ",0,4,120);}
    else if ( (STRINGP(irritant)) &&
              (STRLEN(irritant)>40) ) {
      u8_puts(out," irritant (string):\n    ");
      kno_unparse(out,irritant);}
    else {
      u8_puts(out," irritant=");
      kno_unparse(out,irritant);}}
  u8_putc(out,'\n');
}

KNO_EXPORT
void sum_exception(u8_output out,u8_exception ex)
{
  if (ex->u8x_cond)
    u8_puts(out,ex->u8x_cond);
  else u8_puts(out,"UnknownError");
  if (ex->u8x_context) {
    u8_puts(out," <");
    u8_puts(out,ex->u8x_context);
    u8_puts(out,">");}
  if (ex->u8x_details) {
    u8_puts(out," (");
    u8_puts(out,ex->u8x_details);
    u8_puts(out,")");}
}

KNO_EXPORT
void kno_output_errstack(u8_output out,u8_exception ex)
{
  if (ex==NULL) ex=u8_current_exception;
  u8_exception scan = ex;
  while (scan) {
    kno_output_exception(out,scan);
    scan=scan->u8x_prev;}
  lispval stack = KNO_U8X_STACK(ex);
  if (!(KNO_VOIDP(stack)))
    kno_compact_backtrace(out,stack,kno_sum_stack_max);
}

KNO_EXPORT
void kno_log_errstack(u8_exception ex,int loglevel,int w_irritant)
{
  if (ex==NULL) ex=u8_current_exception;
  while (ex) {
    lispval irritant = kno_get_irritant(ex);
    if (VOIDP(irritant))
      u8_log(loglevel,ex->u8x_cond,"@%s %s",ex->u8x_context,
             U8ALT(ex->u8x_details,""));
    else if ( (w_irritant) ||
              (KNO_IMMEDIATEP(irritant)) ||
              (NUMBERP(irritant)) ||
              (TYPEP(irritant,kno_timestamp_type)) ||
              (TYPEP(irritant,kno_uuid_type)) ||
              (TYPEP(irritant,kno_regex_type)) )
      u8_log(loglevel,ex->u8x_cond,"%q @%s %s",
             irritant,ex->u8x_context,
             U8ALT(ex->u8x_details,""));
    else u8_log(loglevel,ex->u8x_cond,"@%s %s\n    %Q",
                ex->u8x_context,U8ALT(ex->u8x_details,""),
                irritant);
    ex=ex->u8x_prev;}
}

static void compact_stack_entry(u8_output out,lispval entry)
{
  if (KNO_COMPOUND_TYPEP(entry,stack_entry_symbol)) {
    size_t    len = KNO_COMPOUND_LENGTH(entry);
    lispval *elts = KNO_COMPOUND_ELTS(entry);
    long long depth = ( (len>0) && (KNO_FIXNUMP(elts[0])) ) ?
      (KNO_FIX2INT(elts[0])) : (-1);
    u8_string type = ( (len>1) && (KNO_STRINGP(elts[1])) ) ?
      (KNO_CSTRING(elts[1])) : (NULL);
    u8_string label = ( (len>3) && (KNO_STRINGP(elts[3])) ) ?
      (KNO_CSTRING(elts[3])) : (NULL);
    if (type == NULL) type = "?";
    if (label == NULL) label = "*";
    u8_printf(out,"%lld:%s:%s",depth,type,label);}
  else {}
}

KNO_EXPORT void kno_compact_backtrace(u8_output out,lispval stack,int limit)
{
  if (KNO_VECTORP(stack)) {
    int i = 0, len = KNO_VECTOR_LENGTH(stack), count = 0;
    if ( (limit > 0) && (len > limit) ) {
      int head_limit = limit-(limit/2);
      while (i < head_limit) {
        lispval stack_entry = KNO_VECTOR_REF(stack,i);
        compact_stack_entry(out,stack_entry);
        count++; i++;
        if ( (i < head_limit) && ((count%4) == 0) )
          u8_puts(out,"\n  ");}
      u8_printf(out,"\n... %d/%d calls ...\n",len-limit*2,len);
      i = len-(limit/2);
      while (i < len) {
        lispval stack_entry = KNO_VECTOR_REF(stack,i);
        compact_stack_entry(out,stack_entry);
        count++; i++;
        if ( (i < len) && ((count%4) == 0) )
          u8_puts(out,"\n  ");}}
    else while (i < len) {
        lispval stack_entry = KNO_VECTOR_REF(stack,i);
        compact_stack_entry(out,stack_entry);
        count++; i++;
        if ( (i < len) && ((count%4) == 0) )
          u8_puts(out,"\n  ");}}
}

static void output_stack_entry(u8_output out,lispval entry)
{
  if (KNO_COMPOUND_TYPEP(entry,stack_entry_symbol)) {
    ssize_t    len = KNO_COMPOUND_LENGTH(entry);
    lispval *elts = KNO_COMPOUND_ELTS(entry);
    long long depth = ( (len>0) && (KNO_FIXNUMP(elts[0])) ) ?
      (KNO_FIX2INT(elts[0])) : (-1);
    u8_string type = ( (len>1) && (KNO_STRINGP(elts[1])) ) ?
      (KNO_CSTRING(elts[1])) : (NULL);
    u8_string label = ( (len>3) && (KNO_STRINGP(elts[3])) ) ?
      (KNO_CSTRING(elts[3])) : (NULL);
    lispval op = (len > 2) ? (elts[2]) : (KNO_VOID);
    if (type == NULL) type = "?";
    if (label == NULL) label = "*";
    u8_printf(out,"%lld:%s:%s %q",depth,type,label,op);}
  else kno_pprint(out,entry,0,0,0,111);
}

KNO_EXPORT void kno_output_backtrace(u8_output out,lispval stack,int limit)
{
  if (KNO_VECTORP(stack)) {
    int i = 0, len = KNO_VECTOR_LENGTH(stack), count = 0;
    if ( (limit > 0) && (len > limit) ) {
      int head_limit = limit-(limit/2);
      while (i < head_limit) {
        lispval stack_entry = KNO_VECTOR_REF(stack,i);
        output_stack_entry(out,stack_entry);
        count++; i++;
        if ( (i < head_limit) && ((count%4) == 0) )
          u8_puts(out,"\n  ");}
      u8_printf(out,"\n... %d/%d calls ...\n",len-limit*2,len);
      i = len-(limit/2);
      while (i < len) {
        lispval stack_entry = KNO_VECTOR_REF(stack,i);
        output_stack_entry(out,stack_entry);
        count++; i++;
        if ( (i < len) && ((count%4) == 0) )
          u8_puts(out,"\n  ");}}
    else while (i < len) {
        lispval stack_entry = KNO_VECTOR_REF(stack,i);
        output_stack_entry(out,stack_entry);
        count++; i++;
        if ( (i < len) && ((count%4) == 0) )
          u8_puts(out,"\n  ");}}
}

KNO_EXPORT
void kno_sum_exception(U8_OUTPUT *out,u8_exception ex)
{
  u8_exception scan = ex, prev = NULL;
  while (scan) {
    u8_puts(out,";; !! ");
    sum_exception(out,scan);
    lispval stack = KNO_U8X_STACK(scan);
    if (!(KNO_VOIDP(stack)))
      kno_compact_backtrace(out,stack,kno_sum_stack_max);
    prev=scan;
    scan=prev->u8x_prev;
    u8_putc(out,'\n');}
}

KNO_EXPORT u8_string kno_errstring(u8_exception ex)
{
  struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,128);
  if (ex == NULL) ex = u8_current_exception;
  if (ex == NULL) return NULL;
  sum_exception(&out,ex);
  lispval backtrace = KNO_U8X_STACK(ex);
  if (!(KNO_VOIDP(backtrace)))
    kno_compact_backtrace(&out,backtrace,kno_sum_stack_max);
  return out.u8_outbuf;
}

KNO_EXPORT u8_condition kno_retcode_to_exception(lispval err)
{
  switch (err) {
  case KNO_EOF: case KNO_EOD: return kno_UnexpectedEOD;
  case KNO_PARSE_ERROR: case KNO_EOX: return kno_ParseError;
  case KNO_TYPE_ERROR: return kno_TypeError;
  case KNO_RANGE_ERROR: return kno_RangeError;
  case KNO_OOM: return kno_OutOfMemory;
  case KNO_DTYPE_ERROR: return kno_DTypeError;
  default: return kno_UnknownError;
  }
}

KNO_EXPORT
int kno_clear_errors(int report)
{
  int n_errs = 0;
  u8_exception ex = u8_erreify(), scan = ex;
  while (scan) {
    if (report) {
      u8_string sum = kno_errstring(scan);
      u8_logger(LOG_ERR,scan->u8x_cond,sum);
      u8_free(sum);}
    scan = scan->u8x_prev;
    n_errs++;}
  if (ex) u8_free_exception(ex,1);
  return n_errs;
}

/* Exception objects */

KNO_EXPORT lispval kno_init_exception
   (struct KNO_EXCEPTION *exo,
    u8_condition condition,u8_context caller,
    u8_string details,lispval irritant,
    lispval stack,lispval context,
    u8_string sid,
    double moment,
    time_t timebase,
    long long thread)
{
  if (exo == NULL) exo = u8_alloc(struct KNO_EXCEPTION);
  KNO_INIT_CONS(exo,kno_exception_type);
  exo->ex_condition = condition;
  exo->ex_caller    = caller;
  exo->ex_moment    = moment;
  exo->ex_thread    = thread;
  exo->ex_details   = details;
  exo->ex_irritant  = irritant;
  exo->ex_stack     = stack;
  exo->ex_context   = context;
  exo->ex_session   = sid;
  exo->ex_moment    = moment;
  exo->ex_timebase  = timebase;
  exo->ex_thread    = thread;
  return LISP_CONS(exo);
}

static ssize_t write_exception_dtype(struct KNO_OUTBUF *out,lispval x)
{
  struct KNO_EXCEPTION *xo = (struct KNO_EXCEPTION *)x;
  u8_condition condition = xo->ex_condition;
  u8_context caller = xo->ex_caller;
  u8_string details = xo->ex_details;
  lispval irritant = xo->ex_irritant;
  lispval backtrace = xo->ex_stack;
  lispval context = xo->ex_context;
  u8_string session = (xo->ex_session) ? (xo->ex_session) : (u8_sessionid());
  time_t timebase = xo->ex_timebase;
  double moment = xo->ex_moment;
  lispval vector = kno_empty_vector(9);
  KNO_VECTOR_SET(vector,0,kno_intern(condition));
  if (caller) {
    KNO_VECTOR_SET(vector,1,kno_intern(caller));}
  else {KNO_VECTOR_SET(vector,1,KNO_FALSE);}
  if (details) {
    KNO_VECTOR_SET(vector,2,kno_mkstring(details));}
  else {KNO_VECTOR_SET(vector,2,KNO_FALSE);}
  KNO_VECTOR_SET(vector,3,kno_incref(irritant));
  if (!(VOIDP(backtrace)))
    KNO_VECTOR_SET(vector,4,kno_incref(backtrace));
  else KNO_VECTOR_SET(vector,4,KNO_FALSE);
  KNO_VECTOR_SET(vector,5,kno_make_string(NULL,-1,session));
  KNO_VECTOR_SET(vector,6,kno_time2timestamp(timebase));
  KNO_VECTOR_SET(vector,7,kno_make_flonum(moment));
  if (!(VOIDP(context)))
    KNO_VECTOR_SET(vector,8,kno_incref(context));
  else KNO_VECTOR_SET(vector,8,KNO_FALSE);

  struct KNO_OUTBUF tmpbuf;
  unsigned char bytes[16000];
  KNO_INIT_OUTBUF(&tmpbuf,bytes,16000,
                 KNO_IS_WRITING|KNO_BUFFER_NO_FLUSH|KNO_STATIC_BUFFER|
                 KNO_USE_DTYPEV2|KNO_WRITE_OPAQUE);
  kno_write_byte(&tmpbuf,dt_exception);
  ssize_t base_len = kno_write_dtype(&tmpbuf,vector);
  kno_decref(vector);
  kno_write_bytes(out,tmpbuf.buffer,tmpbuf.bufwrite-tmpbuf.buffer);
  kno_close_outbuf(&tmpbuf);
  if (base_len<0)
    return -1;
  return 1+base_len;
}

KNO_EXPORT lispval kno_restore_exception_dtype(lispval content)
{
  /* Return an exception object if possible (content as expected)
     and a compound if there are any big surprises */
  u8_condition condname=_("Poorly Restored Error");
  u8_context caller = NULL; u8_string details = NULL;
  lispval irritant = VOID, stack = VOID, context = VOID;
  u8_string sessionid = NULL;
  double moment = -1.0;
  time_t timebase = -1;
  if (KNO_TYPEP(content,kno_exception_type))
    return content;
  else if (VECTORP(content)) {
    int len = VEC_LEN(content);
    /* And the new format is:
         #(ex caller details [irritant] [stack]
           [sid] [timebase] [moment] [context] )
         where ex and context are symbols and stack and context
          are optional values which default to VOID
       We handle all cases
    */
    if (len>0) {
      lispval condval = VEC_REF(content,0);
      if (SYMBOLP(condval))
        condname = SYM_NAME(condval);
      else if (STRINGP(condval)) {
        lispval tmp = kno_probe_symbol(CSTRING(condval),STRLEN(condval));
        if (KNO_VOIDP(tmp)) {
          u8_log(LOG_WARN,kno_DTypeError,"Bad non-symbolic condition name",
                 condval);
          tmp=kno_intern(CSTRING(condval));}
        condname = SYM_NAME(tmp);}
      else {
        u8_log(LOG_WARN,kno_DTypeError,
               "Bad condition (not a symbol) %q in exception serialization %q",
               condval,content);
        condname="BadCondName";}}
    if (len>1) {
      lispval caller_val = VEC_REF(content,1);
      if (SYMBOLP(caller_val))
        caller = SYM_NAME(caller_val);
      else if (STRINGP(caller_val)) {
        lispval tmp =
          kno_probe_symbol(CSTRING(caller_val),STRLEN(caller_val));
        if (KNO_VOIDP(tmp)) {
          u8_log(LOG_WARN,kno_DTypeError,"Bad non-symbolic caller",
                 caller_val);
          tmp=kno_intern(CSTRING(caller_val));}
        caller = SYM_NAME(tmp);}
      else if ( (KNO_FALSEP(caller_val)) ||
                (KNO_EMPTYP(caller_val)) ||
                (caller_val == KNO_EMPTY_LIST) ||
                (KNO_VOIDP(caller_val)) )
        caller = NULL;
      else {
        u8_log(LOG_WARN,kno_DTypeError,
               "Bad caller (not a symbol) %q in exception serialization %q",
               caller_val,content);
         caller="BadCaller";}}
    if (len > 2) {
      lispval details_val = VEC_REF(content,2);
      if (KNO_STRINGP(details_val))
        details = u8_strdup(CSTRING(details_val));
      else details = NULL;}
    if (len > 3) {
      irritant = VEC_REF(content,3); kno_incref(irritant);}
    if ( (len > 4) && (KNO_VECTORP(VEC_REF(content,4))) ) {
      stack = VEC_REF(content,4); kno_incref(stack);}
    if ( (len > 5) && (KNO_STRINGP(VEC_REF(content,5))) )
      sessionid = u8_strdup(CSTRING(VEC_REF(content,5)));
    if (len > 6) {
      lispval tstamp = VEC_REF(content,6);
      if (TYPEP(tstamp,kno_timestamp_type)) {
        struct KNO_TIMESTAMP *ts = (kno_timestamp) tstamp;
        struct U8_XTIME *xt = &(ts->u8xtimeval);
        timebase = xt->u8_tick;}
      else if (KNO_FIXNUMP(tstamp))
        timebase = (time_t) kno_getint(tstamp);
      else timebase=-1;}
    if ( ( len > 7 ) && (KNO_FLONUMP(VEC_REF(content,7))) ) {
      lispval flonum = VEC_REF(content,7);
      moment = KNO_FLONUM(flonum);}
    if (len > 8) {
      context = VEC_REF(content,8);
      kno_incref(context);}
    return kno_init_exception(NULL,condname,caller,
                             details,irritant,
                             stack,context,
                             sessionid,moment,
                             timebase,
                             -1);}
  else if (KNO_SYMBOLP(content)) {
    return kno_init_exception
      (NULL,KNO_SYMBOL_NAME(content),
       NULL,NULL,content,
       KNO_VOID,KNO_VOID,NULL,
       -1,-1,-1);}
  else if (KNO_STRINGP(content)) {
    return kno_init_exception
      (NULL,"DtypeError",NULL,
       u8_strdup(KNO_CSTRING(content)),content,
       KNO_VOID,KNO_VOID,NULL,
       -1,-1,-1);}
  else return kno_init_exception
         (NULL,kno_DTypeError,
          "kno_restore_exception_dtype",NULL,content,
          KNO_VOID,KNO_VOID,NULL,
          u8_elapsed_time(),
          u8_elapsed_base(),
          u8_threadid());
}

static lispval copy_exception(lispval x,int deep)
{
  struct KNO_EXCEPTION *xo=
    kno_consptr(struct KNO_EXCEPTION *,x,kno_exception_type);
  return kno_init_exception(NULL,xo->ex_condition,xo->ex_caller,
                           u8_strdup(xo->ex_details),
                           kno_incref(xo->ex_irritant),
                           kno_incref(xo->ex_stack),
                           kno_incref(xo->ex_context),
                           xo->ex_session,
                           xo->ex_moment,
                           xo->ex_timebase,
                           xo->ex_thread);
}

static int unparse_exception(struct U8_OUTPUT *out,lispval x)
{
  struct KNO_EXCEPTION *xo=
    kno_consptr(struct KNO_EXCEPTION *,x,kno_exception_type);
  u8_condition condition = xo->ex_condition;
  u8_context caller = xo->ex_caller;
  u8_string details = xo->ex_details;
  lispval irritant = xo->ex_irritant;
  u8_puts(out,"#<!EXCEPTION");
  if (condition) u8_printf(out," %s",condition);
  else u8_puts(out," missingCondition");
  if (caller) u8_printf(out," <%s>",caller);
  if (details) u8_printf(out," (%s)",details);
  if (!(VOIDP(irritant))) {
    if (max_irritant_len==0) {}
    else if (max_irritant_len<0)
      u8_printf(out," =%q",irritant);
    else {
      u8_byte buf[max_irritant_len+1];
      u8_sprintf(buf,max_irritant_len,"%q",irritant);
      u8_printf(out," =%s...",buf);}}
  u8_printf(out,"!>");
  return 1;
}

void kno_init_err_c()
{
  u8_register_source_file(_FILEINFO);

  kno_copiers[kno_exception_type]=copy_exception;
  if (kno_dtype_writers[kno_exception_type]==NULL)
    kno_dtype_writers[kno_exception_type]=write_exception_dtype;
  if (kno_unparsers[kno_exception_type]==NULL)
    kno_unparsers[kno_exception_type]=unparse_exception;

  stack_entry_symbol=kno_intern("%%stack");
  exception_stack_symbol = kno_intern("exception-stack");
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
