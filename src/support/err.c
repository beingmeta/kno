/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/numbers.h"
#include "framerd/apply.h"

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
#if FD_FILECONFIG_ENABLED
#include <libu8/u8filefns.h>
#include <libu8/libu8io.h>
#endif

static int max_irritant_len=256;

static lispval stacktrace_symbol;

/* Managing error data */

FD_EXPORT void fd_decref_u8x_xdata(void *ptr)
{
  lispval v = (lispval)ptr;
  fd_decref(v);
}

FD_EXPORT void fd_decref_embedded_exception(void *ptr)
{
  lispval v = (lispval)ptr;
  fd_decref(v);
}

/* This stores the details and irritant arguments directly,
   so they should be dup'd or incref'd by the caller. */
FD_EXPORT void fd_seterr
  (u8_condition c,u8_context caller,u8_string details,lispval irritant)
{
  u8_exception ex = u8_current_exception;
  u8_condition condition = (c) ? (c) : (ex) ? (ex->u8x_cond) :
    ((u8_condition)"Unknown (NULL) error");
  lispval backtrace = fd_get_backtrace(fd_stackptr);
  lispval exception = fd_init_exception
    (NULL,condition,caller,u8_strdup(details),
     irritant,backtrace,VOID,
     NULL,u8_elapsed_time(),u8_threadid(),
     u8_elapsed_base());
  fd_incref(irritant);
  u8_push_exception(condition,caller,u8_strdup(details),
                    (void *)exception,fd_decref_embedded_exception);
}

FD_EXPORT void fd_restore_exception(struct FD_EXCEPTION *exo)
{
  u8_push_exception(exo->ex_condition,exo->ex_caller,
                    u8_strdup(exo->ex_details),
                    (void *)exo,fd_decref_embedded_exception);
}

FD_EXPORT lispval fd_wrap_exception(u8_exception ex)
{
  if (ex == NULL) ex = u8_current_exception;
  lispval exception = fd_get_exception(ex);
  if (!(FD_VOIDP(exception)))
    return fd_incref(exception);
  else {
    u8_condition condition = (ex) ? (ex->u8x_cond) :
      ((u8_condition)"missingCondition");
    u8_context caller = (ex) ? (ex->u8x_context) : (NULL);
    u8_string details = (ex) ? (ex->u8x_details) : (NULL);
    lispval irritant = (ex) ? (fd_get_irritant(ex)) : (FD_VOID);
    lispval backtrace = fd_get_backtrace(fd_stackptr);
    return fd_init_exception(NULL,condition,caller,
                             u8_strdup(details),
                             fd_incref(irritant),
                             backtrace,FD_VOID,
                             NULL,ex->u8x_moment,ex->u8x_thread,
                             u8_elapsed_base());}
}

FD_EXPORT int fd_stacktracep(lispval rep)
{
  if ((PAIRP(rep)) && (FD_CAR(rep) == stacktrace_symbol))
    return 1;
  else return 0;
}

/* This gets the 'actual' irritant from a u8_exception, extracting it
   from the underlying u8_condition (if that's an irritant) */
FD_EXPORT lispval fd_get_irritant(u8_exception ex)
{
  if (ex->u8x_free_xdata == fd_decref_embedded_exception) {
    lispval irritant = (lispval) ex->u8x_xdata;
    if (FD_EXCEPTIONP(irritant)) {
      struct FD_EXCEPTION *exo = (fd_exception) irritant;
      return exo->ex_irritant;}
    else return irritant;}
  else if (ex->u8x_free_xdata == fd_decref_u8x_xdata)
    return (lispval) ex->u8x_xdata;
  else return FD_VOID;
}

/* This gets the 'actual' irritant from a u8_exception, extracting it
   from the underlying u8_condition (if that's an irritant) */
FD_EXPORT lispval fd_get_exception(u8_exception ex)
{
  if (ex->u8x_free_xdata == fd_decref_embedded_exception) {
    lispval irritant = (lispval) ex->u8x_xdata;
    if (FD_EXCEPTIONP(irritant))
      return irritant;
    else return FD_VOID;}
  else return FD_VOID;
}

FD_EXPORT void fd_raise
  (u8_condition c,u8_context cxt,u8_string details,lispval irritant)
{
  fd_seterr(c,cxt,details,irritant);
  u8_raise(c,cxt,u8dup(details));
}

FD_EXPORT int fd_poperr
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
        (current->u8x_free_xdata == fd_decref_u8x_xdata)) {
      /* Likewise for the irritant */
      *irritant = (lispval)(current->u8x_xdata);
      current->u8x_xdata = NULL;
      current->u8x_free_xdata = NULL;}
    else *irritant = VOID;}
  u8_pop_exception();
  return 1;
}

FD_EXPORT lispval fd_exception_xdata(u8_exception ex)
{
  if ((ex->u8x_xdata) && (FD_XDATA_ISLISP(ex)))
    return (lispval)(ex->u8x_xdata);
  else return VOID;
}

FD_EXPORT int fd_reterr
  (u8_condition c,u8_context cxt,u8_string details,lispval irritant)
{
  fd_seterr(c,cxt,details,irritant);
  return -1;
}

FD_EXPORT int fd_interr(lispval x)
{
  return -1;
}

FD_EXPORT lispval fd_err
  (u8_condition ex,u8_context cxt,u8_string details,lispval irritant)
{
  if (FD_CHECK_PTR(irritant)) {
    if (details)
      fd_seterr(ex,cxt,details,irritant);
    else fd_seterr(ex,cxt,NULL,irritant);}
  else if (details)
    fd_seterr(ex,cxt,details,VOID);
  else fd_seterr(ex,cxt,NULL,VOID);
  return FD_ERROR;
}

FD_EXPORT lispval fd_type_error(u8_string type_name,u8_context cxt,lispval irritant)
{
  u8_byte buf[512];
  u8_string msg = u8_sprintf(buf,512,_("object is not a %m"),type_name);
  fd_seterr(fd_TypeError,cxt,msg,irritant);
  return FD_TYPE_ERROR;
}

FD_EXPORT void fd_set_type_error(u8_string type_name,lispval irritant)
{
  u8_byte buf[512];
  u8_string msg = u8_sprintf(buf,512,_("object is not a %m"),type_name);
  fd_seterr(fd_TypeError,NULL,msg,irritant);
}

FD_EXPORT
void fd_print_exception(U8_OUTPUT *out,u8_exception ex)
{
  u8_printf(out,";;(ERROR %m)",ex->u8x_cond);
  if (ex->u8x_details) u8_printf(out," %m",ex->u8x_details);
  if (ex->u8x_context) u8_printf(out," (%s)",ex->u8x_context);
  u8_printf(out,"\n");
  if (ex->u8x_xdata) {
    lispval irritant = fd_exception_xdata(ex);
    u8_puts(out,";; ");
    fd_pprint(out,irritant,";; ",0,3,100);}
}

FD_EXPORT
void fd_log_exception(u8_exception ex)
{
  if (ex->u8x_xdata) {
    lispval irritant = fd_get_irritant(ex);
    u8_log(LOG_WARN,ex->u8x_cond,"%m (%m)\n\t%Q",
           (U8ALT((ex->u8x_details),((U8S0())))),
           (U8ALT((ex->u8x_context),((U8S0())))),
           irritant);}
  else u8_log(LOG_WARN,ex->u8x_cond,"%m (%m)",
              (U8ALT((ex->u8x_details),((U8S0())))),
              (U8ALT((ex->u8x_context),((U8S0())))));
}

FD_EXPORT
void fd_output_exception(u8_output out,u8_exception ex)
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
  if (ex->u8x_free_xdata == fd_decref_u8x_xdata) {
    lispval irritant=fd_get_irritant(ex);
    if (VOIDP(irritant)) {}
    else if ( (PAIRP(irritant)) ||
              (VECTORP(irritant)) ||
              (SLOTMAPP(irritant)) ||
              (SCHEMAPP(irritant)) ) {
      u8_puts(out," irritant:\n    ");
      fd_pprint(out,irritant,"    ",0,4,120);}
    else if ( (STRINGP(irritant)) &&
              (STRLEN(irritant)>40) ) {
      u8_puts(out," irritant (string):\n    ");
      fd_unparse(out,irritant);}
    else {
      u8_puts(out," irritant=");
      fd_unparse(out,irritant);}}
  u8_putc(out,'\n');
}

FD_EXPORT
void fd_output_errstack(u8_output out,u8_exception ex)
{
  if (ex==NULL) ex=u8_current_exception;
  while (ex) {
    fd_output_exception(out,ex);
    ex=ex->u8x_prev;}
}

FD_EXPORT
void fd_log_errstack(u8_exception ex,int loglevel,int w_irritant)
{
  if (ex==NULL) ex=u8_current_exception;
  while (ex) {
    lispval irritant = fd_get_irritant(ex);
    if (VOIDP(irritant))
      u8_log(loglevel,ex->u8x_cond,"@%s %s",ex->u8x_context,
             U8ALT(ex->u8x_details,""));
    else if ( (FD_IMMEDIATEP(irritant)) ||
              (NUMBERP(irritant)) ||
              (TYPEP(irritant,fd_timestamp_type)) ||
              (TYPEP(irritant,fd_uuid_type)) ||
              (TYPEP(irritant,fd_regex_type)) )
      u8_log(loglevel,ex->u8x_cond,"%q @%s %s",
             irritant,ex->u8x_context,
             U8ALT(ex->u8x_details,""));
    else {
      lispval backtrace = FD_U8X_STACK(ex);
      if (!(FD_VOIDP(backtrace))) {
        U8_STATIC_OUTPUT(tmp,1000);
        fd_pprint(tmpout,backtrace,NULL,0,0,111);
        if (tmp.u8_outbuf[0])
          u8_log(loglevel,ex->u8x_cond,"%s",tmp.u8_outbuf);
        u8_close_output(tmpout);}}
    ex=ex->u8x_prev;}
}

int sum_exception(U8_OUTPUT *out,u8_exception ex,u8_exception bg)
{
  if (!(ex)) return 0;
  else {
    u8_condition cond = ex->u8x_cond;
    u8_context context = ex->u8x_context;
    u8_string details = ex->u8x_details;
    lispval irritant = fd_get_irritant(ex);
    if (bg) {
      if ( (cond) && (bg->u8x_cond == cond) ) cond = NULL;
      if ( (context) &&
           ( (bg->u8x_context == context) ||
             ( (bg->u8x_context) &&
               ( strcmp(bg->u8x_context,context) == 0 )) ) )
        context = NULL;
      if ( (details) && (bg->u8x_details) &&
           ( (bg->u8x_details == details) ||
             (strcmp(bg->u8x_details,details)==0)) )
        details = NULL;
      if ( (!(VOIDP(irritant))) &&
           ( irritant == ((lispval)(bg->u8x_xdata)) ) )
        irritant=VOID;}
    if ((cond) && (context) && (details))
      u8_printf(out,"%m @%s (%s)",cond,context,details);
    else if ((cond) && (context))
      u8_printf(out,"%m @%s",cond,context);
    else if ((cond) && (details))
      u8_printf(out,"%m (%s)",cond,details);
    else if ((context) && (details))
      u8_printf(out,"@%s (%s)",context,details);
    else if (cond)
      u8_printf(out,"%m",cond);
    else if (context)
      u8_printf(out,"@%s",context);
    else if (details)
      u8_printf(out,"(%s)",details);
    else return 0;
    if (!(VOIDP(irritant))) {
      u8_byte buf[max_irritant_len+1];
      u8_sprintf(buf,max_irritant_len,"%Q",irritant);
      if ( (strchr(buf,'\n')) || (strlen(buf)>42) )
        u8_printf(out,"\n  %s",buf);
      else u8_printf(out,"  %s",buf);}
    return 1;}
}

FD_EXPORT void fd_compact_backtrace(u8_output out,lispval backtrace)
{
  lispval scan = backtrace; int depth = 0;
  u8_puts(out,"\n\t");
  while (PAIRP(scan)) {
    lispval car=FD_CAR(scan); scan=FD_CDR(scan);
    if (STRINGP(car)) {
      if (depth) u8_puts(out,"// ");
      u8_puts(out,CSTRING(car));
      depth++;
      if ((depth%7)==0)
        u8_puts(out," //...\n;;* ...");
      else u8_putc(out,' ');}}
}


FD_EXPORT
void fd_sum_exception(U8_OUTPUT *out,u8_exception ex)
{
  u8_exception scan = ex, prev = NULL;
  while (scan) {
    sum_exception(out,scan,prev); u8_puts(out,"\n");
    lispval backtrace = FD_U8X_STACK(scan);
    if (!(FD_VOIDP(backtrace)))
      fd_compact_backtrace(out,backtrace);
    prev=scan;
    scan=prev->u8x_prev;}
}

FD_EXPORT u8_string fd_errstring(u8_exception ex)
{
  struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,128);
  if (ex == NULL) ex = u8_current_exception;
  if (ex == NULL) return NULL;
  sum_exception(&out,ex,NULL);
  lispval backtrace = FD_U8X_STACK(ex);
  if (!(FD_VOIDP(backtrace)))
    fd_compact_backtrace(&out,backtrace);
  return out.u8_outbuf;
}

FD_EXPORT u8_condition fd_retcode_to_exception(lispval err)
{
  switch (err) {
  case FD_EOF: case FD_EOD: return fd_UnexpectedEOD;
  case FD_PARSE_ERROR: case FD_EOX: return fd_ParseError;
  case FD_TYPE_ERROR: return fd_TypeError;
  case FD_RANGE_ERROR: return fd_RangeError;
  case FD_OOM: return fd_OutOfMemory;
  case FD_DTYPE_ERROR: return fd_DTypeError;
  default: return fd_UnknownError;
  }
}

FD_EXPORT
int fd_clear_errors(int report)
{
  int n_errs = 0;
  u8_exception ex = u8_erreify(), scan = ex;
  while (scan) {
    if (report) {
      u8_string sum = fd_errstring(scan);
      u8_logger(LOG_ERR,scan->u8x_cond,sum);
      u8_free(sum);}
    scan = scan->u8x_prev;
    n_errs++;}
  if (ex) u8_free_exception(ex,1);
  return n_errs;
}

/* Exception objects */

FD_EXPORT lispval fd_init_exception
   (struct FD_EXCEPTION *exo,
    u8_condition condition,u8_context caller,
    u8_string details,lispval irritant,
    lispval stack,lispval context,
    u8_string sid,double moment,long long thread,
    time_t timebase)
{
  if (exo == NULL) exo = u8_alloc(struct FD_EXCEPTION);
  FD_INIT_CONS(exo,fd_exception_type);
  exo->ex_condition = condition;
  exo->ex_caller    = caller;
  exo->ex_moment    = moment;
  exo->ex_thread    = thread;
  exo->ex_details   = details;
  exo->ex_irritant  = irritant;
  exo->ex_stack     = stack;
  exo->ex_context   = context;
  exo->ex_moment    = moment;
  exo->ex_thread    = thread;
  exo->ex_timebase  = timebase;
  exo->ex_session   = sid;
  return LISP_CONS(exo);
}

static int dtype_exception(struct FD_OUTBUF *out,lispval x)
{
  struct FD_EXCEPTION *xo = (struct FD_EXCEPTION *)x;
  u8_condition condition = xo->ex_condition;
  u8_context caller = xo->ex_caller;
  u8_string details = xo->ex_details;
  lispval irritant = xo->ex_irritant;
  lispval backtrace = xo->ex_stack;
  lispval context = xo->ex_context;
  u8_string session = (xo->ex_session) ? (xo->ex_session) : (u8_sessionid());
  time_t timebase = xo->ex_timebase;
  double moment = xo->ex_moment;
  int veclen = (FD_VOIDP(irritant)) ? (8) : (9);
  lispval vector = fd_init_vector(NULL,veclen,NULL);
  int n_bytes;
  FD_VECTOR_SET(vector,0,fd_intern(condition));
  if (caller) {
    FD_VECTOR_SET(vector,1,fd_intern(caller));}
  else {FD_VECTOR_SET(vector,1,FD_FALSE);}
  if (details) {
    FD_VECTOR_SET(vector,2,lispval_string(details));}
  else {FD_VECTOR_SET(vector,2,FD_FALSE);}
  FD_VECTOR_SET(vector,3,irritant);
  if (!(VOIDP(backtrace)))
    FD_VECTOR_SET(vector,4,fd_incref(backtrace));
  else FD_VECTOR_SET(vector,4,FD_FALSE);
  FD_VECTOR_SET(vector,5,fd_make_string(NULL,-1,session));
  FD_VECTOR_SET(vector,6,fd_time2timestamp(timebase));
  FD_VECTOR_SET(vector,7,fd_make_flonum(moment));
  if (!(VOIDP(context)))
    FD_VECTOR_SET(vector,8,fd_incref(context));
  else FD_VECTOR_SET(vector,8,FD_FALSE);
  fd_write_byte(out,dt_exception);
  n_bytes = 1+fd_write_dtype(out,vector);
  fd_decref(vector);
  return n_bytes;
}

FD_EXPORT lispval fd_restore_exception_dtype(lispval content)
{
  /* Return an exception object if possible (content as expected)
     and a compound if there are any big surprises */
  u8_condition condname=_("Poorly Restored Error");
  u8_context caller = NULL; u8_string details = NULL;
  lispval irritant = VOID, stack = VOID, context = VOID;
  u8_string sessionid = NULL;
  double moment = -1.0;
  time_t timebase = -1;
  if (FD_TYPEP(content,fd_exception_type))
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
        lispval tmp = fd_probe_symbol(CSTRING(condval),STRLEN(condval));
        if (FD_VOIDP(tmp)) {
          u8_log(LOG_WARN,fd_DTypeError,"Bad non-symbolic condition name",
                 condval);
          tmp=fd_intern(CSTRING(condval));}
        condname = SYM_NAME(tmp);}
      else {
        u8_log(LOG_WARN,fd_DTypeError,
               "Bad condition (not a symbol) %q in exception serialization %q",
               condval,content);
        condname="BadCondName";}}
    if (len>1) {
      lispval caller_val = VEC_REF(content,0);
      if (SYMBOLP(caller_val))
        caller = SYM_NAME(caller_val);
      else if (STRINGP(caller_val)) {
        lispval tmp =
          fd_probe_symbol(CSTRING(caller_val),STRLEN(caller_val));
        if (FD_VOIDP(tmp)) {
          u8_log(LOG_WARN,fd_DTypeError,"Bad non-symbolic caller",
                 caller_val);
          tmp=fd_intern(CSTRING(caller_val));}
        caller = SYM_NAME(tmp);}
      else if ( (FD_FALSEP(caller_val)) ||
                (FD_EMPTYP(caller_val)) ||
                (caller_val == FD_EMPTY_LIST) ||
                (FD_VOIDP(caller_val)) )
        caller = NULL;
      else {
        u8_log(LOG_WARN,fd_DTypeError,
               "Bad caller (not a symbol) %q in exception serialization %q",
               caller_val,content);
         caller="BadCaller";}}
    if (len > 2) {
      lispval details_val = VEC_REF(content,2);
      if (FD_STRINGP(details_val))
        details = u8_strdup(CSTRING(details_val));
      else details = NULL;}
    if (len > 3) irritant = VEC_REF(content,3);
    if ( (len > 4) && (FD_STRINGP(VEC_REF(content,4))) )
      sessionid = CSTRING(VEC_REF(content,4));
    if (len > 5) {
      lispval tstamp = VEC_REF(content,5);
      if (TYPEP(tstamp,fd_timestamp_type)) {
        struct FD_TIMESTAMP *ts = (fd_timestamp) tstamp;
        struct U8_XTIME *xt = &(ts->u8xtimeval);
        timebase = xt->u8_tick;}
      else if (FD_FIXNUMP(tstamp))
        timebase = (time_t) fd_getint(tstamp);
      else timebase=-1;}
    if ( ( len > 6 ) && (FD_FLONUMP(VEC_REF(content,6))) ) {
      lispval flonum = VEC_REF(content,6);
      moment = FD_FLONUM(flonum);}
    if (len > 7) stack = VEC_REF(content,7);
    if (len > 8) context = VEC_REF(content,8);
    return fd_init_exception(NULL,condname,caller,
                             details,irritant,
                             stack,context,
                             sessionid,moment,-1,
                             timebase);}
  else if (FD_SYMBOLP(content))
    return fd_init_exception
      (NULL,FD_SYMBOL_NAME(content),
       NULL,NULL,content,
       FD_VOID,FD_VOID,NULL,
       -1,-1,-1);
  else if (FD_STRINGP(content))
    return fd_init_exception
      (NULL,"DtypeError",NULL,
       u8_strdup(FD_CSTRING(content)),content,
       FD_VOID,FD_VOID,NULL,
       -1,-1,-1);
  else return fd_init_exception
         (NULL,fd_DTypeError,
          "fd_restore_exception_dtype",NULL,content,
          FD_VOID,FD_VOID,NULL,
          u8_elapsed_time(),
          u8_threadid(),
          u8_elapsed_base());
}

static lispval copy_exception(lispval x,int deep)
{
  struct FD_EXCEPTION *xo=
    fd_consptr(struct FD_EXCEPTION *,x,fd_exception_type);
  return fd_init_exception(NULL,xo->ex_condition,xo->ex_caller,
                           u8_strdup(xo->ex_details),
                           fd_incref(xo->ex_irritant),
                           fd_incref(xo->ex_stack),
                           fd_incref(xo->ex_context),
                           xo->ex_session,xo->ex_moment,xo->ex_thread,
                           xo->ex_timebase);
}

static int unparse_exception(struct U8_OUTPUT *out,lispval x)
{
  struct FD_EXCEPTION *xo=
    fd_consptr(struct FD_EXCEPTION *,x,fd_exception_type);
  u8_condition condition = xo->ex_condition;
  u8_context caller = xo->ex_caller;
  u8_string details = xo->ex_details;
  lispval irritant = xo->ex_irritant;
  u8_puts(out,"#<!EXCEPTION %s");
  if (condition) u8_printf(out," %s",condition);
  else u8_puts(out," missingCondition");
  if (caller) u8_printf(out," @%s",caller);
  if (details) u8_printf(out," (%s)",details);
  if (!(VOIDP(irritant))) {
    if (max_irritant_len==0) {}
    else if (max_irritant_len<0)
      u8_printf(out," %q",irritant);
    else {
      u8_byte buf[max_irritant_len+1];
      u8_sprintf(buf,max_irritant_len,"%q",irritant);
      u8_printf(out," %s...",buf);}}
  u8_printf(out,"!>");
  return 1;
}

/* Converting signals to exceptions */

struct sigaction sigaction_raise, sigaction_abort;
struct sigaction sigaction_exit, sigaction_default;
static sigset_t sigcatch_set, sigexit_set, sigdefault_set, sigabort_set;

static sigset_t default_sigmask;
sigset_t *fd_default_sigmask = &default_sigmask;

struct sigaction *fd_sigaction_raise = &sigaction_raise;
struct sigaction *fd_sigaction_abort = &sigaction_abort;
struct sigaction *fd_sigaction_exit = &sigaction_exit;
struct sigaction *fd_sigaction_default = &sigaction_default;

static void siginfo_raise(int signum,siginfo_t *info,void *stuff)
{
  u8_contour c = u8_dynamic_contour;
  u8_condition ex = u8_signal_name(signum);
  if (!(c)) {
    u8_log(LOG_CRIT,ex,"Unexpected signal");
    exit(1);}
  else {
    u8_raise(ex,c->u8c_label,NULL);}
  exit(1);
}

static void siginfo_exit(int signum,siginfo_t *info,void *stuff)
{
  fd_signal_doexit(signum);
  exit(0);
}

static void siginfo_abort(int signum,siginfo_t *info,void *stuff)
{
  abort();
}

/* Signal handling configs */

static lispval sigmask2dtype(sigset_t *mask)
{
  lispval result = EMPTY;
  int sig = 1; while (sig<32) {
    if (sigismember(mask,sig)) {
      CHOICE_ADD(result,fd_intern(u8_signal_name(sig)));}
    sig++;}
  return result;
}

static int arg2signum(lispval arg)
{
  long long sig = -1;
  if (FIXNUMP(arg))
    sig = FIX2INT(arg);
  else if (SYMBOLP(arg))
    sig = u8_name2signal(SYM_NAME(arg));
  else if (STRINGP(arg))
    sig = u8_name2signal(CSTRING(arg));
  else sig = -1;
  if ((sig>1)&&(sig<32))
    return sig;
  else {
    fd_seterr(fd_TypeError,"arg2signum",NULL,arg);
    return -1;}
}

static lispval sigconfig_getfn(lispval var,void *data)
{
  sigset_t *mask = (sigset_t *)data;
  return sigmask2dtype(mask);
}

static int sigconfig_setfn(lispval var,lispval val,
                           sigset_t *mask,
                           struct sigaction *action,
                           u8_string caller)
{
  int sig = arg2signum(val);
  if (sig<0) return sig;
  if (sigismember(mask,sig))
    return 0;
  else {
    sigaction(sig,action,NULL);
    sigaddset(mask,sig);
    if (mask!= &sigcatch_set) sigdelset(&sigcatch_set,sig);
    if (mask!= &sigexit_set) sigdelset(&sigexit_set,sig);
    if (mask!= &sigabort_set) sigdelset(&sigabort_set,sig);
    if (mask!= &sigdefault_set) sigdelset(&sigdefault_set,sig);
    return 1;}
}

static int sigconfig_catch_setfn(lispval var,lispval val,void *data)
{
  sigset_t *mask = (sigset_t *)data;
  return sigconfig_setfn(var,val,mask,&sigaction_raise,
                         "sigconfig_catch_setfn");
}

static int sigconfig_exit_setfn(lispval var,lispval val,void *data)
{
  sigset_t *mask = (sigset_t *)data;
  return sigconfig_setfn(var,val,mask,&sigaction_exit,
                         "sigconfig_exit_setfn");
}

static int sigconfig_abort_setfn(lispval var,lispval val,void *data)
{
  sigset_t *mask = (sigset_t *)data;
  return sigconfig_setfn(var,val,mask,&sigaction_abort,
                         "sigconfig_abort_setfn");
}
static int sigconfig_default_setfn(lispval var,lispval val,void *data)
{
  sigset_t *mask = (sigset_t *)data;
  return sigconfig_setfn(var,val,mask,&sigaction_default,
                         "sigconfig_default_setfn");
}

void fd_init_err_c()
{
  u8_register_source_file(_FILEINFO);

  fd_copiers[fd_exception_type]=copy_exception;
  if (fd_dtype_writers[fd_exception_type]==NULL)
    fd_dtype_writers[fd_exception_type]=dtype_exception;
  if (fd_unparsers[fd_exception_type]==NULL)
    fd_unparsers[fd_exception_type]=unparse_exception;

  stacktrace_symbol=fd_intern("%%STACK");

  /* Setup sigaction handler */

  memset(&sigaction_raise,0,sizeof(struct sigaction));
  memset(&sigaction_abort,0,sizeof(struct sigaction));
  memset(&sigaction_exit,0,sizeof(struct sigaction));
  memset(&sigaction_default,0,sizeof(struct sigaction));

  sigemptyset(&sigcatch_set);
  sigemptyset(&sigexit_set);
  sigemptyset(&sigabort_set);
  sigemptyset(&sigdefault_set);

  /* Setup sigaction for converting signals to u8_raise (longjmp or exit) */
  sigaction_raise.sa_sigaction = siginfo_raise;
  sigaction_raise.sa_flags = SA_SIGINFO;
  sigemptyset(&(sigaction_raise.sa_mask));

  /* Setup sigaction for default action */
  sigaction_default.sa_handler = SIG_DFL;
  sigemptyset(&(sigaction_default.sa_mask));

  /* Setup sigaction for exit action */
  sigaction_exit.sa_sigaction = siginfo_exit;
  sigaction_exit.sa_flags = SA_SIGINFO;
  sigemptyset(&(sigaction_exit.sa_mask));

  /* Setup sigaction for exit action */
  sigaction_abort.sa_sigaction = siginfo_abort;
  sigaction_abort.sa_flags = SA_SIGINFO;
  sigemptyset(&(sigaction_abort.sa_mask));

  /* Default exit actions */

  sigaddset(&(sigaction_abort.sa_mask),SIGSEGV);
  sigaction(SIGSEGV,&(sigaction_abort),NULL);

  sigaddset(&(sigaction_abort.sa_mask),SIGILL);
  sigaction(SIGILL,&(sigaction_abort),NULL);

  sigaddset(&(sigaction_abort.sa_mask),SIGFPE);
  sigaction(SIGFPE,&(sigaction_abort),NULL);

  sigaddset(&(sigaction_exit.sa_mask),SIGTERM);
  sigaction(SIGTERM,&(sigaction_exit),NULL);

  sigaddset(&(sigaction_abort.sa_mask),SIGQUIT);
  sigaction(SIGQUIT,&(sigaction_abort),NULL);

#ifdef SIGBUS
  sigaddset(&(sigaction_abort.sa_mask),SIGBUS);
  sigaction(SIGBUS,&(sigaction_abort),NULL);
#endif

  /* The default sigmask is masking everything but synchronous
     signals */
  sigfillset(&default_sigmask);
  sigdelset(&default_sigmask,SIGSEGV);
  sigdelset(&default_sigmask,SIGILL);
  sigdelset(&default_sigmask,SIGFPE);
#ifdef SIGBUS
  sigaddset(&default_sigmask,SIGBUS);
#endif

  fd_register_config
    ("SIGCATCH",_("SIGNALS to catch and return as errors"),
     sigconfig_getfn,sigconfig_catch_setfn,
     &sigcatch_set);
  fd_register_config
    ("SIGEXIT",_("Signals to trigger exits"),
     sigconfig_getfn,sigconfig_exit_setfn,
     &sigexit_set);
  fd_register_config
    ("SIGABORT",_("Signals to trigger exits"),
     sigconfig_getfn,sigconfig_abort_setfn,
     &sigabort_set);
  fd_register_config
    ("SIGDEFAULT",_("Signals to trigger exits"),
     sigconfig_getfn,sigconfig_default_setfn,
     &sigexit_set);

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
