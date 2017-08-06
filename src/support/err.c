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
#include <libu8/u8netfns.h>
#include <libu8/u8printf.h>
#if FD_FILECONFIG_ENABLED
#include <libu8/u8filefns.h>
#include <libu8/libu8io.h>
#endif

static int max_irritant_len=256;

static lispval stacktrace_symbol;

/* Managing error data */

FD_EXPORT void fd_free_exception_xdata(void *ptr)
{
  lispval v = (lispval)ptr;
  fd_decref(v);
}

/* This stores the details and irritant arguments directly,
   so they should be dup'd or incref'd by the caller. */
FD_EXPORT void fd_seterr
  (u8_condition c,u8_context cxt,u8_string details,lispval irritant)
{

  u8_condition condition = (c) ? (c) :
    (u8_current_exception) ? (u8_current_exception->u8x_cond) :
    ((u8_condition)"Unknown (NULL) error");
  lispval backtrace = fd_get_backtrace(fd_stackptr);
  lispval exception = fd_make_exception(condition,cxt,details,irritant);
  if (!(VOIDP(irritant))) {
    backtrace = fd_init_pair(NULL,irritant,backtrace);
    fd_incref(irritant);}
  backtrace = fd_init_pair(NULL,exception,backtrace);
  backtrace = fd_init_pair(NULL,stacktrace_symbol,backtrace);
  // TODO: Push the exception and then generate the stack, just in
  // case. Set the exception xdata explicitly if you can.
  u8_push_exception(condition,cxt,u8_strdup(details),
		    (void *)backtrace,fd_free_exception_xdata);
}

/* This stores the details and irritant arguments directly,
   so they should be dup'd or incref'd by the caller. */
FD_EXPORT void fd_pusherr
(u8_condition c,u8_context cxt,u8_string details,lispval irritant)
{
  if (errno) u8_graberrno(cxt,u8dup(details));
  if (VOIDP(irritant))
    u8_push_exception(c,cxt,details,NULL,NULL);
  else {
    fd_incref(irritant);
    u8_push_exception(c,cxt,details,(void *)irritant,
		      fd_free_exception_xdata);}
}

/* This stores the details and irritant arguments directly,
   so they should be dup'd or incref'd by the caller. */
FD_EXPORT void fd_graberr(int _errno,u8_context cxt,u8_string details)
{
  u8_graberr(_errno,cxt,details);
  lispval errinfo = fd_init_pair
    (NULL,stacktrace_symbol,fd_get_backtrace(fd_stackptr));
  u8_push_exception(NULL,cxt,NULL,(void *)errinfo,
		    fd_free_exception_xdata);
}

FD_EXPORT int fd_stacktracep(lispval rep)
{
  if ((PAIRP(rep)) && (FD_CAR(rep) == stacktrace_symbol))
    return 1;
  else return 0;
}

FD_EXPORT lispval fd_get_irritant(u8_exception ex)
{
  lispval irritant = (ex->u8x_free_xdata == fd_free_exception_xdata) ?
    ((lispval) ex->u8x_xdata) : (VOID);
  if (VOIDP(irritant))
    return irritant;
  if (fd_stacktracep(irritant)) {
    lispval head = (PAIRP(FD_CDR(irritant))) ? (FD_CADR(irritant)) : (FD_VOID);
    if (FD_EXCEPTIONP(head)) {
      struct FD_EXCEPTION_OBJECT *embedded = (fd_exception_object) head;
      u8_exception embedded_ex = embedded->ex_u8ex;
      if (embedded_ex->u8x_free_xdata == fd_free_exception_xdata)
	return ((lispval) embedded_ex->u8x_xdata);
      else return FD_FALSE;}
    else return FD_FALSE;}
  else return irritant;
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
        (current->u8x_free_xdata == fd_free_exception_xdata)) {
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
  if ((ex->u8x_xdata) &&
      (ex->u8x_free_xdata == fd_free_exception_xdata))
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
  (fd_exception ex,u8_context cxt,u8_string details,lispval irritant)
{
  if (FD_CHECK_PTR(irritant)) {
    if (details)
      fd_seterr(ex,cxt,details,irritant);
    else fd_seterr(ex,cxt,NULL,irritant);}
  else if (details)
    fd_seterr(ex,cxt,details,VOID);
  else fd_seterr2(ex,cxt);
  return FD_ERROR;
}

FD_EXPORT lispval fd_type_error
  (u8_string type_name,u8_context cxt,lispval irritant)
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
  if (ex->u8x_free_xdata == fd_free_exception_xdata) {
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
      U8_STATIC_OUTPUT(tmp,1000);
      fd_pprint(tmpout,irritant,NULL,0,0,111);
      u8_log(loglevel,ex->u8x_cond,"%s",tmp.u8_outbuf);
      u8_close_output(tmpout);}
    ex=ex->u8x_prev;}
}

FD_EXPORT
lispval fd_exception_backtrace(u8_exception ex)
{
  while (ex) {
    if (ex->u8x_free_xdata == fd_free_exception_xdata) {
      lispval data = (lispval) ex->u8x_xdata;
      if (fd_stacktracep(data))
	return data;}
    ex=ex->u8x_prev;}
  return NIL;
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
    else if (!(VOIDP(irritant))) {
      u8_byte buf[max_irritant_len+1];
      u8_sprintf(buf,max_irritant_len,"%Q",irritant);
      if ( (strchr(buf,'\n')) || (strlen(buf)>40) )
	u8_printf(out,"  %s",buf);
      else u8_printf(out,"\n  %s",buf);}
    else return 0;
    return 1;}
}

FD_EXPORT
void fd_sum_exception(U8_OUTPUT *out,u8_exception ex)
{
  u8_exception scan = ex, prev = NULL;
  while (scan) {
    sum_exception(out,scan,prev); u8_puts(out,"\n");
    prev=scan;
    scan=prev->u8x_prev;}
  if ( (prev) && (prev->u8x_free_xdata == fd_free_exception_xdata )  ) {
    lispval scan = (lispval)(prev->u8x_xdata); int depth=0;
    if (fd_stacktracep(scan)) {
      u8_puts(out,";;* ");
      while (PAIRP(scan)) {
	lispval car=FD_CAR(scan); scan=FD_CDR(scan);
	if (STRINGP(car)) {
	  if (depth) u8_puts(out,"// ");
	  u8_puts(out,CSTRING(car));
	  depth++;
	  if ((depth%7)==0)
	    u8_puts(out," //...\n;;* ...");
	  else u8_putc(out,' ');}}}}
}

FD_EXPORT u8_string fd_errstring(u8_exception ex)
{
  struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,128);
  if (ex == NULL) ex = u8_current_exception;
  if (ex == NULL) return NULL;
  sum_exception(&out,ex,NULL);
  if (ex->u8x_free_xdata == fd_free_exception_xdata) {
    lispval irritant = (lispval) ex->u8x_xdata;
    if (fd_stacktracep(irritant)) {
      lispval scan = FD_CDR(irritant);
      while (PAIRP(scan)) {
	lispval car=FD_CAR(scan); scan=FD_CDR(scan);
	if (STRINGP(car)) {
	  u8_puts(&out," ⇒ ");
	  u8_puts(&out,CSTRING(car));}
	else if (FD_EXCEPTIONP(car)) {
	  struct FD_EXCEPTION_OBJECT *exo=
	    (struct FD_EXCEPTION_OBJECT *)car;
	  u8_exception ex=exo->ex_u8ex;
	  u8_puts(&out," ⇒ ");
	  u8_puts(&out,ex->u8x_cond);
	  if (ex->u8x_context) u8_printf(&out,"@%s",ex->u8x_context);
	  if (ex->u8x_details) u8_printf(&out," (%s)",ex->u8x_details);
	  if (ex->u8x_free_xdata == fd_free_exception_xdata) {
	    lispval irritant=(lispval)ex->u8x_xdata;
	    char buf[32]; buf[0]='\0';
	    u8_sprintf(buf,32," =%q",irritant);
	    u8_puts(&out,buf);}}}}}
  return out.u8_outbuf;
}

FD_EXPORT fd_exception fd_retcode_to_exception(lispval err)
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
  u8_exception ex = u8_erreify(), scan = ex; int n_errs = 0;
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
   (struct FD_EXCEPTION_OBJECT *exo,u8_exception ex)
{
  if (exo == NULL) exo = u8_alloc(struct FD_EXCEPTION_OBJECT);
  FD_INIT_CONS(exo,fd_exception_type);
  exo->ex_u8ex = ex;
  return LISP_CONS(exo);
}

FD_EXPORT lispval fd_make_exception
  (fd_exception c,u8_context cxt,u8_string details,lispval content)
{
  struct FD_EXCEPTION_OBJECT *exo = u8_alloc(struct FD_EXCEPTION_OBJECT);
  u8_exception ex; void *xdata; u8_exception_xdata_freefn freefn;
  if (VOIDP(content)) {
    xdata = NULL; freefn = NULL;}
  else {
    fd_incref(content);
    xdata = (void *) content;
    freefn = fd_free_exception_xdata;}
  ex = u8_make_exception(c,cxt,u8dup(details),xdata,freefn);
  FD_INIT_CONS(exo,fd_exception_type);
  exo->ex_u8ex = ex;
  return LISP_CONS(exo);
}

static int dtype_exception(struct FD_OUTBUF *out,lispval x)
{
  struct FD_EXCEPTION_OBJECT *exo = (struct FD_EXCEPTION_OBJECT *)x;
  if (exo->ex_u8ex == NULL) {
    u8_log(LOG_CRIT,NULL,"Trying to serialize expired exception ");
    fd_write_byte(out,dt_void);
    return 1;}
  else {
    u8_exception ex = exo->ex_u8ex;
    lispval irritant = fd_exception_xdata(ex);
    int veclen = ((VOIDP(irritant)) ? (3) : (4));
    lispval vector = fd_init_vector(NULL,veclen,NULL);
    int n_bytes;
    FD_VECTOR_SET(vector,0,fd_intern((u8_string)(ex->u8x_cond)));
    if (ex->u8x_context) {
      FD_VECTOR_SET(vector,1,fd_intern((u8_string)(ex->u8x_context)));}
    else {FD_VECTOR_SET(vector,1,FD_FALSE);}
    if (ex->u8x_details) {
      FD_VECTOR_SET(vector,2,lispval_string(ex->u8x_details));}
    else {FD_VECTOR_SET(vector,2,FD_FALSE);}
    if (!(VOIDP(irritant)))
      FD_VECTOR_SET(vector,3,fd_incref(irritant));
    fd_write_byte(out,dt_exception);
    n_bytes = 1+fd_write_dtype(out,vector);
    fd_decref(vector);
    return n_bytes;}
}

static u8_exception copy_exception_helper(u8_exception ex,int flags)
{
  u8_exception newex; u8_string details = NULL; lispval irritant;
  if (ex == NULL) return ex;
  if (ex->u8x_details) details = u8_strdup(ex->u8x_details);
  irritant = fd_exception_xdata(ex);
  if (VOIDP(irritant))
    newex = u8_make_exception
      (ex->u8x_cond,ex->u8x_context,details,NULL,NULL);
  else if (flags)
    newex = u8_make_exception
      (ex->u8x_cond,ex->u8x_context,details,
       (void *)fd_copier(irritant,flags),fd_free_exception_xdata);
  else newex = u8_make_exception
         (ex->u8x_cond,ex->u8x_context,details,
          (void *)fd_incref(irritant),fd_free_exception_xdata);
  newex->u8x_prev = copy_exception_helper(ex->u8x_prev,flags);
  return newex;
}

static lispval copy_exception(lispval x,int deep)
{
  struct FD_EXCEPTION_OBJECT *xo=
    fd_consptr(struct FD_EXCEPTION_OBJECT *,x,fd_exception_type);
  return fd_init_exception(NULL,copy_exception_helper(xo->ex_u8ex,deep));
}

static int unparse_exception(struct U8_OUTPUT *out,lispval x)
{
  struct FD_EXCEPTION_OBJECT *xo=
    fd_consptr(struct FD_EXCEPTION_OBJECT *,x,fd_exception_type);
  u8_exception ex = xo->ex_u8ex;
  if (ex == NULL)
    u8_printf(out,"#!!!OLDEXCEPTION");
  else {
    u8_exception ex = xo->ex_u8ex;
    lispval irritant = fd_get_irritant(ex);
    u8_printf(out,"#<!!EXCEPTION %s (%s)",
	      ex->u8x_cond,ex->u8x_context);
    if (ex->u8x_details)
      u8_printf(out,"[%s]",ex->u8x_details);
    if (!(VOIDP(irritant))) {
      if (max_irritant_len==0) {}
      else if (max_irritant_len<0)
	u8_printf(out," %q",irritant);
      else {
	u8_byte buf[max_irritant_len+1];
	u8_sprintf(buf,max_irritant_len,"%q",irritant);
	u8_printf(out," %s...",buf);}}
    u8_printf(out,"!!>");}
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
  sigaction_exit.sa_handler = SIG_DFL;
  sigemptyset(&(sigaction_exit.sa_mask));

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