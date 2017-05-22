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

static fdtype stacktrace_symbol;

static int max_irritant_len=256;

/* Managing error data */

FD_EXPORT void fd_free_exception_xdata(void *ptr)
{
  fdtype v = (fdtype)ptr;
  fd_decref(v);
}

FD_EXPORT void fd_seterr
  (u8_condition c,u8_context cxt,u8_string details,fdtype irritant)
{
  fdtype exception = fd_make_exception(c,cxt,details,irritant);
  fdtype base      = fd_init_pair(NULL,exception,FD_EMPTY_LIST);
  fdtype errinfo   = fd_init_pair(NULL,stacktrace_symbol,
				  fd_get_backtrace(fd_stackptr,base));
  // TODO: Push the exception and then generate the stack, just in
  // case. Set the exception xdata explicitly if you can.
  u8_push_exception(c,cxt,u8dup(details),(void *)errinfo,
		    fd_free_exception_xdata);
  if (details) u8_free(details);
}

FD_EXPORT void fd_xseterr
  (u8_condition c,u8_context cxt,u8_string details,fdtype irritant)
{
  fd_incref(irritant);
  fd_seterr(c,cxt,u8dup(details),irritant);
}

FD_EXPORT fdtype fd_get_irritant(u8_exception ex)
{
  fdtype irritant = (ex->u8x_free_xdata == fd_free_exception_xdata) ?
    ((fdtype) ex->u8x_xdata) : (FD_VOID);
  fdtype use_irritant;
  if (FD_VOIDP(irritant))
    return irritant;
  if ((FD_PAIRP(irritant)) && (FD_PAIRP(FD_CDR(irritant))) &&
      (FD_CAR(irritant) == stacktrace_symbol)) {
    if (FD_TYPEP(FD_CADR(irritant),fd_error_type)) {
      struct FD_EXCEPTION_OBJECT *embedded=
	(struct FD_EXCEPTION_OBJECT *) FD_CADR(irritant);
      u8_exception embedded_ex = embedded->fdex_u8ex;
      if (embedded_ex->u8x_free_xdata == fd_free_exception_xdata)
	return ((fdtype) embedded_ex->u8x_xdata);
      else return FD_VOID;}
    else return FD_VOID;}
  else return irritant;
}

FD_EXPORT void fd_raise
  (u8_condition c,u8_context cxt,u8_string details,fdtype irritant)
{
  fd_seterr(c,cxt,details,irritant);
  u8_raise(c,cxt,u8dup(details));
}

FD_EXPORT int fd_poperr
  (u8_condition *c,u8_context *cxt,u8_string *details,fdtype *irritant)
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
      *irritant = (fdtype)(current->u8x_xdata);
      current->u8x_xdata = NULL;
      current->u8x_free_xdata = NULL;}
    else *irritant = FD_VOID;}
  u8_pop_exception();
  return 1;
}

FD_EXPORT fdtype fd_exception_xdata(u8_exception ex)
{
  if ((ex->u8x_xdata) &&
      (ex->u8x_free_xdata == fd_free_exception_xdata))
    return (fdtype)(ex->u8x_xdata);
  else return FD_VOID;
}

FD_EXPORT int fd_reterr
  (u8_condition c,u8_context cxt,u8_string details,fdtype irritant)
{
  fd_seterr(c,cxt,details,irritant);
  return -1;
}

FD_EXPORT int fd_interr(fdtype x)
{
  return -1;
}

FD_EXPORT fdtype fd_err
  (fd_exception ex,u8_context cxt,u8_string details,fdtype irritant)
{
  if (FD_CHECK_PTR(irritant)) {
    if (details)
      fd_seterr(ex,cxt,u8_strdup(details),fd_incref(irritant));
    else fd_seterr(ex,cxt,NULL,fd_incref(irritant));}
  else if (details)
    fd_seterr(ex,cxt,u8_strdup(details),FD_VOID);
  else fd_seterr2(ex,cxt);
  return FD_ERROR_VALUE;
}

FD_EXPORT void fd_push_error_context(u8_context cxt,u8_string label,fdtype data)
{
  u8_exception ex;
  u8_condition condition = NULL;
  ex = u8_current_exception;
  if (ex) condition = ex->u8x_cond;
  if (condition == NULL) condition = fd_UnknownError;
  if ( condition == fd_StackOverflow ) {
    fd_decref(data);
    return;}
  else if (errno) u8_graberrno(cxt,u8_strdup(label));
  else {}
  if (label) {
    u8_string copied = u8_strdup(label);
    u8_push_exception(NULL,cxt,copied,(void *)data,
                      fd_free_exception_xdata);}
  else u8_push_exception(NULL,cxt,NULL,(void *)data,
                         fd_free_exception_xdata);
}

FD_EXPORT fdtype fd_type_error
  (u8_string type_name,u8_context cxt,fdtype irritant)
{
  u8_string msg = u8_mkstring(_("object is not a %m"),type_name);
  fd_seterr(fd_TypeError,cxt,msg,irritant);
  return FD_TYPE_ERROR;
}

FD_EXPORT void fd_set_type_error(u8_string type_name,fdtype irritant)
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
    fdtype irritant = fd_exception_xdata(ex);
    u8_puts(out,";; ");
    fd_pprint(out,irritant,";; ",0,3,100,0);}
}

FD_EXPORT
void fd_log_exception(u8_exception ex)
{
  if (ex->u8x_xdata) {
    fdtype irritant = fd_exception_xdata(ex);
    u8_log(LOG_WARN,ex->u8x_cond,"%m (%m)\n\t%Q",
           (U8ALT((ex->u8x_details),((U8S0())))),
           (U8ALT((ex->u8x_context),((U8S0())))),
           irritant);}
  else u8_log(LOG_WARN,ex->u8x_cond,"%m (%m)\n\t%q",
              (U8ALT((ex->u8x_details),((U8S0())))),
              (U8ALT((ex->u8x_context),((U8S0())))));
}

FD_EXPORT
fdtype fd_exception_backtrace(u8_exception ex)
{
  fdtype result = FD_EMPTY_LIST;
  u8_condition cond = NULL;  u8_string details = NULL; u8_context cxt = NULL;
  while (ex) {
    u8_condition c = ex->u8x_cond;
    u8_string d = ex->u8x_details;
    u8_context cx = ex->u8x_context;
    fdtype x = fd_exception_xdata(ex);
    if (FD_PAIRP(x)) {
      int i=0, len=fd_list_length(x);
      fdtype vec=fd_make_vector(len,NULL);
      fdtype *data=FD_VECTOR_ELTS(vec);
      fdtype scan=x;
      while (i<len) {
	fdtype v=FD_CAR(scan);
	fd_incref(v);
	data[i]=v;
	scan=FD_CDR(scan);
	i++;}
      return vec;}
    if ((c!=cond)||
        ((d)&&(d!=details))||
        ((cx)&&(cx!=cxt))) {
      u8_string sum=
        (((d)&&cx)?(u8_mkstring("%s (%s) %s",c,cx,d)):
         (d)?(u8_mkstring("%s: %s",c,d)):
         (cx)?(u8_mkstring("%s (%s)",c,cx)):
         ((u8_string)u8_strdup(c)));
      result = fd_conspair(fd_make_string(NULL,-1,sum),result);
      u8_free(sum);}
    if (!((FD_NULLP(x))||(FD_VOIDP(x)))) {
      if (FD_VECTORP(x)) {
        int len = FD_VECTOR_LENGTH(x);
        fdtype applyvec = fd_init_vector(NULL,len+1,NULL);
        int i = 0; while (i<len) {
          fdtype elt = FD_VECTOR_REF(x,i); fd_incref(elt);
          FD_VECTOR_SET(applyvec,i+1,elt);
          i++;}
        FD_VECTOR_SET(applyvec,0,fd_intern("=>"));
        result = fd_conspair(applyvec,result);}
      else {
        fd_incref(x); result = fd_conspair(x,result);}}
    ex = ex->u8x_prev;}
  return result;
}

void sum_exception(U8_OUTPUT *out,u8_exception ex,u8_exception bg)
{
  if (!(ex)) {
    u8_printf(out,"what error?");
    return;}
  else if ((bg == NULL) || ((bg->u8x_cond) != (ex->u8x_cond)))
    u8_printf(out,"(%m)",ex->u8x_cond);
  if ((bg == NULL) || ((bg->u8x_context) != (ex->u8x_context)))
    u8_printf(out," <%s>",ex->u8x_context);
  if ((bg == NULL) || ((bg->u8x_details) != (ex->u8x_details)))
    u8_printf(out," (%m)",ex->u8x_details);
  if (ex->u8x_xdata) {
    fdtype irritant = fd_exception_xdata(ex);
    if ((bg == NULL) || (bg->u8x_xdata == NULL))
      u8_printf(out," -- %q",irritant);
    else {
      fdtype bgirritant = fd_exception_xdata(bg);
      if (!(FD_EQUAL(irritant,bgirritant)))
        u8_printf(out," -- %q",irritant);}}
}

int compact_exception(U8_OUTPUT *out,u8_exception ex,u8_exception bg,
                      int depth,int skipped)
{
  if (!(ex)) return 0;
  else {
    u8_condition cond = ex->u8x_cond;
    u8_context context = ex->u8x_context;
    u8_string details = ex->u8x_details;
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
        details = NULL;}
    if ( (depth) && ((cond) || (context) || (details)) ) {
      if (skipped)
        u8_printf(out," >> %d… >> ",skipped);
      else u8_puts(out," >> ");}
    if ((cond) && (context) && (details))
      u8_printf(out,"%m <%s> (%s)",cond,context,details);
    else if ((cond) && (context))
      u8_printf(out,"%m <%s>",cond,context);
    else if ((cond) && (details))
      u8_printf(out,"%m (%s)",cond,details);
    else if ((context) && (details))
      u8_printf(out,"<%s> (%s)",context,details);
    else if (cond)
      u8_printf(out,"%m",cond);
    else if (context)
      u8_printf(out,"<%s>",context);
    else if (details)
      u8_printf(out,"(%s)",details);
    else return 0;
    return 1;}
}

FD_EXPORT
void fd_sum_exception(U8_OUTPUT *out,u8_exception ex)
{
  u8_exception root = u8_exception_root(ex);
  int show_top = 8, show_bottom = 32;
  int stacklen = u8_exception_stacklen(ex);
  int stackfoot = stacklen-show_bottom;
  int depth = 0, elided = 0, skipped = 0;
  u8_exception scan = ex, prev = NULL;
  sum_exception(out,root,NULL);
  while (scan) {
    if (depth==0) u8_puts(out,"\n");
    if ((depth<show_top)||(depth>stackfoot)) {
      if (compact_exception(out,scan,prev,depth,skipped))
        skipped = 0;
      else skipped++;}
    else if (elided) {}
    else if ((stacklen-(show_bottom+show_top))==0) {
      elided = 1; prev = NULL;}
    else {
      u8_printf(out," >> %d/%d calls... ",
                stacklen-(show_bottom+show_top),
                stacklen);
      skipped = 0;
      elided = 1;}
    prev = scan; scan = prev->u8x_prev;
    depth++;}
  sum_exception(out,ex,root);
}

FD_EXPORT u8_string fd_errstring(u8_exception ex)
{
  struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,128);
  if (ex == NULL) ex = u8_current_exception;
  fd_sum_exception(&out,ex);
  return out.u8_outbuf;
}

FD_EXPORT fd_exception fd_retcode_to_exception(fdtype err)
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

FD_EXPORT fdtype fd_init_exception
   (struct FD_EXCEPTION_OBJECT *exo,u8_exception ex)
{
  if (exo == NULL) exo = u8_alloc(struct FD_EXCEPTION_OBJECT);
  FD_INIT_CONS(exo,fd_error_type);
  exo->fdex_u8ex = ex;
  return FDTYPE_CONS(exo);
}

FD_EXPORT fdtype fd_make_exception
  (fd_exception c,u8_context cxt,u8_string details,fdtype content)
{
  struct FD_EXCEPTION_OBJECT *exo = u8_alloc(struct FD_EXCEPTION_OBJECT);
  u8_exception ex; void *xdata; u8_exception_xdata_freefn freefn;
  if (FD_VOIDP(content)) {
    xdata = NULL; freefn = NULL;}
  else {
    fd_incref(content);
    xdata = (void *) content;
    freefn = fd_free_exception_xdata;}
  ex = u8_make_exception(c,cxt,u8dup(details),xdata,freefn);
  FD_INIT_CONS(exo,fd_error_type);
  exo->fdex_u8ex = ex;
  return FDTYPE_CONS(exo);
}

static int dtype_exception(struct FD_OUTBUF *out,fdtype x)
{
  struct FD_EXCEPTION_OBJECT *exo = (struct FD_EXCEPTION_OBJECT *)x;
  if (exo->fdex_u8ex == NULL) {
    u8_log(LOG_CRIT,NULL,"Trying to serialize expired exception ");
    fd_write_byte(out,dt_void);
    return 1;}
  else {
    u8_exception ex = exo->fdex_u8ex;
    fdtype irritant = fd_exception_xdata(ex);
    int veclen = ((FD_VOIDP(irritant)) ? (3) : (4));
    fdtype vector = fd_init_vector(NULL,veclen,NULL);
    int n_bytes;
    FD_VECTOR_SET(vector,0,fd_intern((u8_string)(ex->u8x_cond)));
    if (ex->u8x_context) {
      FD_VECTOR_SET(vector,1,fd_intern((u8_string)(ex->u8x_context)));}
    else {FD_VECTOR_SET(vector,1,FD_FALSE);}
    if (ex->u8x_details) {
      FD_VECTOR_SET(vector,2,fdtype_string(ex->u8x_details));}
    else {FD_VECTOR_SET(vector,2,FD_FALSE);}
    if (!(FD_VOIDP(irritant)))
      FD_VECTOR_SET(vector,3,fd_incref(irritant));
    fd_write_byte(out,dt_exception);
    n_bytes = 1+fd_write_dtype(out,vector);
    fd_decref(vector);
    return n_bytes;}
}

static u8_exception copy_exception_helper(u8_exception ex,int flags)
{
  u8_exception newex; u8_string details = NULL; fdtype irritant;
  if (ex == NULL) return ex;
  if (ex->u8x_details) details = u8_strdup(ex->u8x_details);
  irritant = fd_exception_xdata(ex);
  if (FD_VOIDP(irritant))
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

static fdtype copy_exception(fdtype x,int deep)
{
  struct FD_EXCEPTION_OBJECT *xo=
    fd_consptr(struct FD_EXCEPTION_OBJECT *,x,fd_error_type);
  return fd_init_exception(NULL,copy_exception_helper(xo->fdex_u8ex,deep));
}

static int unparse_exception(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_EXCEPTION_OBJECT *xo=
    fd_consptr(struct FD_EXCEPTION_OBJECT *,x,fd_error_type);
  u8_exception ex = xo->fdex_u8ex;
  if (ex == NULL)
    u8_printf(out,"#!!!OLDEXCEPTION");
  else {
    u8_exception ex = xo->fdex_u8ex;
    fdtype irritant = fd_get_irritant(ex);
    u8_printf(out,"#<!!EXCEPTION %s (%s)",
	      ex->u8x_cond,ex->u8x_context);
    if (ex->u8x_details)
      u8_printf(out,"[%s]",ex->u8x_details);
    if (!(FD_VOIDP(irritant))) {
      if (max_irritant_len==0) {}
      else if (max_irritant_len<0)
	u8_printf(out," %q",irritant);
      else {
	u8_byte buf[max_irritant_len+1];
	u8_sprintf(buf,max_irritant_len,"%q");
	u8_printf(out," %s...",buf);}}
    u8_printf(out,"!!>");}
  return 1;
}

/* Converting signals to exceptions */

struct sigaction sigaction_catch, sigaction_exit, sigaction_default;
static sigset_t sigcatch_set, sigexit_set, sigdefault_set;

static sigset_t default_sigmask;
sigset_t *fd_default_sigmask = &default_sigmask;

struct sigaction *fd_sigaction_catch = &sigaction_catch;
struct sigaction *fd_sigaction_exit = &sigaction_catch;
struct sigaction *fd_sigaction_default = &sigaction_catch;

static void siginfo_raise(int signum,siginfo_t *info,void *stuff)
{
  u8_contour c = u8_dynamic_contour;
  u8_condition ex = u8_signal_name(signum);
  if (!(c)) {
    u8_log(LOG_CRIT,ex,"Unexpected signal");
    exit(1);}
  else {
    u8_raise(ex,c->u8c_label,NULL);}
}

static void siginfo_exit(int signum,siginfo_t *info,void *stuff)
{
  u8_contour c = u8_dynamic_contour;
  u8_condition ex = u8_signal_name(signum);
  if (!(c)) {
    u8_log(LOG_CRIT,ex,"Unexpected signal");}
  else {
    u8_raise(ex,c->u8c_label,NULL);}
  exit(1);
}

/* Signal handling configs */

static fdtype sigmask2dtype(sigset_t *mask)
{
  fdtype result = FD_EMPTY_CHOICE;
  int sig = 1; while (sig<32) {
    if (sigismember(mask,sig)) {
      FD_ADD_TO_CHOICE(result,fd_intern(u8_signal_name(sig)));}
    sig++;}
  return result;
}

static int arg2signum(fdtype arg)
{
  long long sig = -1;
  if (FD_FIXNUMP(arg))
    sig = FD_FIX2INT(arg);
  else if (FD_SYMBOLP(arg))
    sig = u8_name2signal(FD_SYMBOL_NAME(arg));
  else if (FD_STRINGP(arg))
    sig = u8_name2signal(FD_STRDATA(arg));
  else sig = -1;
  if ((sig>1)&&(sig<32))
    return sig;
  else {
    fd_seterr(fd_TypeError,"arg2signum",NULL,arg);
    return -1;}
}

static fdtype sigconfig_getfn(fdtype var,void *data)
{
  sigset_t *mask = (sigset_t *)data;
  return sigmask2dtype(mask);
}

static int sigconfig_setfn(fdtype var,fdtype val,
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
    if (mask!= &sigdefault_set) sigdelset(&sigdefault_set,sig);
    return 1;}
}

static int sigconfig_catch_setfn(fdtype var,fdtype val,void *data)
{
  sigset_t *mask = (sigset_t *)data;
  return sigconfig_setfn(var,val,mask,&sigaction_catch,
                         "sigconfig_catch_setfn");
}

static int sigconfig_exit_setfn(fdtype var,fdtype val,void *data)
{
  sigset_t *mask = (sigset_t *)data;
  return sigconfig_setfn(var,val,mask,&sigaction_exit,
                         "sigconfig_exit_setfn");
}

static int sigconfig_default_setfn(fdtype var,fdtype val,void *data)
{
  sigset_t *mask = (sigset_t *)data;
  return sigconfig_setfn(var,val,mask,&sigaction_default,
                         "sigconfig_default_setfn");
}

void fd_init_err_c()
{
  u8_register_source_file(_FILEINFO);

  fd_copiers[fd_error_type]=copy_exception;
  if (fd_dtype_writers[fd_error_type]==NULL)
    fd_dtype_writers[fd_error_type]=dtype_exception;
  if (fd_unparsers[fd_error_type]==NULL)
    fd_unparsers[fd_error_type]=unparse_exception;

  stacktrace_symbol=fd_intern("%%STACK");

  /* Setup sigaction handler */

  memset(&sigaction_catch,0,sizeof(struct sigaction));
  memset(&sigaction_exit,0,sizeof(struct sigaction));
  memset(&sigaction_default,0,sizeof(struct sigaction));

  sigemptyset(&sigcatch_set);
  sigemptyset(&sigexit_set);
  sigemptyset(&sigdefault_set);

  /* Setup sigaction for converting signals to u8_raise (longjmp or exit) */
  sigaction_catch.sa_sigaction = siginfo_raise;
  sigaction_catch.sa_flags = SA_SIGINFO;
  sigemptyset(&(sigaction_catch.sa_mask));

  /* Setup sigaction for default action */
  sigaction_exit.sa_handler = SIG_DFL;
  sigemptyset(&(sigaction_exit.sa_mask));

  /* Setup sigaction for exit action */
  sigaction_exit.sa_sigaction = siginfo_exit;
  sigaction_exit.sa_flags = SA_SIGINFO;
  sigemptyset(&(sigaction_exit.sa_mask));

  /* Default exit actions */

  sigaddset(&(sigaction_exit.sa_mask),SIGSEGV);
  sigaction(SIGSEGV,&(sigaction_exit),NULL);

  sigaddset(&(sigaction_exit.sa_mask),SIGILL);
  sigaction(SIGILL,&(sigaction_exit),NULL);

  sigaddset(&(sigaction_exit.sa_mask),SIGFPE);
  sigaction(SIGFPE,&(sigaction_exit),NULL);

#ifdef SIGBUS
  sigaddset(&(sigaction_exit.sa_mask),SIGBUS);
  sigaction(SIGBUS,&(sigaction_exit),NULL);
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
    ("SIGCATCH",_("Errors to catch and return as errors"),
     sigconfig_getfn,sigconfig_catch_setfn,
     &sigcatch_set);
  fd_register_config
    ("SIGEXIT",_("Errors to trigger exists"),
     sigconfig_getfn,sigconfig_exit_setfn,
     &sigexit_set);
  fd_register_config
    ("SIGDEFAULT",_("Errors to trigger exists"),
     sigconfig_getfn,sigconfig_default_setfn,
     &sigexit_set);

}
