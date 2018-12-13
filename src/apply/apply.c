/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_FCNIDS 1
#define FD_INLINE_STACKS 1
#define FD_INLINE_LEXENV 1
#define FD_INLINE_APPLY  1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/lexenv.h"
#include "framerd/stacks.h"
#include "framerd/profiles.h"
#include "framerd/apply.h"

#include <libu8/u8printf.h>
#include <libu8/u8contour.h>
#include <libu8/u8strings.h>

#include <errno.h>

#if HAVE_UNISTD_H
#include <unistd.h>
#endif

#if TIME_WITH_SYS_TIME
#include <time.h>
#include <sys/time.h>
#elif HAVE_TIME_H
#include <time.h>
#elif HAVE_SYS_TIME_H
#include <sys/time.h>
#endif

#include <sys/resource.h>
#include <stdarg.h>

lispval fd_default_stackspec = VOID;
u8_string fd_ndcallstack_type = "ndapply";
u8_string fd_callstack_type   = "apply";

#define FD_APPLY_STACK(name,fname,fn)                   \
  FD_PUSH_STACK(name,fd_callstack_type,fname,fn)

fd_applyfn fd_applyfns[FD_TYPE_MAX];
/* This is set if the type is a CONS with a FUNCTION header */
short fd_functionp[FD_TYPE_MAX];

u8_condition fd_NotAFunction=_("calling a non function");
u8_condition fd_TooManyArgs=_("too many arguments");
u8_condition fd_TooFewArgs=_("too few arguments");
u8_condition fd_NoDefault=_("no default value for #default argument");
u8_condition fd_ProfilingDisabled=_("profiling not built");

u8_condition fd_NoStackChecking=
  _("Stack checking is not available in this build of FramerD");
u8_condition fd_StackTooSmall=
  _("This value is too small for an application stack limit");
u8_condition fd_StackTooLarge=
  _("This value is larger than the available stack space");
u8_condition fd_BadStackSize=
  _("This is an invalid stack size");
u8_condition fd_BadStackFactor=
  _("This is an invalid stack resize factor");
u8_condition fd_InternalStackSizeError=
  _("Internal stack size didn't make any sense, punting");

#if ((FD_THREADS_ENABLED)&&(FD_USE_TLS))
u8_tld_key fd_stack_limit_key;
#elif ((FD_THREADS_ENABLED)&&(HAVE_THREAD_STORAGE_CLASS))
__thread ssize_t fd_stack_limit = -1;
#else
ssize_t fd_stack_limit = -1;
#endif

static lispval moduleid_symbol;

/* Whether to always profile */
int fd_profiling = 0;

/* Stack checking */

int fd_wrap_apply = FD_WRAP_APPLY_DEFAULT;

#if FD_STACKCHECK
static int stackcheck()
{
  if (fd_stack_limit>=FD_MIN_STACKSIZE) {
    ssize_t depth = u8_stack_depth();
    if (depth>fd_stack_limit)
      return 0;
    else return 1;}
  else return 1;
}
FD_EXPORT ssize_t fd_stack_getsize()
{
  if (fd_stack_limit>=FD_MIN_STACKSIZE)
    return fd_stack_limit;
  else return -1;
}
FD_EXPORT ssize_t fd_stack_setsize(ssize_t limit)
{
  if (limit<FD_MIN_STACKSIZE) {
    char *detailsbuf = u8_malloc(64);
    u8_seterr("StackLimitTooSmall","fd_stack_setsize",
              u8_write_long_long(limit,detailsbuf,64));
    return -1;}
  else {
    ssize_t maxstack = u8_stack_size;
    if (maxstack < FD_MIN_STACKSIZE) {
      u8_seterr(fd_InternalStackSizeError,"fd_stack_resize",NULL);
      return -1;}
    else if (limit <=  maxstack) {
      ssize_t old = fd_stack_limit;
      fd_set_stack_limit(limit);
      return old;}
    else {
#if ( (HAVE_PTHREAD_SELF) &&       \
      (HAVE_PTHREAD_GETATTR_NP) &&              \
      (HAVE_PTHREAD_ATTR_SETSTACKSIZE) )
      pthread_t self = pthread_self();
      pthread_attr_t attr;
      if (pthread_getattr_np(self,&attr)) {
        u8_graberrno("fd_set_stacksize",NULL);
        return -1;}
      else if ((pthread_attr_setstacksize(&attr,limit))) {
        u8_graberrno("fd_set_stacksize",NULL);
        return -1;}
      else {
        ssize_t old = fd_stack_limit;
        fd_set_stack_limit(limit-limit/8);
        return old;}
#else
      char *detailsbuf = u8_malloc(64);
      u8_seterr("StackLimitTooLarge","fd_stack_setsize",
                u8_write_long_long(limit,detailsbuf,64));
      return -1;
#endif
    }}
}
FD_EXPORT ssize_t fd_stack_resize(double factor)
{
  if ( (factor<0) || (factor>1000) ) {
    u8_log(LOG_WARN,fd_BadStackFactor,
           "The value %f is not a valid stack resize factor",factor);
    return fd_stack_limit;}
  else {
    ssize_t current = fd_stack_limit;
    ssize_t limit = u8_stack_size;
    if (limit<FD_MIN_STACKSIZE) limit = current;
    return fd_stack_setsize((limit*factor));}
}
#else
static int youve_been_warned = 0;
#define stackcheck() (1)
FD_EXPORT ssize_t fd_stack_getsize()
{
  if (youve_been_warned) return -1;
  u8_log(LOG_WARN,"NoStackChecking",
         "Stack checking is not enabled in this build");
  youve_been_warned = 1;
  return -1;
}
FD_EXPORT ssize_t fd_stack_setsize(ssize_t limit)
{
  u8_log(LOG_WARN,"NoStackChecking",
         "Stack checking is not enabled in this build");
  return -1;
}
FD_EXPORT ssize_t fd_stack_resize(double factor)
{
  if (factor<0) {
    u8_log(LOG_WARN,"NoStackChecking",
           "Stack checking is not enabled in this build and the value you provided (%f) is invalid anyway",
           factor);}
  else if (factor<=1000) {
    u8_log(LOG_WARN,"StackResizeTooLarge",
           "Stack checking is not enabled in this build and the value you provided (%f) is too big anyway",
           factor);}
  else {
    u8_log(LOG_WARN,"NoStackChecking",
           "Stack checking is not enabled in this build so setting the limit doesn't do anything",
           factor);}
  return -1;
}
#endif

FD_EXPORT int fd_stackcheck()
{
  return stackcheck();
}

/* Initialize stack limits */

FD_EXPORT ssize_t fd_init_cstack()
{
  u8_init_stack();
  if (VOIDP(fd_default_stackspec)) {
    ssize_t stacksize = u8_stack_size;
    return fd_stack_setsize(stacksize-stacksize/8);}
  else if (FIXNUMP(fd_default_stackspec))
    return fd_stack_setsize((ssize_t)(FIX2INT(fd_default_stackspec)));
  else if (FD_FLONUMP(fd_default_stackspec))
    return fd_stack_resize(FD_FLONUM(fd_default_stackspec));
  else if (FD_BIGINTP(fd_default_stackspec)) {
    unsigned long long val = fd_getint(fd_default_stackspec);
    if (val) return fd_stack_setsize((ssize_t) val);
    else {
      u8_log(LOG_CRIT,fd_BadStackSize,
             "The default stack value %q wasn't a valid stack size",
             fd_default_stackspec);
      return -1;}}
  else {
    u8_log(LOG_CRIT,fd_BadStackSize,
           "The default stack value %q wasn't a valid stack size",
           fd_default_stackspec);
    return -1;}
}

static int init_thread_stack_limit()
{
  fd_init_cstack();
  return 1;
}

/* Call stack (not used yet) */

FD_EXPORT void _fd_free_stack(struct FD_STACK *stack)
{
  fd_pop_stack(stack);
}

FD_EXPORT void _fd_pop_stack(struct FD_STACK *stack)
{
  fd_pop_stack(stack);
}

#if 0
FD_EXPORT struct FD_STACK_CLEANUP *_fd_add_cleanup
(struct FD_STACK *stack,fd_stack_cleanop op,void *arg0,void *arg1)
{
  return fd_add_cleanup(stack,op,arg0,arg1);
}
#endif

/* Calling primitives */

static lispval dcall0(struct FD_FUNCTION *f)
{
  if (PRED_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall0(f);
  else return f->fcn_handler.call0();
}
static lispval dcall1(struct FD_FUNCTION *f,lispval arg1)
{
  if (PRED_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall1(f,arg1);
  else return f->fcn_handler.call1(arg1);
}
static lispval dcall2(struct FD_FUNCTION *f,lispval arg1,lispval arg2)
{
  if (PRED_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall2(f,arg1,arg2);
  else return f->fcn_handler.call2(arg1,arg2);
}
static lispval dcall3(struct FD_FUNCTION *f,
                      lispval arg1,lispval arg2,lispval arg3)
{
  if (PRED_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall3(f,arg1,arg2,arg3);
  else return f->fcn_handler.call3(arg1,arg2,arg3);
}
static lispval dcall4(struct FD_FUNCTION *f,
                      lispval arg1,lispval arg2,lispval arg3,lispval arg4)
{
  if (PRED_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall4(f,arg1,arg2,arg3,arg4);
  else return f->fcn_handler.call4(arg1,arg2,arg3,arg4);
}
static lispval dcall5(struct FD_FUNCTION *f,
                      lispval arg1,lispval arg2,lispval arg3,lispval arg4,
                      lispval arg5)
{
  if (PRED_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall5(f,arg1,arg2,arg3,arg4,arg5);
  else return f->fcn_handler.call5(arg1,arg2,arg3,arg4,arg5);
}
static lispval dcall6(struct FD_FUNCTION *f,
                      lispval arg1,lispval arg2,lispval arg3,lispval arg4,
                      lispval arg5,lispval arg6)
{
  if (PRED_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall6(f,arg1,arg2,arg3,arg4,arg5,arg6);
  else return f->fcn_handler.call6(arg1,arg2,arg3,arg4,arg5,arg6);
}

static lispval dcall7(struct FD_FUNCTION *f,
                      lispval arg1,lispval arg2,lispval arg3,lispval arg4,
                      lispval arg5,lispval arg6,lispval arg7)
{
  if (PRED_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall7(f,arg1,arg2,arg3,arg4,arg5,arg6,arg7);
  else return f->fcn_handler.call7(arg1,arg2,arg3,arg4,arg5,arg6,arg7);
}

static lispval dcall8(struct FD_FUNCTION *f,
                      lispval arg1,lispval arg2,lispval arg3,lispval arg4,
                      lispval arg5,lispval arg6,lispval arg7,lispval arg8)
{
  if (PRED_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall8(f,arg1,arg2,arg3,arg4,arg5,arg6,arg7,arg8);
  else return f->fcn_handler.call8(arg1,arg2,arg3,arg4,arg5,arg6,arg7,arg8);
}

static lispval dcall9(struct FD_FUNCTION *f,
                      lispval arg1,lispval arg2,lispval arg3,lispval arg4,
                      lispval arg5,lispval arg6,lispval arg7,lispval arg8,
                      lispval arg9)
{
  if (PRED_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall9(f,arg1,arg2,arg3,
                                 arg4,arg5,arg6,
                                 arg7,arg8,arg9);
  else return f->fcn_handler.call9(arg1,arg2,arg3,
                                   arg4,arg5,arg6,
                                   arg7,arg8,arg9);
}

static lispval dcall10(struct FD_FUNCTION *f,
                       lispval arg1,lispval arg2,lispval arg3,
                       lispval arg4,lispval arg5,lispval arg6,
                       lispval arg7,lispval arg8,lispval arg9,
                       lispval arg10)
{
  if (PRED_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall10(f,arg1,arg2,arg3,
                                  arg4,arg5,arg6,
                                  arg7,arg8,arg9,
                                  arg10);
  else return f->fcn_handler.call10(arg1,arg2,arg3,
                                    arg4,arg5,arg6,
                                    arg7,arg8,arg9,
                                    arg10);
}

static lispval dcall11(struct FD_FUNCTION *f,
                       lispval arg1,lispval arg2,lispval arg3,
                       lispval arg4,lispval arg5,lispval arg6,
                       lispval arg7,lispval arg8,lispval arg9,
                       lispval arg10,lispval arg11)
{
  if (PRED_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall11(f,arg1,arg2,arg3,
                                  arg4,arg5,arg6,
                                  arg7,arg8,arg9,
                                  arg10,arg11);
  else return f->fcn_handler.call11(arg1,arg2,arg3,
                                    arg4,arg5,arg6,
                                    arg7,arg8,arg9,
                                    arg10,arg11);
}

static lispval dcall12(struct FD_FUNCTION *f,
                       lispval arg1,lispval arg2,lispval arg3,
                       lispval arg4,lispval arg5,lispval arg6,
                       lispval arg7,lispval arg8,lispval arg9,
                       lispval arg10,lispval arg11,lispval arg12)
{
  if (PRED_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall12(f,arg1,arg2,arg3,
                                  arg4,arg5,arg6,
                                  arg7,arg8,arg9,
                                  arg10,arg11,arg12);
  else return f->fcn_handler.call12(arg1,arg2,arg3,
                                    arg4,arg5,arg6,
                                    arg7,arg8,arg9,
                                    arg10,arg11,arg12);
}

static lispval dcall13(struct FD_FUNCTION *f,
                       lispval arg1,lispval arg2,lispval arg3,
                       lispval arg4,lispval arg5,lispval arg6,
                       lispval arg7,lispval arg8,lispval arg9,
                       lispval arg10,lispval arg11,lispval arg12,
                       lispval arg13)
{
  if (PRED_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall13(f,arg1,arg2,arg3,
                                  arg4,arg5,arg6,
                                  arg7,arg8,arg9,
                                  arg10,arg11,arg12,
                                  arg13);
  else return f->fcn_handler.call13(arg1,arg2,arg3,
                                    arg4,arg5,arg6,
                                    arg7,arg8,arg9,
                                    arg10,arg11,arg12,
                                    arg13);
}

static lispval dcall14(struct FD_FUNCTION *f,
                       lispval arg1,lispval arg2,lispval arg3,
                       lispval arg4,lispval arg5,lispval arg6,
                       lispval arg7,lispval arg8,lispval arg9,
                       lispval arg10,lispval arg11,lispval arg12,
                       lispval arg13,lispval arg14)
{
  if (PRED_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall14(f,arg1,arg2,arg3,
                                  arg4,arg5,arg6,
                                  arg7,arg8,arg9,
                                  arg10,arg11,arg12,
                                  arg13,arg14);
  else return f->fcn_handler.call14(arg1,arg2,arg3,
                                    arg4,arg5,arg6,
                                    arg7,arg8,arg9,
                                    arg10,arg11,arg12,
                                    arg13,arg14);
}

static lispval dcall15(struct FD_FUNCTION *f,
                       lispval arg1,lispval arg2,lispval arg3,
                       lispval arg4,lispval arg5,lispval arg6,
                       lispval arg7,lispval arg8,lispval arg9,
                       lispval arg10,lispval arg11,lispval arg12,
                       lispval arg13,lispval arg14,lispval arg15)
{
  if (PRED_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall15(f,arg1,arg2,arg3,
                                  arg4,arg5,arg6,
                                  arg7,arg8,arg9,
                                  arg10,arg11,arg12,
                                  arg13,arg14,arg15);
  else return f->fcn_handler.call15(arg1,arg2,arg3,
                                    arg4,arg5,arg6,
                                    arg7,arg8,arg9,
                                    arg10,arg11,arg12,
                                    arg13,arg14,arg15);
}

/* Generic calling function */

#define BAD_PTRP(x) (!(FD_EXPECT_TRUE(FD_CHECK_CONS_PTR(x))))

static int bad_arg(u8_context cxt,struct FD_FUNCTION *f,int i,lispval v)
{
  u8_byte buf[128];
  u8_string details =
    ((f->fcn_name) ?
     (u8_sprintf(buf,128,"%s[%d]",f->fcn_name,i)) :
     (u8_sprintf(buf,128,"#!%llx[%d]",(long long)f,i)));
  if (cxt)
    fd_seterr(fd_TypeError,cxt,details,v);
  else {
    u8_byte addr_buf[64];
    u8_string addr = u8_sprintf(addr_buf,64,"#!%llx",v);
    fd_seterr(fd_TypeError,cxt,details,fdstring(addr));}
  return -1;
}

FD_FASTOP int check_args(struct FD_FUNCTION *f,int n,
                         lispval *argbuf,lispval *argvec)
{
  /* Check typeinfo */
  int *typeinfo = f->fcn_typeinfo; int i = 0;
  if (typeinfo) while (i<n) {
      lispval arg = argvec[i];
      if (BAD_PTRP(arg))
        return bad_arg(NULL,f,i,arg);
      else if (FD_QCHOICEP(arg))
        arg = FD_XQCHOICE(arg)->qchoiceval;
      else NO_ELSE;
      int expect_type = typeinfo[i], arg_type = FD_PTR_TYPE(arg);
      if ( (VOIDP(arg)) || (FD_DEFAULTP(arg))) {}
      else if (FD_EXPECT_FALSE( (expect_type >= 0) &&
                                ( arg_type != expect_type )))
        return bad_arg(fd_type2name(arg_type),f,i,arg);
      else if (FD_QCHOICEP(arg))
        arg = FD_XQCHOICE(arg)->qchoiceval;
      else NO_ELSE;
      argbuf[i++]=arg;}
  else while (i < n) {
      lispval arg = argvec[i];
      if (BAD_PTRP(arg))
        return bad_arg(NULL,f,i,arg);
      else if (FD_QCHOICEP(arg))
        arg = FD_XQCHOICE(arg)->qchoiceval;
      else NO_ELSE;
      argbuf[i++]=arg;}
  return i;
}

FD_FASTOP int fill_arbguf(struct FD_FUNCTION *f,int n,
                          lispval *argbuf,
                          lispval *argvec)
{
  int arity = f->fcn_arity, min_arity = f->fcn_min_arity;
  lispval fptr = (lispval)f;
  if ((min_arity>0) && (n<min_arity)) {
    fd_seterr(fd_TooFewArgs,"fd_dapply",f->fcn_name,fptr);
    return -1;}
  else if ((arity>=0) && (n>arity)) {
    fd_seterr(fd_TooManyArgs,"fd_dapply",f->fcn_name,fptr);
    return -1;}
  else if ((arity<0)||(arity == n))
    return check_args(f,n,argbuf,argvec);
  else {
    int *typeinfo = f->fcn_typeinfo;
    lispval *defaults = f->fcn_defaults;
    int i=0; while (i<n) {
      lispval arg = argvec[i]; int incref = 0;
      int expect_type = (typeinfo) ? (typeinfo[i]) : (-1);
      if ( (defaults) &&
           ( (arg == VOID) ||
             (arg == FD_DEFAULT_VALUE) ) ) {
        arg=defaults[i]; incref=1;}
      if (BAD_PTRP(arg))
        return bad_arg(NULL,f,i,arg);
      int arg_type = FD_PTR_TYPE(arg);
      if (FD_EXPECT_FALSE( (expect_type >= 0) &&
                           ( arg_type != expect_type )))
        return bad_arg(fd_type2name(expect_type),f,i,arg);
      else if (arg_type == fd_qchoice_type)
        arg = FD_XQCHOICE(arg)->qchoiceval;
      else NO_ELSE;
      if (incref) fd_incref(arg);
      argbuf[i++] = arg;}
    if (defaults)
      while (i<arity) { argbuf[i]=defaults[i]; i++;}
    else while (i<arity) { argbuf[i++]=VOID; }
    return i;}
}

FD_FASTOP lispval dcall(u8_string fname,fd_function f,int n,lispval *args)
{
  if (FD_INTERRUPTED()) return FD_ERROR;
  else if (f->fcn_handler.fnptr)
    switch (f->fcn_arity) {
    case 0: return dcall0(f);
    case 1: return dcall1(f,args[0]);
    case 2: return dcall2(f,args[0],args[1]);
    case 3: return dcall3(f,args[0],args[1],args[2]);
    case 4: return dcall4(f,args[0],args[1],args[2],args[3]);
    case 5: return dcall5(f,args[0],args[1],args[2],args[3],args[4]);
    case 6:
      return dcall6(f,args[0],args[1],args[2],args[3],args[4],args[5]);
    case 7:
      return dcall7(f,args[0],args[1],args[2],args[3],
                    args[4],args[5],args[6]);
    case 8:
      return dcall8(f,args[0],args[1],args[2],args[3],
                    args[4],args[5],args[6],args[7]);
    case 9:
      return dcall9(f,args[0],args[1],args[2],args[3],
                    args[4],args[5],args[6],args[7],args[8]);
    case 10:
      return dcall10(f,args[0],args[1],args[2],args[3],
                     args[4],args[5],args[6],args[7],args[8],
                     args[9]);
    case 11:
      return dcall11(f,args[0],args[1],args[2],
                     args[3],args[4],args[5],
                     args[6],args[7],args[8],
                     args[9],args[10]);
    case 12:
      return dcall12(f,args[0],args[1],args[2],
                     args[3],args[4],args[5],
                     args[6],args[7],args[8],
                     args[9],args[10],args[11]);
    case 13:
      return dcall13(f,args[0],args[1],args[2],
                     args[3],args[4],args[5],
                     args[6],args[7],args[8],
                     args[9],args[10],args[11],
                     args[12]);
    case 14:
      return dcall14(f,args[0],args[1],args[2],
                     args[3],args[4],args[5],
                     args[6],args[7],args[8],
                     args[9],args[10],args[11],
                     args[12],args[13]);
    case 15:
      return dcall15(f,args[0],args[1],args[2],
                     args[3],args[4],args[5],
                     args[6],args[7],args[8],
                     args[9],args[10],args[11],
                     args[12],args[13],args[14]);
    default:
      if (f->fcn_xcall)
        return f->fcn_handler.calln(n,args);
      else return f->fcn_handler.calln(n,args);}
  else {
    int ctype = FD_CONS_TYPE(f);
    if (fd_applyfns[ctype])
      return fd_applyfns[ctype]((lispval)f,n,args);
    else return fd_type_error("applicable","dcall",(lispval)f);}
}

FD_FASTOP lispval apply_fcn(struct FD_STACK *stack,
                            u8_string name,fd_function f,int n,
                            lispval *argvec)
{
  lispval fnptr = (lispval)f; int arity=f->fcn_arity;
  if (PRED_FALSE(n<0))
    return fd_err(_("Negative arg count"),"apply_fcn",name,fnptr);
  else if (arity<0) { /* Is a LEXPR */
    if (n<(f->fcn_min_arity))
      return fd_err(fd_TooFewArgs,"apply_fcn",f->fcn_name,fnptr);
    lispval argbuf[n];
    if (FD_EXPECT_FALSE(check_args(f,n,argbuf,argvec)<0))
      return FD_ERROR;
    else if ( (f->fcn_xcall) && (f->fcn_handler.xcalln) )
      return f->fcn_handler.xcalln(f,n,argbuf);
    else if (f->fcn_handler.calln)
      return f->fcn_handler.calln(n,argbuf);
    else {
      /* There's no explicit method on this function object, so we use
         the method on the type (if there is one) */
      int ctype = FD_CONS_TYPE(f);
      if (fd_applyfns[ctype])
        return fd_applyfns[ctype]((lispval)f,n,argbuf);
      else return fd_err("NotApplicable","apply_fcn",f->fcn_name,fnptr);}}
  else if (n==arity) {
    lispval argbuf[arity];
    if (FD_EXPECT_FALSE(check_args(f,n,argbuf,argvec)<0))
      return FD_ERROR;
    else return dcall(name,f,n,argbuf);}
  else {
    lispval argbuf[arity];
    if (FD_EXPECT_FALSE(fill_arbguf(f,n,argbuf,argvec)<0))
      return FD_ERROR;
    else return dcall(name,f,n,argbuf);}
}

FD_EXPORT lispval fd_dcall(struct FD_STACK *_stack,lispval fn,
                           int n,lispval *argvec)
{
  u8_byte namebuf[60], numbuf[32];
  u8_string fname="apply";
  fd_ptr_type ftype=FD_PRIM_TYPE(fn);
  struct FD_FUNCTION *f=NULL;

  if (ftype==fd_fcnid_type) {
    fn=fd_fcnid_ref(fn);
    ftype=FD_PTR_TYPE(fn);}

  if (fd_functionp[ftype]) {
    f=(struct FD_FUNCTION *)fn;
    if (f->fcn_name) fname=f->fcn_name;}
  else if (fd_applyfns[ftype]) {
    strcpy(namebuf,"λ0x");
    strcat(namebuf,u8_uitoa16((unsigned long long)fn,numbuf));
    fname=namebuf;}
  else return fd_type_error("applicable","fd_determinstic_apply",fn);

  /* Make the call */
  if (stackcheck()) {
    lispval result=VOID;
    struct FD_PROFILE *profile = (f) ? (f->fcn_profile) : (NULL);
    FD_APPLY_STACK(apply_stack,fname,fn);
    if (f) {
      apply_stack->stack_src      = f->fcn_filename;
      U8_CLEARBITS(apply_stack->stack_flags,FD_STACK_FREE_SRC);}
    apply_stack->stack_args=argvec;
    apply_stack->n_args=n;
    U8_WITH_CONTOUR(fname,0)
      if ( (f) && (profile) ) {
        long long nsecs = 0;
        long long stime = 0, utime = 0;
        long long n_waits = 0, n_contests = 0, n_faults = 0;
#if ( (FD_EXTENDED_PROFILING) && (HAVE_DECL_RUSAGE_THREAD) )
        struct rusage before = { 0 }, after = { 0 };
        getrusage(RUSAGE_THREAD,&before);
#endif
#if HAVE_CLOCK_GETTIME
        struct timespec start, end;
        clock_gettime(CLOCK_MONOTONIC,&start);
#endif
        result=apply_fcn(apply_stack,fname,f,n,argvec);
        if ( (FD_TAILCALLP(result)) && (f->fcn_notail) )
          result=fd_finish_call(result);
#if HAVE_CLOCK_GETTIME
        clock_gettime(CLOCK_MONOTONIC,&end);
        nsecs = ((end.tv_sec*1000000000)+(end.tv_nsec)) -
          ((start.tv_sec*1000000000)+(start.tv_nsec));
#endif
#if ( (FD_EXTENDED_PROFILING) && (HAVE_DECL_RUSAGE_THREAD) )
        getrusage(RUSAGE_THREAD,&after);
        utime = (after.ru_utime.tv_sec*1000000000+after.ru_utime.tv_usec*1000)-
          (before.ru_utime.tv_sec*1000000000+before.ru_utime.tv_usec*1000);
        stime = (after.ru_stime.tv_sec*1000000000+after.ru_stime.tv_usec*1000)-
          (before.ru_stime.tv_sec*1000000000+before.ru_stime.tv_usec*1000);
#if HAVE_STRUCT_RUSAGE_RU_NVCSW
        n_waits = after.ru_nvcsw - before.ru_nvcsw;
#endif
#if HAVE_STRUCT_RUSAGE_RU_MAJFLT
        n_faults = after.ru_majflt - before.ru_majflt;
#endif
#if HAVE_STRUCT_RUSAGE_RU_NIVCSW
        n_faults = after.ru_nivcsw - before.ru_nivcsw;
#endif
#endif

        fd_profile_record(profile,0,nsecs,utime,stime,
                          n_waits,n_contests,n_faults);}
      else if (f) {
        result=apply_fcn(apply_stack,fname,f,n,argvec);
        if (!(PRED_TRUE(FD_CHECK_PTR(result))))
          return fd_badptr_err(result,"fd_deterministic_apply",fname);
        else if ( (FD_TAILCALLP(result)) && (f->fcn_notail) )
          result=fd_finish_call(result);}
      else result=fd_applyfns[ftype](fn,n,argvec);
    U8_ON_EXCEPTION {
      U8_CLEAR_CONTOUR();
      result = FD_ERROR;}
    U8_END_EXCEPTION;
    if (!(FD_CHECK_PTR(result))) {
      if (errno) {u8_graberrno("fd_apply",fname);}
      result = fd_badptr_err(result,"fd_deterministic_apply",fname);}
    if ( (errno) && (!(FD_TROUBLEP(result)))) {
      u8_string cond=u8_strerror(errno);
      u8_log(LOG_WARN,cond,"Unexpected errno=%d (%s) after %s",
             errno,cond,U8ALT(fname,"primcall"));
      errno=0;}
    if ( (FD_TROUBLEP(result)) &&  (u8_current_exception==NULL) ) {
      if (errno) {u8_graberrno("fd_apply",fname);}
      else fd_seterr(fd_UnknownError,"fd_apply",fname,VOID);}
    fd_pop_stack(apply_stack);
    return result;}
  else {
    u8_string limit=u8_mkstring("%lld",fd_stack_limit);
    lispval depth=FD_INT2DTYPE(u8_stack_depth());
    return fd_err(fd_StackOverflow,fname,limit,depth);}
}

/* Calling non-deterministically */

#define FD_ADD_RESULT(to,result)          \
  if (to == EMPTY) to = result;                 \
  else {                                        \
    if (TYPEP(to,fd_tailcall_type))               \
      to = fd_finish_call(to);                    \
    if (TYPEP(result,fd_tailcall_type))           \
      result = fd_finish_call(result);            \
    CHOICE_ADD(to,result);}

static lispval ndcall_loop
(struct FD_STACK *_stack,
 struct FD_FUNCTION *f,lispval *results,int *typeinfo,
 int i,int n,lispval *nd_args,lispval *d_args)
{
  lispval retval=VOID;
  if (i == n) {
    lispval value = fd_dapply((lispval)f,n,d_args);
    if (FD_ABORTP(value)) {
      return value;}
    else {
      value = fd_finish_call(value);
      if (FD_ABORTP(value)) return value;
      FD_ADD_RESULT(*results,value);}}
  else if ((!(CHOICEP(nd_args[i]))) ||
           ((typeinfo)&&(typeinfo[i]==fd_choice_type))) {
    d_args[i]=nd_args[i];
    return ndcall_loop(_stack,f,results,typeinfo,i+1,n,nd_args,d_args);}
  else {
    DO_CHOICES(elt,nd_args[i]) {
      d_args[i]=elt;
      retval = ndcall_loop(_stack,f,results,typeinfo,i+1,n,nd_args,d_args);}}
  if (FD_ABORTP(retval))
    return FD_ERROR;
  else return *results;
}

static lispval ndapply1(fd_stack _stack,lispval fp,lispval args1)
{
  lispval results = EMPTY;
  DO_CHOICES(arg1,args1) {
    lispval r = fd_dapply(fp,1,&arg1);
    if (FD_ABORTP(r)) {
      FD_STOP_DO_CHOICES;
      fd_decref(results);
      return r;}
    else {FD_ADD_RESULT(results,r);}}
  fd_pop_stack(_stack);
  return results;
}

static lispval ndapply2(fd_stack _stack,lispval fp,lispval args0,lispval args1)
{
  lispval results = EMPTY;
  DO_CHOICES(arg0,args0) {
    DO_CHOICES(arg1,args1) {
      lispval argv[2]={arg0,arg1};
      lispval r = fd_dapply(fp,2,argv);
      if (FD_ABORTP(r)) {
        fd_decref(results);
        results = r;
        FD_STOP_DO_CHOICES;
        break;}
      else {FD_ADD_RESULT(results,r);}}
    if (FD_ABORTP(results)) {
      FD_STOP_DO_CHOICES;
      break;}}
  fd_pop_stack(_stack);
  return results;
}

static lispval ndapply3(fd_stack _stack,lispval fp,lispval args0,lispval args1,lispval args2)
{
  lispval results = EMPTY;
  DO_CHOICES(arg0,args0) {
    DO_CHOICES(arg1,args1) {
      DO_CHOICES(arg2,args2) {
        lispval argv[3]={arg0,arg1,arg2};
        lispval r = fd_dapply(fp,3,argv);
        if (FD_ABORTP(r)) {
          fd_decref(results);
          results = r;
          FD_STOP_DO_CHOICES;
          break;}
        else {FD_ADD_RESULT(results,r);}}
      if (FD_ABORTP(results)) {
        FD_STOP_DO_CHOICES;
        break;}}
    if (FD_ABORTP(results)) {
      FD_STOP_DO_CHOICES;
      break;}}
  fd_pop_stack(_stack);
  return results;
}

static lispval ndapply4(fd_stack _stack,
                        lispval fp,
                        lispval args0,lispval args1,
                        lispval args2,lispval args3)
{
  lispval results = EMPTY;
  DO_CHOICES(arg0,args0) {
    DO_CHOICES(arg1,args1) {
      DO_CHOICES(arg2,args2) {
        DO_CHOICES(arg3,args3) {
          lispval argv[4]={arg0,arg1,arg2,arg3};
          lispval r = fd_dapply(fp,4,argv);
          if (FD_ABORTP(r)) {
            fd_decref(results);
            results = r;
            FD_STOP_DO_CHOICES;
            break;}
          else {FD_ADD_RESULT(results,r);}}
        if (FD_ABORTP(results)) {
          FD_STOP_DO_CHOICES;
          break;}}
      if (FD_ABORTP(results)) {
        FD_STOP_DO_CHOICES;
        break;}}
    if (FD_ABORTP(results)) {
      FD_STOP_DO_CHOICES;
      break;}}
  fd_pop_stack(_stack);
  return results;
}

FD_EXPORT lispval fd_ndcall(struct FD_STACK *_stack,
                            lispval fp,int n,lispval *args)
{
  lispval handler = (FD_FCNIDP(fp) ? (fd_fcnid_ref(fp)) : (fp));
  if (EMPTYP(handler))
    return EMPTY;
  else if (CHOICEP(handler)) {
    FD_APPLY_STACK(ndapply_stack,"fnchoices",handler);
    lispval results=EMPTY;
    DO_CHOICES(h, handler) {
      lispval r=fd_call(ndapply_stack,h,n,args);
      if (FD_ABORTP(r)) {
        fd_decref(results);
        FD_STOP_DO_CHOICES;
        fd_pop_stack(ndapply_stack);
        return r;}
      else {CHOICE_ADD(results,r);}}
    fd_pop_stack(ndapply_stack);
    return results;}
  else {
    fd_ptr_type fntype = FD_PTR_TYPE(handler);
    if (fd_functionp[fntype]) {
      struct FD_FUNCTION *f = FD_DTYPE2FCN(handler);
      if (f->fcn_arity == 0)
        return fd_dapply(handler,n,args);
      else if ((f->fcn_arity < 0) ?
               (n >= (f->fcn_min_arity)) :
               ((n <= (f->fcn_arity)) &&
                (n >= (f->fcn_min_arity)))) {
        lispval d_args[n];
        lispval retval, results = EMPTY;
        FD_PUSH_STACK(ndstack,fd_ndcallstack_type,f->fcn_name,handler);
        ndstack->stack_src = f->fcn_filename;
        U8_CLEARBITS(ndstack->stack_flags,FD_STACK_FREE_SRC);
        /* Initialize the d_args vector */
        if (n==1)
          return ndapply1(ndstack,handler,args[0]);
        else if (n==2)
          return ndapply2(ndstack,handler,args[0],args[1]);
        else if (n==3)
          return ndapply3(ndstack,handler,
                          args[0],args[1],args[2]);
        else if (n==4)
          return ndapply4(ndstack,handler,
                          args[0],args[1],
                          args[2],args[3]);
        else retval = ndcall_loop
               (ndstack,f,&results,f->fcn_typeinfo,
                0,n,args,d_args);
        fd_pop_stack(ndstack);
        if (FD_ABORTP(retval)) {
          fd_decref(results);
          return retval;}
        else return fd_simplify_choice(results);}
      else {
        u8_condition ex = (n>f->fcn_arity) ? (fd_TooManyArgs) :
          (fd_TooFewArgs);
        return fd_err(ex,"fd_ndapply",f->fcn_name,LISP_CONS(f));}}
    else if (fd_applyfns[fntype])
      return (fd_applyfns[fntype])(handler,n,args);
    else return fd_type_error("Applicable","fd_ndapply",handler);
  }
}

/* The default apply function */

static int contains_qchoicep(int n,lispval *args);
static lispval qchoice_dcall
(fd_stack stack,lispval fp,int n,lispval *args);

FD_EXPORT lispval fd_call(struct FD_STACK *_stack,
                          lispval fp,int n,lispval *args)
{
  lispval result;
  lispval handler = (FD_FCNIDP(fp)) ? (fd_fcnid_ref(fp)) : (fp);
  if (FD_FUNCTIONP(handler))  {
    struct FD_FUNCTION *f = (fd_function) handler;
    if ( (f) && (f->fcn_ndcall) ) {
      if (!(PRED_FALSE(contains_qchoicep(n,args))))
        result = fd_dcall(_stack,(lispval)f,n,args);
      else result = qchoice_dcall(_stack,fp,n,args);
      return fd_finish_call(result);}}
  if (fd_applyfns[FD_PRIM_TYPE(handler)]) {
    int i = 0, qchoice = 0;
    while (i<n)
      if (args[i]==EMPTY)
        return EMPTY;
      else if (ATOMICP(args[i])) i++;
      else {
        fd_ptr_type argtype = FD_PTR_TYPE(args[i]);
        if ((argtype == fd_choice_type) ||
            (argtype == fd_prechoice_type)) {
          result = fd_ndcall(_stack,handler,n,args);
          return fd_finish_call(result);}
        else if (argtype == fd_qchoice_type) {
          qchoice = 1; i++;}
        else i++;}
    if (qchoice)
      result=qchoice_dcall(_stack,handler,n,args);
    else result=fd_dcall(_stack,handler,n,args);
    return fd_finish_call(result);}
  else return fd_err("Not applicable","fd_call",NULL,fp);
}

static int contains_qchoicep(int n,lispval *args)
{
  lispval *scan = args, *limit = args+n;
  while (scan<limit)
    if (QCHOICEP(*scan)) return 1;
    else scan++;
  return 0;
}

static lispval qchoice_dcall(struct FD_STACK *stck,lispval fp,int n,lispval *args)
{
  lispval argbuf[n]; /* *argbuf=fd_alloca(n); */
  lispval *read = args, *limit = read+n, *write=argbuf;
  while (read<limit)
    if (QCHOICEP(*read)) {
      struct FD_QCHOICE *qc = (struct FD_QCHOICE *) (*read++);
      *write++=qc->qchoiceval;}
    else *write++= *read++;
  return fd_dcall(stck,fp,n,argbuf);
}

/* Apply wrappers (which finish calls) */

FD_EXPORT lispval _fd_stack_apply
(struct FD_STACK *stack,lispval fn,int n_args,lispval *args)
{
  return fd_stack_apply(stack,fn,n_args,args);
}
FD_EXPORT lispval _fd_stack_dapply
(struct FD_STACK *stack,lispval fn,int n_args,lispval *args)
{
  return fd_dapply(fn,n_args,args);
}
FD_EXPORT lispval _fd_stack_ndapply
(struct FD_STACK *stack,lispval fn,int n_args,lispval *args)
{
  return fd_ndapply(fn,n_args,args);
}

/* Tail calls */

FD_EXPORT lispval fd_tail_call(lispval fcn,int n,lispval *vec)
{
  if (FD_FCNIDP(fcn)) fcn = fd_fcnid_ref(fcn);
  struct FD_FUNCTION *f = (struct FD_FUNCTION *)fcn;
  if (PRED_FALSE(((f->fcn_arity)>=0) && (n>(f->fcn_arity)))) {
    u8_byte buf[64];
    fd_seterr(fd_TooManyArgs,"fd_tail_call",
              u8_sprintf(buf,64,"%d",n),
              fcn);
    return FD_ERROR;}
  else {
    int atomic = 1, nd = 0; lispval fcnid = f->fcnid;
    struct FD_TAILCALL *tc = (struct FD_TAILCALL *)
      u8_malloc(sizeof(struct FD_TAILCALL)+LISPVEC_BYTELEN(n));
    lispval *write = &(tc->tailcall_head);
    lispval *write_limit = write+(n+1);
    lispval *read = vec;
    FD_INIT_FRESH_CONS(tc,fd_tailcall_type);
    tc->tailcall_arity = n+1;
    tc->tailcall_flags = 0;
    if (fcnid == FD_NULL) {fcnid = f->fcnid = VOID;}
    if (FD_FCNIDP(fcnid))
      *write++=fcnid;
    else *write++=fd_incref(fcn);
    while (write<write_limit) {
      lispval v = *read++;
      if (CONSP(v)) {
        if (CHOICEP(v)) nd = 1;
        atomic = 0;
        *write++=fd_incref(v);}
      else *write++=v;}
    if (atomic) tc->tailcall_flags |= FD_TAILCALL_ATOMIC_ARGS;
    if (nd) tc->tailcall_flags |= FD_TAILCALL_ND_ARGS;
    return LISP_CONS(tc);}
}
FD_EXPORT lispval fd_void_tail_call(lispval fcn,int n,lispval *vec)
{
  if (FD_FCNIDP(fcn)) fcn = fd_fcnid_ref(fcn);
  struct FD_FUNCTION *f = (struct FD_FUNCTION *)fcn;
  if (PRED_FALSE(((f->fcn_arity)>=0) && (n>(f->fcn_arity)))) {
    u8_byte buf[64];
    fd_seterr(fd_TooManyArgs,"fd_void_tail_call",
              u8_sprintf(buf,64,"%d",n),
              fcn);
    return FD_ERROR;}
  else {
    int atomic = 1, nd = 0;
    struct FD_TAILCALL *tc = (struct FD_TAILCALL *)
      u8_malloc(sizeof(struct FD_TAILCALL)+LISPVEC_BYTELEN(n));
    lispval *write = &(tc->tailcall_head);
    lispval *write_limit = write+(n+1);
    lispval *read = vec;
    FD_INIT_FRESH_CONS(tc,fd_tailcall_type);
    tc->tailcall_arity = n+1;
    tc->tailcall_flags = FD_TAILCALL_VOID_VALUE;
    *write++=fd_incref(fcn);
    while (write<write_limit) {
      lispval v = *read++;
      if (CONSP(v)) {
        atomic = 0;
        if (QCHOICEP(v)) {
          struct FD_QCHOICE *qc = (fd_qchoice)v;
          lispval cv = qc->qchoiceval;
          fd_incref(cv);
          *write++=cv;}
        else {
          if (CHOICEP(v)) nd = 1;
          *write++=fd_incref(v);}}
      else *write++=v;}
    if (atomic) tc->tailcall_flags |= FD_TAILCALL_ATOMIC_ARGS;
    if (nd) tc->tailcall_flags |= FD_TAILCALL_ND_ARGS;
    return LISP_CONS(tc);}
}

FD_EXPORT lispval fd_step_call(lispval c)
{
  struct FD_TAILCALL *tc=
    fd_consptr(struct FD_TAILCALL *,c,fd_tailcall_type);
  int discard = U8_BITP(tc->tailcall_flags,FD_TAILCALL_VOID_VALUE);
  int n=tc->tailcall_arity;
  lispval head=tc->tailcall_head;
  lispval *arg0=(&(tc->tailcall_head))+1;
  lispval result=
    ((tc->tailcall_flags&FD_TAILCALL_ND_ARGS)?
     (fd_apply(head,n-1,arg0)):
     (fd_dapply(head,n-1,arg0)));
  fd_decref(c);
  if (discard) {
    if (FD_TAILCALLP(result))
      return result;
    fd_decref(result);
    return VOID;}
  else return result;
}

FD_EXPORT lispval _fd_finish_call(lispval call)
{
  if (FD_TAILCALLP(call)) {
    lispval result = VOID;
    while (1) {
      struct FD_TAILCALL *tc=
        fd_consptr(struct FD_TAILCALL *,call,fd_tailcall_type);
      int n=tc->tailcall_arity;
      int flags = tc->tailcall_flags;
      lispval head=tc->tailcall_head;
      lispval *args=(&(tc->tailcall_head))+1;
      int voidval = (U8_BITP(flags,FD_TAILCALL_VOID_VALUE));
      lispval next = (U8_BITP(flags,FD_TAILCALL_ND_ARGS)) ?
        (fd_apply(head,n-1,args)) :
        (fd_dapply(head,n-1,args));
      int finished = (!(TYPEP(next,fd_tailcall_type)));
      fd_decref(call);
      call = next;
      if (finished) {
        if (FD_ABORTP(next))
          return next;
        else if (voidval) {
          fd_decref(next);
          result = VOID;}
        else result = next;
        break;}
      else if (FD_ABORTP(next))
        return next;}
    return result;}
  else return call;
}
static int unparse_tail_call(struct U8_OUTPUT *out,lispval x)
{
  struct FD_TAILCALL *tc=
    fd_consptr(struct FD_TAILCALL *,x,fd_tailcall_type);
  u8_printf(out,"#<TAILCALL %q on %d args>",
            tc->tailcall_head,tc->tailcall_arity);
  return 1;
}

static void recycle_tail_call(struct FD_RAW_CONS *c)
{
  struct FD_TAILCALL *tc = (struct FD_TAILCALL *)c;
  int mallocd = FD_MALLOCD_CONSP(c), n_elts = tc->tailcall_arity;
  lispval *scan = &(tc->tailcall_head), *limit = scan+n_elts;
  size_t tc_size = sizeof(struct FD_TAILCALL)+
    (LISPVEC_BYTELEN((n_elts-1)));
  if (!(tc->tailcall_flags&FD_TAILCALL_ATOMIC_ARGS)) {
    while (scan<limit) {fd_decref(*scan); scan++;}}
  /* The head is always incref'd */
  else fd_decref(*scan);
  memset(tc,0,tc_size);
  if (mallocd) u8_free(tc);
}

/* Initializations */

static u8_condition DefnFailed=_("Definition Failed");

FD_EXPORT void fd_defn(lispval table,lispval fcn)
{
  struct FD_FUNCTION *f = fd_consptr(struct FD_FUNCTION *,fcn,fd_cprim_type);
  if (fd_store(table,fd_intern(f->fcn_name),fcn)<0)
    u8_raise(DefnFailed,"fd_defn",NULL);
  if ( (FD_NULLP(f->fcn_moduleid)) || (FD_VOIDP(f->fcn_moduleid)) ) {
    lispval moduleid = fd_get(table,moduleid_symbol,FD_VOID);
    if (!(FD_VOIDP(moduleid))) f->fcn_moduleid=moduleid;}
}

FD_EXPORT void fd_idefn(lispval table,lispval fcn)
{
  struct FD_FUNCTION *f = fd_consptr(struct FD_FUNCTION *,fcn,fd_cprim_type);
  if (fd_store(table,fd_intern(f->fcn_name),fcn)<0)
    u8_raise(DefnFailed,"fd_defn",NULL);
  if ( (FD_NULLP(f->fcn_moduleid)) || (FD_VOIDP(f->fcn_moduleid)) ) {
    lispval moduleid = fd_get(table,moduleid_symbol,FD_VOID);
    if (!(FD_VOIDP(moduleid))) f->fcn_moduleid=moduleid;}
  fd_decref(fcn);
}

FD_EXPORT void fd_defalias(lispval table,u8_string to,u8_string from)
{
  lispval to_symbol = fd_intern(to);
  lispval from_symbol = fd_intern(from);
  lispval v = fd_get(table,from_symbol,VOID);
  fd_store(table,to_symbol,v);
  fd_decref(v);
}

FD_EXPORT void fd_defalias2(lispval table,u8_string to,lispval src,u8_string from)
{
  lispval to_symbol = fd_intern(to);
  lispval from_symbol = fd_intern(from);
  lispval v = fd_get(src,from_symbol,VOID);
  fd_store(table,to_symbol,v);
  fd_decref(v);
}

static lispval dapply(lispval f,int n_args,lispval *argvec)
{
  return fd_dapply(f,n_args,argvec);
}

void fd_init_cprims_c(void);
void fd_init_stacks_c(void);
void fd_init_lexenv_c(void);
void fd_init_ffi_c(void);

/* PROFILED config */

static lispval profiled_fcns = FD_EMPTY;
static u8_mutex profiled_lock;

static int config_add_profiled(lispval var,lispval val,void *data)
{
  if (FD_FUNCTIONP(val)) {
    struct FD_FUNCTION *fcn = FD_XFUNCTION(val);
    if (fcn->fcn_profile)
      return 0;
    u8_lock_mutex(&profiled_lock);
    struct FD_PROFILE *profile = fd_make_profile(fcn->fcn_name);
    lispval *ptr = (lispval *) data;
    lispval cur = *ptr;
    fd_incref(val);
    FD_ADD_TO_CHOICE(cur,val);
    fcn->fcn_profile=profile;
    *ptr = cur;
    return 1;}
  else {
    fd_seterr("Not a function","config_add_profiled",NULL,val);
    return -1;}
}

/* Initializations */

FD_EXPORT void fd_init_apply_c()
{
  int i = 0; while (i < FD_TYPE_MAX) fd_applyfns[i++]=NULL;
  i = 0; while (i < FD_TYPE_MAX) fd_functionp[i++]=0;

  moduleid_symbol = fd_intern("%MODULEID");

  fd_functionp[fd_fcnid_type]=1;

  fd_applyfns[fd_cprim_type]=dapply;

  u8_register_source_file(_FILEINFO);
  u8_register_source_file(FRAMERD_APPLY_H_INFO);

#if (FD_USE_TLS)
  u8_new_threadkey(&fd_stack_limit_key,NULL);
#endif

  fd_unparsers[fd_tailcall_type]=unparse_tail_call;
  fd_recyclers[fd_tailcall_type]=recycle_tail_call;
  fd_type_names[fd_tailcall_type]="tailcall";

  u8_register_threadinit(init_thread_stack_limit);
  u8_init_mutex(&profiled_lock);


  fd_register_config
    ("STACKLIMIT",
     _("Size of the stack (in bytes or as a factor of the current size)"),
     fd_sizeconfig_get,fd_sizeconfig_set,&fd_default_stackspec);

  fd_register_config
    ("PROFILING",_("Whether to profile function applications by default"),
     fd_boolconfig_get,fd_boolconfig_set,&fd_profiling);

  fd_register_config
    ("PROFILED",_("Functions declared for profiling"),
     fd_lconfig_get,config_add_profiled,&profiled_fcns);

  u8_register_threadinit(init_thread_stack_limit);

  fd_init_cprims_c();
  fd_init_ffi_c();
  fd_init_stacks_c();
  fd_init_lexenv_c();
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
