/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
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
#include "framerd/apply.h"

#include <libu8/u8printf.h>
#include <libu8/u8contour.h>
#include <libu8/u8strings.h>

#include <errno.h>

#if HAVE_UNISTD_H
#include <unistd.h>
#endif

#include <stdarg.h>

fdtype fd_default_stackspec = FD_VOID;
u8_string fd_ndcallstack_type = "ndapply stack";
u8_string fd_callstack_type   = "apply stack";

#define FD_APPLY_STACK(name,fname,fn) \
  FD_PUSH_STACK(name,fd_callstack_type,fname,fn)

fd_applyfn fd_applyfns[FD_TYPE_MAX];
/* This is set if the type is a CONS with a FUNCTION header */
short fd_functionp[FD_TYPE_MAX];

u8_condition fd_apply_context="APPLY";

fd_exception fd_NotAFunction=_("calling a non function");
fd_exception fd_TooManyArgs=_("too many arguments");
fd_exception fd_TooFewArgs=_("too few arguments");
fd_exception fd_NoDefault=_("no default value for #default argument");
fd_exception fd_ProfilingDisabled=_("profiling not built");

fd_exception fd_NoStackChecking=
  _("Stack checking is not available in this build of FramerD");
fd_exception fd_StackTooSmall=
  _("This value is too small for an application stack limit");
fd_exception fd_StackTooLarge=
  _("This value is larger than the available stack space");
fd_exception fd_BadStackSize=
  _("This is an invalid stack size");
fd_exception fd_BadStackFactor=
  _("This is an invalid stack resize factor");
fd_exception fd_InternalStackSizeError=
  _("Internal stack size didn't make any sense, punting");

const int fd_calltrack_enabled = FD_CALLTRACK_ENABLED;

#if ((FD_THREADS_ENABLED)&&(FD_USE_TLS))
u8_tld_key fd_stack_limit_key;
#elif ((FD_THREADS_ENABLED)&&(HAVE_THREAD_STORAGE_CLASS))
__thread ssize_t fd_stack_limit = -1;
#else
ssize_t fd_stack_limit = -1;
#endif

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
#if ( (HAVE_PTHREAD_SELF) && \
      (HAVE_PTHREAD_GETATTR_NP) && \
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

FD_EXPORT ssize_t fd_init_stack()
{
  u8_init_stack();
  if (FD_VOIDP(fd_default_stackspec)) {
    ssize_t stacksize = u8_stack_size;
    return fd_stack_setsize(stacksize-stacksize/8);}
  else if (FD_FIXNUMP(fd_default_stackspec))
    return fd_stack_setsize((ssize_t)(FD_FIX2INT(fd_default_stackspec)));
  else if (FD_FLONUMP(fd_default_stackspec))
    return fd_stack_resize(FD_FLONUM(fd_default_stackspec));
  else if (FD_BIGINTP(fd_default_stackspec)) {
    unsigned long long val = fd_getint(fd_default_stackspec);
    if (val) return fd_stack_setsize((ssize_t) val);
    else {
      u8_log(LOGCRIT,fd_BadStackSize,
             "The default stack value %q wasn't a valid stack size",
             fd_default_stackspec);
      return -1;}}
  else {
    u8_log(LOGCRIT,fd_BadStackSize,
           "The default stack value %q wasn't a valid stack size",
           fd_default_stackspec);
    return -1;}
}

static int init_thread_stack_limit()
{
  fd_init_stack();
  return 1;
}

/* Call stack (not used yet) */

#if (U8_USE_TLS)
u8_tld_key fd_stackptr_key;
#elif (U8_USE__THREAD)
__thread struct FD_STACK *fd_stackptr = NULL;
#else
struct FD_STACK *fd_stackptr = NULL;
#endif

FD_EXPORT void _fd_free_stack(struct FD_STACK *stack)
{
  fd_pop_stack(stack);
}

FD_EXPORT void _fd_pop_stack(struct FD_STACK *stack)
{
  fd_pop_stack(stack);
}

FD_EXPORT struct FD_STACK_CLEANUP *_fd_push_cleanup
(struct FD_STACK *stack,fd_stack_cleanop op)
{
  return fd_push_cleanup(stack,op);
}

/* Calling primitives */

static fdtype dcall0(struct FD_FUNCTION *f)
{
  if (FD_EXPECT_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall0(f);
  else return f->fcn_handler.call0();
}
static fdtype dcall1(struct FD_FUNCTION *f,fdtype arg1)
{
  if (FD_EXPECT_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall1(f,arg1);
  else return f->fcn_handler.call1(arg1);
}
static fdtype dcall2(struct FD_FUNCTION *f,fdtype arg1,fdtype arg2)
{
  if (FD_EXPECT_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall2(f,arg1,arg2);
  else return f->fcn_handler.call2(arg1,arg2);
}
static fdtype dcall3(struct FD_FUNCTION *f,
                      fdtype arg1,fdtype arg2,fdtype arg3)
{
  if (FD_EXPECT_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall3(f,arg1,arg2,arg3);
  else return f->fcn_handler.call3(arg1,arg2,arg3);
}
static fdtype dcall4(struct FD_FUNCTION *f,
                      fdtype arg1,fdtype arg2,fdtype arg3,fdtype arg4)
{
  if (FD_EXPECT_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall4(f,arg1,arg2,arg3,arg4);
  else return f->fcn_handler.call4(arg1,arg2,arg3,arg4);
}
static fdtype dcall5(struct FD_FUNCTION *f,
                      fdtype arg1,fdtype arg2,fdtype arg3,fdtype arg4,
                      fdtype arg5)
{
  if (FD_EXPECT_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall5(f,arg1,arg2,arg3,arg4,arg5);
  else return f->fcn_handler.call5(arg1,arg2,arg3,arg4,arg5);
}
static fdtype dcall6(struct FD_FUNCTION *f,
                      fdtype arg1,fdtype arg2,fdtype arg3,fdtype arg4,
                      fdtype arg5,fdtype arg6)
{
  if (FD_EXPECT_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall6(f,arg1,arg2,arg3,arg4,arg5,arg6);
  else return f->fcn_handler.call6(arg1,arg2,arg3,arg4,arg5,arg6);
}

static fdtype dcall7(struct FD_FUNCTION *f,
                      fdtype arg1,fdtype arg2,fdtype arg3,fdtype arg4,
                      fdtype arg5,fdtype arg6,fdtype arg7)
{
  if (FD_EXPECT_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall7(f,arg1,arg2,arg3,arg4,arg5,arg6,arg7);
  else return f->fcn_handler.call7(arg1,arg2,arg3,arg4,arg5,arg6,arg7);
}

static fdtype dcall8(struct FD_FUNCTION *f,
                     fdtype arg1,fdtype arg2,fdtype arg3,fdtype arg4,
                     fdtype arg5,fdtype arg6,fdtype arg7,fdtype arg8)
{
  if (FD_EXPECT_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall8(f,arg1,arg2,arg3,arg4,arg5,arg6,arg7,arg8);
  else return f->fcn_handler.call8(arg1,arg2,arg3,arg4,arg5,arg6,arg7,arg8);
}

static fdtype dcall9(struct FD_FUNCTION *f,
                     fdtype arg1,fdtype arg2,fdtype arg3,fdtype arg4,
                     fdtype arg5,fdtype arg6,fdtype arg7,fdtype arg8,
                     fdtype arg9)
{
  if (FD_EXPECT_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall9(f,arg1,arg2,arg3,
                                 arg4,arg5,arg6,
                                 arg7,arg8,arg9);
  else return f->fcn_handler.call9(arg1,arg2,arg3,
                                   arg4,arg5,arg6,
                                   arg7,arg8,arg9);
}

static fdtype dcall10(struct FD_FUNCTION *f,
                      fdtype arg1,fdtype arg2,fdtype arg3,
                      fdtype arg4,fdtype arg5,fdtype arg6,
                      fdtype arg7,fdtype arg8,fdtype arg9,
                      fdtype arg10)
{
  if (FD_EXPECT_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall10(f,arg1,arg2,arg3,
                                  arg4,arg5,arg6,
                                  arg7,arg8,arg9,
                                  arg10);
  else return f->fcn_handler.call10(arg1,arg2,arg3,
                                    arg4,arg5,arg6,
                                    arg7,arg8,arg9,
                                    arg10);
}

static fdtype dcall11(struct FD_FUNCTION *f,
                      fdtype arg1,fdtype arg2,fdtype arg3,
                      fdtype arg4,fdtype arg5,fdtype arg6,
                      fdtype arg7,fdtype arg8,fdtype arg9,
                      fdtype arg10,fdtype arg11)
{
  if (FD_EXPECT_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall11(f,arg1,arg2,arg3,
                                  arg4,arg5,arg6,
                                  arg7,arg8,arg9,
                                  arg10,arg11);
  else return f->fcn_handler.call11(arg1,arg2,arg3,
                                    arg4,arg5,arg6,
                                    arg7,arg8,arg9,
                                    arg10,arg11);
}

static fdtype dcall12(struct FD_FUNCTION *f,
                      fdtype arg1,fdtype arg2,fdtype arg3,
                      fdtype arg4,fdtype arg5,fdtype arg6,
                      fdtype arg7,fdtype arg8,fdtype arg9,
                      fdtype arg10,fdtype arg11,fdtype arg12)
{
  if (FD_EXPECT_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall12(f,arg1,arg2,arg3,
                                  arg4,arg5,arg6,
                                  arg7,arg8,arg9,
                                  arg10,arg11,arg12);
  else return f->fcn_handler.call12(arg1,arg2,arg3,
                                    arg4,arg5,arg6,
                                    arg7,arg8,arg9,
                                    arg10,arg11,arg12);
}

static fdtype dcall13(struct FD_FUNCTION *f,
                      fdtype arg1,fdtype arg2,fdtype arg3,
                      fdtype arg4,fdtype arg5,fdtype arg6,
                      fdtype arg7,fdtype arg8,fdtype arg9,
                      fdtype arg10,fdtype arg11,fdtype arg12,
                      fdtype arg13)
{
  if (FD_EXPECT_FALSE(f->fcn_xcall))
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

static fdtype dcall14(struct FD_FUNCTION *f,
                      fdtype arg1,fdtype arg2,fdtype arg3,
                      fdtype arg4,fdtype arg5,fdtype arg6,
                      fdtype arg7,fdtype arg8,fdtype arg9,
                      fdtype arg10,fdtype arg11,fdtype arg12,
                      fdtype arg13,fdtype arg14)
{
  if (FD_EXPECT_FALSE(f->fcn_xcall))
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

static fdtype dcall15(struct FD_FUNCTION *f,
                      fdtype arg1,fdtype arg2,fdtype arg3,
                      fdtype arg4,fdtype arg5,fdtype arg6,
                      fdtype arg7,fdtype arg8,fdtype arg9,
                      fdtype arg10,fdtype arg11,fdtype arg12,
                      fdtype arg13,fdtype arg14,fdtype arg15)
{
  if (FD_EXPECT_FALSE(f->fcn_xcall))
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

FD_FASTOP int check_typeinfo(struct FD_FUNCTION *f,int n,fdtype *args)
{
  /* Check typeinfo */
  int *typeinfo = f->fcn_typeinfo; int i = 0;
  if (typeinfo) while (i<n) {
      fdtype arg = args[i];
      int argtype = typeinfo[i];
      if (argtype<0) i++;
      else if (FD_TYPEP(arg,argtype)) i++;
      else if ( (FD_VOIDP(arg)) || (FD_DEFAULTP(arg))) i++;
      else {
        u8_string type_name = fd_type2name(argtype);
        fd_seterr(fd_TypeError,type_name,u8dup(f->fcn_name),args[i]);
        return -1;}}
  return 0;
}

FD_FASTOP fdtype *prepare_argbuf(struct FD_FUNCTION *f,int n,
                                 fdtype *argbuf,
                                 fdtype *argvec)
{
  int arity = f->fcn_arity, min_arity = f->fcn_min_arity;
  fdtype fptr = (fdtype)f;
  if ((min_arity>0) && (n<min_arity)) {
    fd_xseterr(fd_TooFewArgs,"fd_dapply",f->fcn_name,fptr);
    return NULL;}
  else if ((arity>=0) && (n>arity)) {
    fd_xseterr(fd_TooManyArgs,"fd_dapply",f->fcn_name,fptr);
    return NULL;}
  else if ((arity<0)||(arity == n))
    return argvec;
  else {
    fdtype *defaults = f->fcn_defaults;
    int i=0; while (i<n) {
      fdtype v = argvec[i];
      if ( (v == FD_VOID) ||
           (v == FD_DEFAULT_VALUE) ||
           (v == FD_NULL) ) {
        if (defaults) {
          argbuf[i]=defaults[i];
          fd_incref(defaults[i]);}
        else if (v==FD_NULL)
          argbuf[i]=FD_VOID;
        else {}}
      else argbuf[i]=argvec[i];
      i++;}
    if (defaults)
      while (i<arity) { argbuf[i]=defaults[i]; i++;}
    else while (i<arity) { argbuf[i++]=FD_VOID; }
    return argbuf;}
}

FD_FASTOP fdtype dcall(u8_string fname,fd_function f,int n,fdtype *args)
{
  if (FD_INTERRUPTED()) return FD_ERROR_VALUE;
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
      return fd_applyfns[ctype]((fdtype)f,n,args);
    else return fd_type_error("applicable","dcall",(fdtype)f);}
}

FD_FASTOP fdtype apply_fcn(struct FD_STACK *stack,
                           u8_string name,fd_function f,int n,
                           fdtype *argvec)
{
  fdtype fnptr = (fdtype)f, arity=f->fcn_arity;
  if (FD_EXPECT_FALSE(n<0))
    return fd_err(_("Negative arg count"),"apply_fcn",name,fnptr);
  else if (arity<0) { /* Is a LEXPR */
    if (n<(f->fcn_min_arity))
      return fd_err(fd_TooFewArgs,"apply_fcn",f->fcn_name,fnptr);
    else if ( (f->fcn_xcall) && (f->fcn_handler.xcalln) )
      return f->fcn_handler.xcalln(f,n,argvec);
    else if (f->fcn_handler.calln)
      return f->fcn_handler.calln(n,argvec);
    else {
      /* There's no explicit method on this function object, so we use
         the method on the type (if there is one) */
      int ctype = FD_CONS_TYPE(f);
      if (fd_applyfns[ctype])
        return fd_applyfns[ctype]((fdtype)f,n,argvec);
      else return fd_err("NotApplicable","apply_fcn",f->fcn_name,fnptr);}}
  else if (n==arity)
    return dcall(name,f,n,argvec);
  else {
    fdtype argbuf[arity];
    fdtype *args = prepare_argbuf(f,n,argbuf,argvec);
    if (args == NULL)
      return FD_ERROR_VALUE;
    else if (check_typeinfo(f,n,args)<0)
      return FD_ERROR_VALUE;
    else return dcall(name,f,n,args);}
}

FD_EXPORT fdtype fd_docall(struct FD_STACK *_stack,
                           fdtype fn,int n,fdtype *argvec)
{
  u8_byte namebuf[60];
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
    sprintf(namebuf,"λ0x%llx",U8_PTR2INT(fn));
    fname=namebuf;}
  else return fd_type_error("applicable","fd_determinstic_apply",fn);

  /* Make the call */
  if (stackcheck()) {
    fdtype result=FD_VOID;
    FD_APPLY_STACK(apply_stack,fname,fn);
    apply_stack->stack_args=argvec;
    apply_stack->n_args=n;
    U8_WITH_CONTOUR(fname,0)
      if (f) result=apply_fcn(apply_stack,fname,f,n,argvec);
      else result=fd_applyfns[ftype](fn,n,argvec);
    U8_ON_EXCEPTION {
      U8_CLEAR_CONTOUR();
      result = FD_ERROR_VALUE;}
    U8_END_EXCEPTION;
    if ((errno)&&(!(FD_TROUBLEP(result)))) {
      u8_string cond=u8_strerror(errno);
      u8_log(LOG_WARN,cond,"Unexpected errno=%d (%s) after %s",
             errno,cond,U8ALT(fname,"primcall"));
      errno=0;}
    if ( (FD_TROUBLEP(result)) &&  (u8_current_exception==NULL) ) {
      if (errno) u8_graberrno("fd_apply",fname);
      else fd_seterr(fd_UnknownError,"fd_apply",fname,FD_VOID);}
    fd_pop_stack(apply_stack);
    if (FD_EXPECT_TRUE(FD_CHECK_PTR(result)))
      return result;
    else return fd_badptr_err(result,"fd_deterministic_apply",fname);}
  else {
    u8_string limit=u8_mkstring("%lld",fd_stack_limit);
    fdtype depth=FD_INT2DTYPE(u8_stack_depth());
    return fd_err(fd_StackOverflow,fname,limit,depth);}
}

/* Calling non-deterministically */

#define FD_ADD_RESULT(to,result)                \
  if (to == FD_EMPTY_CHOICE) to = result;           \
  else {                                        \
    if (FD_TYPEP(to,fd_tailcall_type))          \
      to = fd_finish_call(to);                    \
    if (FD_TYPEP(result,fd_tailcall_type))      \
      result = fd_finish_call(result);            \
  FD_ADD_TO_CHOICE(to,result);}

static fdtype ndcall_loop
  (struct FD_STACK *_stack,
   struct FD_FUNCTION *f,fdtype *results,int *typeinfo,
   int i,int n,fdtype *nd_args,fdtype *d_args)
{
  fdtype retval=FD_VOID;
  if (i == n) {
    fdtype value = fd_dapply((fdtype)f,n,d_args);
    if (FD_ABORTP(value)) {
      return value;}
    else {
      value = fd_finish_call(value);
      if (FD_ABORTP(value)) return value;
      FD_ADD_RESULT(*results,value);}}
  else if (FD_TYPEP(nd_args[i],fd_qchoice_type)) {
    d_args[i]=FD_XQCHOICE(nd_args[i])->qchoiceval;
    return ndcall_loop(_stack,f,results,typeinfo,i+1,n,nd_args,d_args);}
  else if ((!(FD_CHOICEP(nd_args[i]))) ||
           ((typeinfo)&&(typeinfo[i]==fd_choice_type))) {
    d_args[i]=nd_args[i];
    retval = ndcall_loop(_stack,f,results,typeinfo,i+1,n,nd_args,d_args);}
  else {
    FD_DO_CHOICES(elt,nd_args[i]) {
      d_args[i]=elt;
      retval = ndcall_loop(_stack,f,results,typeinfo,i+1,n,nd_args,d_args);}}
  if (FD_ABORTP(retval))
    return FD_ERROR_VALUE;
  else return *results;
}

static fdtype ndapply1(fd_stack _stack,fdtype fp,fdtype args1)
{
  fdtype results = FD_EMPTY_CHOICE;
  FD_DO_CHOICES(arg1,args1) {
    fdtype r = fd_dapply(fp,1,&arg1);
    if (FD_ABORTP(r)) {
      FD_STOP_DO_CHOICES;
      fd_decref(results);
      return r;}
    else {FD_ADD_RESULT(results,r);}}
  fd_pop_stack(_stack);
  return results;
}

static fdtype ndapply2(fd_stack _stack,fdtype fp,fdtype args0,fdtype args1)
{
  fdtype results = FD_EMPTY_CHOICE;
  FD_DO_CHOICES(arg0,args0) {
    FD_DO_CHOICES(arg1,args1) {
      fdtype argv[2]={arg0,arg1};
      fdtype r = fd_dapply(fp,2,argv);
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

static fdtype ndapply3(fd_stack _stack,fdtype fp,fdtype args0,fdtype args1,fdtype args2)
{
  fdtype results = FD_EMPTY_CHOICE;
  FD_DO_CHOICES(arg0,args0) {
    FD_DO_CHOICES(arg1,args1) {
      FD_DO_CHOICES(arg2,args2) {
        fdtype argv[3]={arg0,arg1,arg2};
        fdtype r = fd_dapply(fp,3,argv);
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

static fdtype ndapply4(fd_stack _stack,
                       fdtype fp,
                       fdtype args0,fdtype args1,
                       fdtype args2,fdtype args3)
{
  fdtype results = FD_EMPTY_CHOICE;
  FD_DO_CHOICES(arg0,args0) {
    FD_DO_CHOICES(arg1,args1) {
      FD_DO_CHOICES(arg2,args2) {
        FD_DO_CHOICES(arg3,args3) {
          fdtype argv[4]={arg0,arg1,arg2,arg3};
          fdtype r = fd_dapply(fp,4,argv);
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

FD_EXPORT fdtype fd_ndcall(struct FD_STACK *_stack,
                           fdtype fp,int n,fdtype *args)
{
  fdtype handler = (FD_FCNIDP(fp) ? (fd_fcnid_ref(fp)) : (fp));
  if (FD_EMPTY_CHOICEP(handler)) return FD_EMPTY_CHOICE;
  else if (FD_CHOICEP(handler)) {
    FD_APPLY_STACK(ndapply_stack,"fnchoices",handler);
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(h, handler) {
      fdtype r=fd_call(ndapply_stack,h,n,args);
      if (FD_ABORTP(r)) {
        fd_decref(results);
        FD_STOP_DO_CHOICES;
        fd_pop_stack(ndapply_stack);
        return r;}
      else {FD_ADD_TO_CHOICE(results,r);}}
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
        FD_PUSH_STACK(ndstack,fd_ndcallstack_type,f->fcn_name,handler);
        fdtype d_args[n]; /* *d_args=fd_alloca(n); */
        fdtype retval, results = FD_EMPTY_CHOICE;
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
        fd_exception ex = (n>f->fcn_arity) ? (fd_TooManyArgs) :
          (fd_TooFewArgs);
        return fd_err(ex,"fd_ndapply",f->fcn_name,FDTYPE_CONS(f));}}
    else if (fd_applyfns[fntype])
      return (fd_applyfns[fntype])(handler,n,args);
    else return fd_type_error("Applicable","fd_ndapply",handler);
  }
}

/* The default apply function */

static int contains_qchoicep(int n,fdtype *args);
static fdtype qchoice_dcall
(fd_stack stack,fdtype fp,int n,fdtype *args);

FD_EXPORT fdtype fd_call(struct FD_STACK *_stack,
                         fdtype fp,int n,fdtype *args)
{
  struct FD_FUNCTION *f = FD_DTYPE2FCN(fp); fdtype result;
  if (f->fcn_ndcall)
    if (!(FD_EXPECT_FALSE(contains_qchoicep(n,args))))
      result = fd_dcall(_stack,(fdtype)f,n,args);
    else result = qchoice_dcall(_stack,fp,n,args);
  else {
    int i = 0, qchoice = 0;
    while (i<n)
      if (args[i]==FD_EMPTY_CHOICE)
        return FD_EMPTY_CHOICE;
      else if (FD_ATOMICP(args[i])) i++;
      else {
        fd_ptr_type argtype = FD_PTR_TYPE(args[i]);
        if ((argtype == fd_choice_type) ||
            (argtype == fd_prechoice_type)) {
          result = fd_ndcall(_stack,(fdtype)f,n,args);
          return fd_finish_call(result);}
        else if (argtype == fd_qchoice_type) {
          qchoice = 1; i++;}
        else i++;}
    if (qchoice)
      result=qchoice_dcall(_stack,(fdtype)f,n,args);
    else result=fd_dcall(_stack,(fdtype)f,n,args);}
  return fd_finish_call(result);
}

static int contains_qchoicep(int n,fdtype *args)
{
  fdtype *scan = args, *limit = args+n;
  while (scan<limit)
    if (FD_QCHOICEP(*scan)) return 1;
    else scan++;
  return 0;
}

static fdtype qchoice_dcall(struct FD_STACK *stck,fdtype fp,int n,fdtype *args)
{
  fdtype argbuf[n]; /* *argbuf=fd_alloca(n); */
  fdtype *read = args, *limit = read+n, *write=argbuf;
  while (read<limit)
    if (FD_QCHOICEP(*read)) {
      struct FD_QCHOICE *qc = (struct FD_QCHOICE *) (*read++);
      *write++=qc->qchoiceval;}
    else *write++= *read++;
  return fd_dcall(stck,fp,n,argbuf);
}

/* Apply wrappers (which finish calls) */

FD_EXPORT fdtype _fd_stack_apply
(struct FD_STACK *stack,fdtype fn,int n_args,fdtype *args)
{
  return fd_stack_apply(stack,fn,n_args,args);
}
FD_EXPORT fdtype _fd_stack_dapply
(struct FD_STACK *stack,fdtype fn,int n_args,fdtype *args)
{
  return fd_dapply(fn,n_args,args);
}
FD_EXPORT fdtype _fd_stack_ndapply
(struct FD_STACK *stack,fdtype fn,int n_args,fdtype *args)
{
  return fd_ndapply(fn,n_args,args);
}

/* Tail calls */

FD_EXPORT fdtype fd_tail_call(fdtype fcn,int n,fdtype *vec)
{
  if (FD_FCNIDP(fcn)) fcn = fd_fcnid_ref(fcn);
  struct FD_FUNCTION *f = (struct FD_FUNCTION *)fcn;
  if (FD_EXPECT_FALSE(((f->fcn_arity)>=0) && (n>(f->fcn_arity)))) {
    fd_seterr(fd_TooManyArgs,"fd_tail_call",
              u8_mkstring("%d",n),
              fcn);
    return FD_ERROR_VALUE;}
  else {
    int atomic = 1, nd = 0; fdtype fcnid = f->fcnid;
    struct FD_TAILCALL *tc = (struct FD_TAILCALL *)
      u8_malloc(sizeof(struct FD_TAILCALL)+sizeof(fdtype)*n);
    fdtype *write = &(tc->tailcall_head);
    fdtype *write_limit = write+(n+1);
    fdtype *read = vec;
    FD_INIT_FRESH_CONS(tc,fd_tailcall_type);
    tc->tailcall_arity = n+1;
    tc->tailcall_flags = 0;
    if (fcnid == FD_NULL) {fcnid = f->fcnid = FD_VOID;}
    if (FD_FCNIDP(fcnid))
      *write++=fcnid;
    else *write++=fd_incref(fcn);
    while (write<write_limit) {
      fdtype v = *read++;
      if (FD_CONSP(v)) {
        atomic = 0;
        if (FD_QCHOICEP(v)) {
          struct FD_QCHOICE *qc = (fd_qchoice)v;
          fdtype cv = qc->qchoiceval;
          fd_incref(cv);
          *write++=cv;}
        else {
          if (FD_CHOICEP(v)) nd = 1;
          *write++=fd_incref(v);}}
      else *write++=v;}
    if (atomic) tc->tailcall_flags |= FD_TAILCALL_ATOMIC_ARGS;
    if (nd) tc->tailcall_flags |= FD_TAILCALL_ND_ARGS;
    return FDTYPE_CONS(tc);}
}
FD_EXPORT fdtype fd_void_tail_call(fdtype fcn,int n,fdtype *vec)
{
  if (FD_FCNIDP(fcn)) fcn = fd_fcnid_ref(fcn);
  struct FD_FUNCTION *f = (struct FD_FUNCTION *)fcn;
  if (FD_EXPECT_FALSE(((f->fcn_arity)>=0) && (n>(f->fcn_arity)))) {
    fd_seterr(fd_TooManyArgs,"fd_void_tail_call",
              u8_mkstring("%d",n),fcn);
    return FD_ERROR_VALUE;}
  else {
    int atomic = 1, nd = 0;
    struct FD_TAILCALL *tc = (struct FD_TAILCALL *)
      u8_malloc(sizeof(struct FD_TAILCALL)+sizeof(fdtype)*n);
    fdtype *write = &(tc->tailcall_head);
    fdtype *write_limit = write+(n+1);
    fdtype *read = vec;
    FD_INIT_FRESH_CONS(tc,fd_tailcall_type);
    tc->tailcall_arity = n+1;
    tc->tailcall_flags = FD_TAILCALL_VOID_VALUE;
    *write++=fd_incref(fcn);
    while (write<write_limit) {
      fdtype v = *read++;
      if (FD_CONSP(v)) {
        atomic = 0;
        if (FD_QCHOICEP(v)) {
          struct FD_QCHOICE *qc = (fd_qchoice)v;
          fdtype cv = qc->qchoiceval;
          fd_incref(cv);
          *write++=cv;}
        else {
          if (FD_CHOICEP(v)) nd = 1;
          *write++=fd_incref(v);}}
      else *write++=v;}
    if (atomic) tc->tailcall_flags |= FD_TAILCALL_ATOMIC_ARGS;
    if (nd) tc->tailcall_flags |= FD_TAILCALL_ND_ARGS;
    return FDTYPE_CONS(tc);}
}

FD_EXPORT fdtype fd_step_call(fdtype c)
{
  struct FD_TAILCALL *tc=
    fd_consptr(struct FD_TAILCALL *,c,fd_tailcall_type);
  int discard = U8_BITP(tc->tailcall_flags,FD_TAILCALL_VOID_VALUE);
  int n=tc->tailcall_arity;
  fdtype head=tc->tailcall_head;
  fdtype *arg0=(&(tc->tailcall_head))+1;
  fdtype result=
    ((tc->tailcall_flags&FD_TAILCALL_ND_ARGS)?
     (fd_apply(head,n-1,arg0)):
     (fd_dapply(head,n-1,arg0)));
  fd_decref(c);
  if (discard) {
    if (FD_TAILCALLP(result))
      return result;
    fd_decref(result);
    return FD_VOID;}
  else return result;
}

FD_EXPORT fdtype _fd_finish_call(fdtype call)
{
  if (FD_TAILCALLP(call)) {
    fdtype result = FD_VOID;
    while (1) {
      struct FD_TAILCALL *tc=
        fd_consptr(struct FD_TAILCALL *,call,fd_tailcall_type);
      int n=tc->tailcall_arity;
      int flags = tc->tailcall_flags;
      fdtype head=tc->tailcall_head;
      fdtype *args=(&(tc->tailcall_head))+1;
      int voidval = (U8_BITP(flags,FD_TAILCALL_VOID_VALUE));
      fdtype next = (U8_BITP(flags,FD_TAILCALL_ND_ARGS)) ?
        (fd_apply(head,n-1,args)) :
        (fd_dapply(head,n-1,args));
      int finished = (!(FD_TYPEP(next,fd_tailcall_type)));
      fd_decref(call); call = next;
      if (finished) {
        if (voidval) {
          fd_decref(next);
          result = FD_VOID;}
        else result = next;
        break;}}
    return result;}
  else return call;
}
static int unparse_tail_call(struct U8_OUTPUT *out,fdtype x)
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
  fdtype *scan = &(tc->tailcall_head), *limit = scan+n_elts;
  size_t tc_size = sizeof(struct FD_TAILCALL)+
    (sizeof(fdtype)*(n_elts-1));
  if (!(tc->tailcall_flags&FD_TAILCALL_ATOMIC_ARGS)) {
    while (scan<limit) {fd_decref(*scan); scan++;}}
  /* The head is always incref'd */
  else fd_decref(*scan);
  memset(tc,0,tc_size);
  if (mallocd) u8_free(tc);
}

/* Initializations */

static u8_condition DefnFailed=_("Definition Failed");

FD_EXPORT void fd_defn(fdtype table,fdtype fcn)
{
  struct FD_FUNCTION *f = fd_consptr(struct FD_FUNCTION *,fcn,fd_cprim_type);
  if (fd_store(table,fd_intern(f->fcn_name),fcn)<0)
    u8_raise(DefnFailed,"fd_defn",NULL);
}

FD_EXPORT void fd_idefn(fdtype table,fdtype fcn)
{
  struct FD_FUNCTION *f = fd_consptr(struct FD_FUNCTION *,fcn,fd_cprim_type);
  if (fd_store(table,fd_intern(f->fcn_name),fcn)<0)
    u8_raise(DefnFailed,"fd_defn",NULL);
  fd_decref(fcn);
}

FD_EXPORT void fd_defalias(fdtype table,u8_string to,u8_string from)
{
  fdtype to_symbol = fd_intern(to);
  fdtype from_symbol = fd_intern(from);
  fdtype v = fd_get(table,from_symbol,FD_VOID);
  fd_store(table,to_symbol,v);
  fd_decref(v);
}

FD_EXPORT void fd_defalias2(fdtype table,u8_string to,fdtype src,u8_string from)
{
  fdtype to_symbol = fd_intern(to);
  fdtype from_symbol = fd_intern(from);
  fdtype v = fd_get(src,from_symbol,FD_VOID);
  fd_store(table,to_symbol,v);
  fd_decref(v);
}

static fdtype dapply(fdtype f,int n_args,fdtype *argvec)
{
  return fd_dapply(f,n_args,argvec);
}

void fd_init_calltrack_c(void);
void fd_init_cprims_c(void);
void fd_init_ffi_c(void);

FD_EXPORT void fd_init_apply_c()
{
  int i = 0; while (i < FD_TYPE_MAX) fd_applyfns[i++]=NULL;
  i = 0; while (i < FD_TYPE_MAX) fd_functionp[i++]=0;

  fd_functionp[fd_fcnid_type]=1;

  fd_applyfns[fd_cprim_type]=dapply;

  u8_register_source_file(_FILEINFO);
  u8_register_source_file(FRAMERD_APPLY_H_INFO);

#if (FD_USE_TLS)
  u8_new_threadkey(&fd_stack_limit_key,NULL);
  u8_new_threadkey(&fd_stackptr_key,NULL);
#endif

  fd_unparsers[fd_tailcall_type]=unparse_tail_call;
  fd_recyclers[fd_tailcall_type]=recycle_tail_call;
  fd_type_names[fd_tailcall_type]="tailcall";

  u8_register_threadinit(init_thread_stack_limit);

  fd_register_config
    ("STACKLIMIT",
     _("Size of the stack (in bytes or as a factor of the current size)"),
     fd_sizeconfig_get,fd_sizeconfig_set,&fd_default_stackspec);

  u8_register_threadinit(init_thread_stack_limit);

  fd_init_cprims_c();
  fd_init_ffi_c();
  fd_init_calltrack_c();
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
