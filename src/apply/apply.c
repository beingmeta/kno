/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_FCNIDS 1
#define KNO_INLINE_STACKS 1
#define KNO_INLINE_LEXENV 1
#define KNO_INLINE_APPLY  1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/lexenv.h"
#include "kno/stacks.h"
#include "kno/profiles.h"
#include "kno/apply.h"

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

lispval kno_default_stackspec = VOID;
u8_string kno_ndcallstack_type = "ndapply";
u8_string kno_callstack_type   = "apply";

#define KNO_APPLY_STACK(name,fname,fn)			\
  KNO_PUSH_STACK(name,kno_callstack_type,fname,fn)

u8_condition kno_NotAFunction=_("calling a non function");
u8_condition kno_TooManyArgs=_("too many arguments");
u8_condition kno_TooFewArgs=_("too few arguments");
u8_condition kno_NoDefault=_("no default value for #default argument");
u8_condition kno_ProfilingDisabled=_("profiling not built");

u8_condition kno_NoStackChecking=
  _("Stack checking is not available in this build of Kno");
u8_condition kno_StackTooSmall=
  _("This value is too small for an application stack limit");
u8_condition kno_StackTooLarge=
  _("This value is larger than the available stack space");
u8_condition kno_BadStackSize=
  _("This is an invalid stack size");
u8_condition kno_BadStackFactor=
  _("This is an invalid stack resize factor");
u8_condition kno_InternalStackSizeError=
  _("Internal stack size didn't make any sense, punting");

#if ((KNO_THREADS_ENABLED)&&(KNO_USE_TLS))
u8_tld_key kno_stack_limit_key;
#elif ((KNO_THREADS_ENABLED)&&(HAVE_THREAD_STORAGE_CLASS))
__thread ssize_t kno_stack_limit = -1;
#else
ssize_t kno_stack_limit = -1;
#endif

static lispval moduleid_symbol;

/* Whether to always use extended apply profiling */
int kno_extended_profiling = 0;

/* Stack checking */

int kno_wrap_apply = KNO_WRAP_APPLY_DEFAULT;

#if KNO_STACKCHECK
static int stackcheck()
{
  if (kno_stack_limit>=KNO_MIN_STACKSIZE) {
    ssize_t depth = u8_stack_depth();
    if (depth>kno_stack_limit)
      return 0;
    else return 1;}
  else return 1;
}
KNO_EXPORT ssize_t kno_stack_getsize()
{
  if (kno_stack_limit>=KNO_MIN_STACKSIZE)
    return kno_stack_limit;
  else return -1;
}
KNO_EXPORT ssize_t kno_stack_setsize(ssize_t limit)
{
  if (limit<KNO_MIN_STACKSIZE) {
    char *detailsbuf = u8_malloc(64);
    u8_seterr("StackLimitTooSmall","kno_stack_setsize",
              u8_write_long_long(limit,detailsbuf,64));
    return -1;}
  else {
    ssize_t maxstack = u8_stack_size;
    if (maxstack < KNO_MIN_STACKSIZE) {
      u8_seterr(kno_InternalStackSizeError,"kno_stack_resize",NULL);
      return -1;}
    else if (limit <=  maxstack) {
      ssize_t old = kno_stack_limit;
      kno_set_stack_limit(limit);
      return old;}
    else {
#if ( (HAVE_PTHREAD_SELF) &&                    \
      (HAVE_PTHREAD_GETATTR_NP) &&              \
      (HAVE_PTHREAD_ATTR_SETSTACKSIZE) )
      pthread_t self = pthread_self();
      pthread_attr_t attr;
      if (pthread_getattr_np(self,&attr)) {
        u8_graberrno("kno_set_stacksize",NULL);
        return -1;}
      else if ((pthread_attr_setstacksize(&attr,limit))) {
        u8_graberrno("kno_set_stacksize",NULL);
        return -1;}
      else {
        ssize_t old = kno_stack_limit;
        kno_set_stack_limit(limit-limit/8);
        return old;}
#else
      char *detailsbuf = u8_malloc(64);
      u8_seterr("StackLimitTooLarge","kno_stack_setsize",
                u8_write_long_long(limit,detailsbuf,64));
      return -1;
#endif
    }}
}
KNO_EXPORT ssize_t kno_stack_resize(double factor)
{
  if ( (factor<0) || (factor>1000) ) {
    u8_log(LOG_WARN,kno_BadStackFactor,
           "The value %f is not a valid stack resize factor",factor);
    return kno_stack_limit;}
  else {
    ssize_t current = kno_stack_limit;
    ssize_t limit = u8_stack_size;
    if (limit<KNO_MIN_STACKSIZE) limit = current;
    return kno_stack_setsize((limit*factor));}
}
#else
static int youve_been_warned = 0;
#define stackcheck() (1)
KNO_EXPORT ssize_t kno_stack_getsize()
{
  if (youve_been_warned) return -1;
  u8_log(LOG_WARN,"NoStackChecking",
         "Stack checking is not enabled in this build");
  youve_been_warned = 1;
  return -1;
}
KNO_EXPORT ssize_t kno_stack_setsize(ssize_t limit)
{
  u8_log(LOG_WARN,"NoStackChecking",
         "Stack checking is not enabled in this build");
  return -1;
}
KNO_EXPORT ssize_t kno_stack_resize(double factor)
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

KNO_EXPORT int kno_stackcheck()
{
  return stackcheck();
}

/* Initialize stack limits */

KNO_EXPORT ssize_t kno_init_cstack()
{
  u8_init_stack();
  if (VOIDP(kno_default_stackspec)) {
    ssize_t stacksize = u8_stack_size;
    return kno_stack_setsize(stacksize-stacksize/8);}
  else if (FIXNUMP(kno_default_stackspec))
    return kno_stack_setsize((ssize_t)(FIX2INT(kno_default_stackspec)));
  else if (KNO_FLONUMP(kno_default_stackspec))
    return kno_stack_resize(KNO_FLONUM(kno_default_stackspec));
  else if (KNO_BIGINTP(kno_default_stackspec)) {
    unsigned long long val = kno_getint(kno_default_stackspec);
    if (val) return kno_stack_setsize((ssize_t) val);
    else {
      u8_log(LOG_CRIT,kno_BadStackSize,
             "The default stack value %q wasn't a valid stack size",
             kno_default_stackspec);
      return -1;}}
  else {
    u8_log(LOG_CRIT,kno_BadStackSize,
           "The default stack value %q wasn't a valid stack size",
           kno_default_stackspec);
    return -1;}
}

static int init_thread_stack_limit()
{
  kno_init_cstack();
  return 1;
}

/* Call stack (not used yet) */

KNO_EXPORT void _kno_free_stack(struct KNO_STACK *stack)
{
  kno_pop_stack(stack);
}

KNO_EXPORT void _kno_pop_stack(struct KNO_STACK *stack)
{
  kno_pop_stack(stack);
}

#if 0
KNO_EXPORT struct KNO_STACK_CLEANUP *_kno_add_cleanup
(struct KNO_STACK *stack,kno_stack_cleanop op,void *arg0,void *arg1)
{
  return kno_add_cleanup(stack,op,arg0,arg1);
}
#endif

/* Calling primitives */

static lispval dcall0(struct KNO_FUNCTION *f)
{
  return f->fcn_handler.call0(f);
}
static lispval dcall1(struct KNO_FUNCTION *f,lispval arg1)
{
  return f->fcn_handler.call1(arg1);
}
static lispval dcall2(struct KNO_FUNCTION *f,lispval arg1,lispval arg2)
{
  return f->fcn_handler.call2(arg1,arg2);
}
static lispval dcall3(struct KNO_FUNCTION *f,
                      lispval arg1,lispval arg2,lispval arg3)
{
  return f->fcn_handler.call3(arg1,arg2,arg3);
}
static lispval dcall4(struct KNO_FUNCTION *f,
                      lispval arg1,lispval arg2,lispval arg3,lispval arg4)
{
  return f->fcn_handler.call4(arg1,arg2,arg3,arg4);
}
static lispval dcall5(struct KNO_FUNCTION *f,
                      lispval arg1,lispval arg2,lispval arg3,lispval arg4,
                      lispval arg5)
{
  return f->fcn_handler.call5(arg1,arg2,arg3,arg4,arg5);
}
static lispval dcall6(struct KNO_FUNCTION *f,
                      lispval arg1,lispval arg2,lispval arg3,lispval arg4,
                      lispval arg5,lispval arg6)
{
  return f->fcn_handler.call6(arg1,arg2,arg3,arg4,arg5,arg6);
}

static lispval dcall7(struct KNO_FUNCTION *f,
                      lispval arg1,lispval arg2,lispval arg3,lispval arg4,
                      lispval arg5,lispval arg6,lispval arg7)
{
  return f->fcn_handler.call7(arg1,arg2,arg3,arg4,arg5,arg6,arg7);
}

static lispval dcall8(struct KNO_FUNCTION *f,
                      lispval arg1,lispval arg2,lispval arg3,lispval arg4,
                      lispval arg5,lispval arg6,lispval arg7,lispval arg8)
{
  return f->fcn_handler.call8(arg1,arg2,arg3,arg4,arg5,arg6,arg7,arg8);
}

static lispval dcall9(struct KNO_FUNCTION *f,
                      lispval arg1,lispval arg2,lispval arg3,lispval arg4,
                      lispval arg5,lispval arg6,lispval arg7,lispval arg8,
                      lispval arg9)
{
  return f->fcn_handler.call9(arg1,arg2,arg3,
                              arg4,arg5,arg6,
                              arg7,arg8,arg9);
}

static lispval dcall10(struct KNO_FUNCTION *f,
                       lispval arg1,lispval arg2,lispval arg3,
                       lispval arg4,lispval arg5,lispval arg6,
                       lispval arg7,lispval arg8,lispval arg9,
                       lispval arg10)
{
  return f->fcn_handler.call10(arg1,arg2,arg3,
                               arg4,arg5,arg6,
                               arg7,arg8,arg9,
                               arg10);
}

static lispval dcall11(struct KNO_FUNCTION *f,
                       lispval arg1,lispval arg2,lispval arg3,
                       lispval arg4,lispval arg5,lispval arg6,
                       lispval arg7,lispval arg8,lispval arg9,
                       lispval arg10,lispval arg11)
{
  return f->fcn_handler.call11(arg1,arg2,arg3,
                               arg4,arg5,arg6,
                               arg7,arg8,arg9,
                               arg10,arg11);
}

static lispval dcall12(struct KNO_FUNCTION *f,
                       lispval arg1,lispval arg2,lispval arg3,
                       lispval arg4,lispval arg5,lispval arg6,
                       lispval arg7,lispval arg8,lispval arg9,
                       lispval arg10,lispval arg11,lispval arg12)
{
  return f->fcn_handler.call12(arg1,arg2,arg3,
                               arg4,arg5,arg6,
                               arg7,arg8,arg9,
                               arg10,arg11,arg12);
}

static lispval dcall13(struct KNO_FUNCTION *f,
                       lispval arg1,lispval arg2,lispval arg3,
                       lispval arg4,lispval arg5,lispval arg6,
                       lispval arg7,lispval arg8,lispval arg9,
                       lispval arg10,lispval arg11,lispval arg12,
                       lispval arg13)
{
  return f->fcn_handler.call13(arg1,arg2,arg3,
                               arg4,arg5,arg6,
                               arg7,arg8,arg9,
                               arg10,arg11,arg12,
                               arg13);
}

static lispval dcall14(struct KNO_FUNCTION *f,
                       lispval arg1,lispval arg2,lispval arg3,
                       lispval arg4,lispval arg5,lispval arg6,
                       lispval arg7,lispval arg8,lispval arg9,
                       lispval arg10,lispval arg11,lispval arg12,
                       lispval arg13,lispval arg14)
{
  return f->fcn_handler.call14(arg1,arg2,arg3,
                               arg4,arg5,arg6,
                               arg7,arg8,arg9,
                               arg10,arg11,arg12,
                               arg13,arg14);
}

static lispval dcall15(struct KNO_FUNCTION *f,
                       lispval arg1,lispval arg2,lispval arg3,
                       lispval arg4,lispval arg5,lispval arg6,
                       lispval arg7,lispval arg8,lispval arg9,
                       lispval arg10,lispval arg11,lispval arg12,
                       lispval arg13,lispval arg14,lispval arg15)
{
  return f->fcn_handler.call15(arg1,arg2,arg3,
                               arg4,arg5,arg6,
                               arg7,arg8,arg9,
                               arg10,arg11,arg12,
                               arg13,arg14,arg15);
}

/* Generic calling function */

KNO_FASTOP lispval fcn_call(u8_string fname,kno_function f,
			    int n,kno_argvec args,
			    kno_stack stack)
{
  if (PRED_FALSE((f->fcn_handler.fnptr==NULL))) {
    int ctype = KNO_CONS_TYPE(f);
    if (kno_applyfns[ctype])
      return kno_applyfns[ctype]((lispval)f,n,args);
    else return kno_type_error("applicable","dcall",(lispval)f);}
  else if (FCN_XCALLP(f))
    return f->fcn_handler.xcalln(stack,f,n,args);
  else if ( (FCN_LEXPRP(f)) || (f->fcn_arity < 0) )
    return f->fcn_handler.calln(n,args);
  else return VOID;
}

static int cprim_prep(u8_string fname,
		      int n_given,kno_argvec given,
		      int n_needed,lispval *needed,
		      kno_lisp_type *typeinfo,
		      const lispval *defaults);

KNO_FASTOP lispval cprim_call(u8_string fname,kno_cprim cp,
			      int n,kno_argvec args,
			      kno_stack stack)
{
  kno_lisp_type *typeinfo = cp->fcn_typeinfo;
  const lispval *defaults = cp->fcn_defaults;
  int arity = cp->fcn_arity, buflen = stack->stack_buflen;
  lispval *argbuf = stack->stack_buf;
  kno_function f = (kno_function) cp;
  if (PRED_FALSE(cprim_prep(fname,n,args,buflen,argbuf,typeinfo,defaults) < 0))
    return KNO_ERROR;
  else {
    stack->stack_args = (kno_argvec) argbuf;
    stack->stack_arglen = buflen;}
  if (FCN_XCALLP(f))
    return f->fcn_handler.xcalln(stack,(kno_function)f,n,argbuf);
  else if ( (FCN_LEXPRP(f)) || (arity < 0) )
    return f->fcn_handler.calln(n,argbuf);
  else switch (arity) {
    case 0: return dcall0(f);
    case 1: return dcall1(f,argbuf[0]);
    case 2: return dcall2(f,argbuf[0],argbuf[1]);
    case 3: return dcall3(f,argbuf[0],argbuf[1],argbuf[2]);
    case 4: return dcall4(f,argbuf[0],argbuf[1],argbuf[2],argbuf[3]);
    case 5: return dcall5(f,argbuf[0],argbuf[1],argbuf[2],argbuf[3],argbuf[4]);
    case 6:
      return dcall6(f,argbuf[0],argbuf[1],argbuf[2],argbuf[3],argbuf[4],argbuf[5]);
    case 7:
      return dcall7(f,argbuf[0],argbuf[1],argbuf[2],argbuf[3],
                    argbuf[4],argbuf[5],argbuf[6]);
    case 8:
      return dcall8(f,argbuf[0],argbuf[1],argbuf[2],argbuf[3],
                    argbuf[4],argbuf[5],argbuf[6],argbuf[7]);
    case 9:
      return dcall9(f,argbuf[0],argbuf[1],argbuf[2],argbuf[3],
                    argbuf[4],argbuf[5],argbuf[6],argbuf[7],argbuf[8]);
    case 10:
      return dcall10(f,argbuf[0],argbuf[1],argbuf[2],argbuf[3],
                     argbuf[4],argbuf[5],argbuf[6],argbuf[7],argbuf[8],
                     argbuf[9]);
    case 11:
      return dcall11(f,argbuf[0],argbuf[1],argbuf[2],
                     argbuf[3],argbuf[4],argbuf[5],
                     argbuf[6],argbuf[7],argbuf[8],
                     argbuf[9],argbuf[10]);
    case 12:
      return dcall12(f,argbuf[0],argbuf[1],argbuf[2],
                     argbuf[3],argbuf[4],argbuf[5],
                     argbuf[6],argbuf[7],argbuf[8],
                     argbuf[9],argbuf[10],argbuf[11]);
    case 13:
      return dcall13(f,argbuf[0],argbuf[1],argbuf[2],
                     argbuf[3],argbuf[4],argbuf[5],
                     argbuf[6],argbuf[7],argbuf[8],
                     argbuf[9],argbuf[10],argbuf[11],
                     argbuf[12]);
    case 14:
      return dcall14(f,argbuf[0],argbuf[1],argbuf[2],
                     argbuf[3],argbuf[4],argbuf[5],
                     argbuf[6],argbuf[7],argbuf[8],
                     argbuf[9],argbuf[10],argbuf[11],
                     argbuf[12],argbuf[13]);
    case 15:
      return dcall15(f,argbuf[0],argbuf[1],argbuf[2],
                     argbuf[3],argbuf[4],argbuf[5],
                     argbuf[6],argbuf[7],argbuf[8],
                     argbuf[9],argbuf[10],argbuf[11],
                     argbuf[12],argbuf[13],argbuf[14]);
    default:
      if (FCN_XCALLP(f))
	return f->fcn_handler.xcalln(stack,f,n,args);
      else return f->fcn_handler.calln(n,args);}
}

static int null_ptr_error(u8_string fname,int i)
{
  return KNO_ERR(-1,kno_NullPtr,"cprim_prep",fname,KNO_INT(i));
}

static int cprim_prep(u8_string fname,
		      int n_given,kno_argvec given,
		      int n_needed,kno_lispval *needed,
		      kno_lisp_type *typeinfo,
		      const lispval *defaults)
{
  if ( (typeinfo) && (defaults) ) {
    int i = 0; while (i < n_given) {
      lispval arg = given[i];
      if (KNO_NULLP(arg)) return null_ptr_error(fname,i);
      else if ( (KNO_VOIDP(arg)) || (KNO_DEFAULTP(arg)) ) {
	/* Note that the defaults for cprims can't be conses, so we don't bother with
	   incref/decref */
	needed[i] = defaults[i]; i++;
	continue;}
      else if (KNO_QCHOICEP(arg))
	arg = (KNO_XQCHOICE(arg))->qchoiceval;
      else NO_ELSE;
      if (typeinfo[i]>0) {
	/* We don't check the type of defaults, since it lets
	   the handler use non-standard arguments as signals. */
	if (! (PRED_TRUE(KNO_TYPEP(arg,typeinfo[i]))) ) {
	  u8_byte buf[128];
	  kno_seterr(kno_TypeError,kno_type_names[typeinfo[i]],
		     u8_bprintf(buf,"%s[%d]",fname,i),
		     arg);
	  return -1;}}
      needed[i] = arg;
      i++;}
    while (i < n_needed) {
      needed[i] = defaults[i];
      i++;}
    return i;}
  else if (defaults) {
    int i = 0; while (i < n_given) {
      lispval arg = given[i];
      if (KNO_NULLP(arg)) return null_ptr_error(fname,i);
      if ( (KNO_VOIDP(arg)) || (KNO_DEFAULTP(arg)) ) {
	/* Note that the defaults can't be conses */
	needed[i] = defaults[i]; i++;
	continue;}
      else if (KNO_QCHOICEP(arg))
	arg = (KNO_XQCHOICE(arg))->qchoiceval;
      else needed[i] = arg;
      i++;}
    while (i < n_needed) {
      needed[i] = defaults[i];
      i++;}
    return i;}
  else if (typeinfo) {
    int i = 0; while (i < n_given) {
      lispval arg = given[i];
      if (KNO_NULLP(arg)) return null_ptr_error(fname,i);
      else if (KNO_QCHOICEP(arg))
	arg = (KNO_XQCHOICE(arg))->qchoiceval;
      else NO_ELSE;
      if (typeinfo[i]>0) {
	if (! (PRED_TRUE(KNO_TYPEP(arg,typeinfo[i]))) ) {
	  u8_byte buf[128];
	  kno_seterr(kno_TypeError,kno_type_names[typeinfo[i]],
		     u8_bprintf(buf,"%s[%d]",fname,i),
		     arg);
	  return -1;}}
      needed[i] = arg;
      i++;}
    while (i < n_needed) {needed[i++] = KNO_VOID;}
    return i;}
  else  {
    int i = 0; while (i < n_given) {
      lispval arg = given[i];
      if (KNO_NULLP(arg)) return null_ptr_error(fname,i);
      else if (KNO_QCHOICEP(arg))
	arg = (KNO_XQCHOICE(arg))->qchoiceval;
      else NO_ELSE;
      needed[i] = arg;
      i++;}
    while (i < n_needed) {needed[i++] = KNO_VOID;}
    return i;}
}

KNO_FASTOP lispval opaque_call
(lispval fn,int n,kno_argvec argvec,struct KNO_STACK *stack)
{
  int ctype = KNO_TYPEOF(fn);
  if (kno_applyfns[ctype])
    return kno_applyfns[ctype](fn,n,argvec);
  else return kno_err("NotApplicable","opaque_apply",
		      kno_type_names[ctype],
		      fn);
}

KNO_FASTOP lispval function_call(u8_string name,kno_function f,
				 int n,kno_argvec argvec,
				 struct KNO_STACK *stack)
{
  if (PRED_FALSE(f->fcn_handler.fnptr == NULL)) {
    /* There's no explicit method on this function object, so we use
       the method associated with the lisp type (if there is one) */
    int ctype = KNO_CONS_TYPE(f);
    if (kno_applyfns[ctype])
      return kno_applyfns[ctype]((lispval)f,n,argvec);
    else return kno_err("NotApplicable","apply_fcn",f->fcn_name,(lispval)f);}
  else if (FCN_XCALLP(f))
    return f->fcn_handler.xcalln(stack,f,n,argvec);
  else if (KNO_FCN_CPRIMP(f))
    return cprim_call(name,(kno_cprim)f,n,argvec,stack);
  else return f->fcn_handler.calln(n,argvec);
}

KNO_FASTOP lispval core_apply(kno_stack stack,
			      lispval fn,kno_function f,
			      int n,kno_argvec argvec)
{
  if ( (f) && (KNO_CONS_TYPE(f) == kno_cprim_type) )
    return cprim_call(stack->stack_label,(kno_cprim)f,n,argvec,stack);
  else if (f)
    return function_call(stack->stack_label,f,n,argvec,stack);
  else return opaque_call(fn,n,argvec,stack);
}

static lispval profiled_apply(kno_stack stack,
			      lispval fn,kno_function f,
			      int n,kno_argvec argvec,
			      kno_profile profile)
{
  lispval result = KNO_VOID;
  long long nsecs = 0;
  long long stime = 0, utime = 0;
  long long n_waits = 0, n_pauses = 0, n_faults = 0;
#if ( (KNO_EXTENDED_PROFILING) && (HAVE_DECL_RUSAGE_THREAD) )
  struct rusage before = { 0 }, after = { 0 };
  if (kno_extended_profiling) getrusage(RUSAGE_THREAD,&before);
#endif
#if HAVE_CLOCK_GETTIME
  struct timespec start, end;
  clock_gettime(CLOCK_MONOTONIC,&start);
#endif

  /* Here's where we actually apply the function */
  result = core_apply(stack,fn,f,n,argvec);

#if HAVE_CLOCK_GETTIME
  clock_gettime(CLOCK_MONOTONIC,&end);
  nsecs = ((end.tv_sec*1000000000)+(end.tv_nsec)) -
    ((start.tv_sec*1000000000)+(start.tv_nsec));
#endif
#if ( (KNO_EXTENDED_PROFILING) && (HAVE_DECL_RUSAGE_THREAD) )
  if (kno_extended_profiling) {
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
    n_pauses = after.ru_nivcsw - before.ru_nivcsw;
#endif
  }
#endif
  kno_profile_record(profile,0,nsecs,utime,stime,
		     n_waits,n_pauses,n_faults);
  return result;
}

static int too_few_args(lispval fn,u8_string fname,int n,int min,int max)
{
  kno_lisp_type ftype = KNO_TYPEOF(fn);
  u8_byte buf[64], namebuf[64];
  if (fname == NULL) {
    u8_string type_name = kno_type_names[ftype];
    if (!(type_name)) type_name="applicable";
    fname = u8_bprintf(namebuf,"<%s>0x%llx",type_name,KNO_LONGVAL(fn));}
  kno_seterr(kno_TooFewArgs,"kno_dcall",
	     ((max>=0) ?
	      (u8_bprintf(buf,"%s %d args < [%d-%d] expected",
			  fname,n,min,max)) :
	      (u8_bprintf(buf,"%s %d args < [%d+] expected",
			  fname,n,min))),
	     fn);
  return -1;
}

static int too_many_args(lispval fn,u8_string fname,int n,int min,int max)
{
  kno_lisp_type ftype = KNO_TYPEOF(fn);
  u8_byte buf[64], namebuf[64];
  if (fname == NULL) {
    u8_string type_name = kno_type_names[ftype];
    if (!(type_name)) type_name="applicable";
    fname = u8_bprintf(namebuf,"<%s>0x%llx",type_name,KNO_LONGVAL(fn));}
  kno_seterr(kno_TooManyArgs,"kno_dcall",
	     u8_bprintf(buf,"%s %d args > [%d,%d] expected",
			fname,n,min,max),
	     fn);
  return -1;
}

static void badptr_err(lispval result,lispval fn)
{
  u8_byte numbuf[64];
  if (errno) u8_graberrno("badptr",NULL);
  kno_seterr( kno_get_pointer_exception(result),"kno_dcall",
	      u8_uitoa16(KNO_LONGVAL(result),numbuf),
	      fn);
}


KNO_FASTOP int setup_call_stack(kno_stack stack,lispval fn,int n,
				kno_function *fcnp)
{
  kno_lisp_type ftype=KNO_TYPEOF(fn);
  if (ftype==kno_fcnid_type) {
    fn=kno_fcnid_ref(fn);
    ftype=KNO_TYPEOF(fn);}

  if ( (KNO_FUNCTION_TYPEP(ftype)) || (kno_function_types[ftype]) ) {
    kno_function f = (kno_function) fn;
    int min_arity = f->fcn_min_arity;
    int max_arity = f->fcn_arity;
    u8_string label = stack->stack_label;
    if (fcnp) *fcnp = f;
    if ( (label) && (stack->stack_flags & (KNO_STACK_FREE_LABEL) ) ) {
      U8_CLEARBITS(stack->stack_flags,(KNO_STACK_FREE_LABEL));
      u8_free(label);}
    stack->stack_label = label = f->fcn_name;
    if ( (min_arity > 0) && (n < min_arity) )
      return too_few_args(fn,label,n,min_arity,max_arity);
    else if ( (max_arity >= 0) && (n > max_arity) )
      return too_many_args(fn,label,n,min_arity,max_arity);
    else NO_ELSE;}
  else if (fcnp)
    *fcnp = NULL;
  else NO_ELSE;

  stack->stack_op = fn;

  return 1;
}

static void errno_warning(u8_string label)
{
  u8_string cond=u8_strerror(errno);
  u8_log(LOG_WARN,cond,"Unexpected errno=%d (%s) after %s",
	 errno,cond,(U8ALT(label,"core_apply")));
  errno=0;
}

KNO_EXPORT lispval kno_dcall
(struct KNO_STACK *caller,lispval fn,
 int n,kno_argvec argvec)
{
  u8_string fname="apply";
  if (caller == NULL) caller = kno_stackptr;

  /* Make the call */
  if (stackcheck()) {
    lispval result=VOID;
    struct KNO_FUNCTION *f;
    struct KNO_PROFILE *profile;
    KNO_NEW_STACK(caller,kno_callstack_type,NULL,VOID);
    int callbuf_width;
    int setup = setup_call_stack(_stack,fn,n,&f);
    if (setup < 0)
      _return KNO_ERROR;
    _stack->stack_args = argvec;
    _stack->stack_arglen = n;
    if (f) {
      profile = f->fcn_profile;
      int call_width = f->fcn_call_width;
      callbuf_width = (call_width<0) ? (n) : call_width;}
    else {
      profile = NULL;
      callbuf_width = n;}
    lispval callbuf[callbuf_width];
    int i = 0; while (i<callbuf_width) callbuf[i++] = VOID;
    _stack->stack_buf = callbuf;
    _stack->stack_buflen = callbuf_width;

    U8_WITH_CONTOUR(fname,0) {
      while (1) {
	_stack->stack_flags |= KNO_STACK_TAILPOS;
	argvec = _stack->stack_args;
	n = _stack->stack_arglen;
	if (profile == NULL)
	  result = core_apply(_stack,fn,f,n,argvec);
	else result = profiled_apply(_stack,fn,f,n,argvec,profile);
	if (!(KNO_CHECK_PTR(result))) {
	  badptr_err(result,fn);
	  break;}
	else if (KNO_ABORTED(result))
	  break;
	if (errno) errno_warning(_stack->stack_label);
	if (result != KNO_TAIL_CALL) break;
	fn = _stack->stack_op;
	int next_setup = setup_call_stack(_stack,fn,n,&f);
	if (next_setup < 0) break;
	if (f) profile = f->fcn_profile;
	else profile = NULL;}}
    U8_ON_EXCEPTION {
      U8_CLEAR_CONTOUR();
      result = KNO_ERROR;}
    U8_END_EXCEPTION;
    _return kno_simplify_choice(result);}
  else {
    u8_string limit=u8_mkstring("%lld",kno_stack_limit);
    lispval depth=KNO_INT2LISP(u8_stack_depth());
    return kno_err(kno_StackOverflow,fname,limit,depth);}
}

/* Calling non-deterministically */

#define KNO_ADD_RESULT(to,result)               \
  if (to == EMPTY) to = result;                 \
  else {CHOICE_ADD(to,result);}

static lispval ndcall_loop
(struct KNO_STACK *_stack,
 struct KNO_FUNCTION *f,lispval *results,
 int i,int n,kno_argvec nd_args,lispval *d_args)
{
  lispval retval=VOID;
  if (i == n) {
    lispval value = kno_stack_dapply(_stack,(lispval)f,n,d_args);
    if (KNO_ABORTP(value)) {
      return value;}
    else {KNO_ADD_RESULT(*results,value);}}
  else {
    DO_CHOICES(elt,nd_args[i]) {
      d_args[i]=elt;
      retval = ndcall_loop(_stack,f,results,i+1,n,nd_args,d_args);}}
  if (KNO_ABORTP(retval))
    return KNO_ERROR;
  else return *results;
}

static lispval ndapply1(kno_stack _stack,
                        lispval fp,
                        lispval args1)
{
  lispval results = EMPTY;
  DO_CHOICES(arg1,args1) {
    lispval r = kno_stack_dapply(_stack,fp,1,&arg1);
    if (KNO_ABORTP(r)) {
      KNO_STOP_DO_CHOICES;
      kno_decref(results);
      kno_pop_stack(_stack);
      return r;}
    else {KNO_ADD_RESULT(results,r);}}
  kno_pop_stack(_stack);
  return kno_simplify_choice(results);
}

static lispval ndapply2(kno_stack _stack,
                        lispval fp,
                        lispval args0,lispval args1)
{
  lispval results = EMPTY;
  DO_CHOICES(arg0,args0) {
    {DO_CHOICES(arg1,args1) {
        lispval argv[2]={arg0,arg1};
	lispval r = kno_stack_dapply(_stack,fp,2,argv);
        if (KNO_ABORTP(r)) {
          kno_decref(results);
          results = r;
          KNO_STOP_DO_CHOICES;
          break;}
        else {KNO_ADD_RESULT(results,r);}}}
    if (KNO_ABORTP(results)) {
      KNO_STOP_DO_CHOICES;
      break;}}
  kno_pop_stack(_stack);
  return kno_simplify_choice(results);
}

static lispval ndapply3(kno_stack _stack,
                        lispval fp,
                        lispval args0,lispval args1,lispval args2)
{
  lispval results = EMPTY;
  DO_CHOICES(arg0,args0) {
    {DO_CHOICES(arg1,args1) {
        {DO_CHOICES(arg2,args2) {
            lispval argv[3]={arg0,arg1,arg2};
	    lispval r = kno_stack_dapply(_stack,fp,3,argv);
            if (KNO_ABORTP(r)) {
              kno_decref(results);
              results = r;
              KNO_STOP_DO_CHOICES;
              break;}
            else {KNO_ADD_RESULT(results,r);}}}
        if (KNO_ABORTP(results)) {
          KNO_STOP_DO_CHOICES;
          break;}}}
    if (KNO_ABORTP(results)) {
      KNO_STOP_DO_CHOICES;
      break;}}
  kno_pop_stack(_stack);
  return kno_simplify_choice(results);
}

static lispval ndapply4(kno_stack _stack,
                        lispval fp,
                        lispval args0,lispval args1,
                        lispval args2,lispval args3)
{
  lispval results = EMPTY;
  DO_CHOICES(arg0,args0) {
    {DO_CHOICES(arg1,args1) {
        {DO_CHOICES(arg2,args2) {
            {DO_CHOICES(arg3,args3) {
                lispval argv[4]={arg0,arg1,arg2,arg3};
		lispval r = kno_stack_dapply(_stack,fp,4,argv);
                if (KNO_ABORTP(r)) {
                  kno_decref(results);
                  results = r;
                  KNO_STOP_DO_CHOICES;
                  break;}
                else {KNO_ADD_RESULT(results,r);}}}
            if (KNO_ABORTP(results)) {
              KNO_STOP_DO_CHOICES;
              break;}}}
        if (KNO_ABORTP(results)) {
          KNO_STOP_DO_CHOICES;
          break;}}}
    if (KNO_ABORTP(results)) {
      KNO_STOP_DO_CHOICES;
      break;}}
  kno_pop_stack(_stack);
  return kno_simplify_choice(results);
}

KNO_EXPORT lispval kno_ndcall(struct KNO_STACK *_stack,
                              lispval fp,
                              int n,kno_argvec args)
{
  lispval handler = (KNO_FCNIDP(fp) ? (kno_fcnid_ref(fp)) : (fp));
  if (EMPTYP(handler))
    return EMPTY;
  else if (CHOICEP(handler)) {
    KNO_APPLY_STACK(ndapply_stack,"fnchoices",handler);
    lispval results=EMPTY;
    DO_CHOICES(h,handler) {
      lispval r=kno_call(ndapply_stack,h,n,args);
      if (KNO_ABORTP(r)) {
        kno_decref(results);
        KNO_STOP_DO_CHOICES;
        kno_pop_stack(ndapply_stack);
        return r;}
      else {CHOICE_ADD(results,r);}}
    kno_pop_stack(ndapply_stack);
    return kno_simplify_choice(results);}
  else {
    kno_lisp_type fntype = KNO_TYPEOF(handler);
    if (KNO_FUNCTION_TYPEP(fntype)) {
      struct KNO_FUNCTION *f = KNO_GETFUNCTION(handler);
      if (f->fcn_arity==0)
	return kno_stack_dapply(_stack,handler,n,args);
      else if ((f->fcn_arity < 0) ?
               (n >= (f->fcn_min_arity)) :
               ((n <= (f->fcn_arity)) &&
                (n >= (f->fcn_min_arity)))) {
        lispval d_args[n];
        lispval retval, results = EMPTY;
        KNO_PUSH_STACK(ndstack,kno_ndcallstack_type,f->fcn_name,handler);
        ndstack->stack_src = f->fcn_filename;
        U8_CLEARBITS(ndstack->stack_flags,KNO_STACK_FREE_SRC);
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
        else retval = ndcall_loop(ndstack,f,&results,0,n,args,d_args);
        kno_pop_stack(ndstack);
        if (KNO_ABORTP(retval)) {
          kno_decref(results);
          return retval;}
        else return kno_simplify_choice(results);}
      else {
        u8_condition ex = (n>f->fcn_arity) ? (kno_TooManyArgs) :
          (kno_TooFewArgs);
        return kno_err(ex,"kno_ndapply",f->fcn_name,LISP_CONS(f));}}
    else if (kno_applyfns[fntype]) {
      lispval r = (kno_applyfns[fntype])(handler,n,args);
      return kno_simplify_choice(r);}
    else return kno_type_error("Applicable","kno_ndapply",handler);
  }
}

/* The default apply function */

KNO_EXPORT lispval kno_call(struct KNO_STACK *_stack,
			    lispval fp,
			    int n,kno_argvec args)
{
  lispval handler = (KNO_FCNIDP(fp)) ? (kno_fcnid_ref(fp)) : (fp);
  if (KNO_FUNCTIONP(handler))  {
    struct KNO_FUNCTION *f = (kno_function) handler;
    if ( (f) && (FCN_NDCALLP(f)) )
      return kno_dcall(_stack,(lispval)f,n,args);}
  if (kno_applyfns[KNO_PRIM_TYPE(handler)]) {
    int i = 0; while (i<n)
      if (args[i]==EMPTY)
        return EMPTY;
      else if (ATOMICP(args[i])) i++;
      else {
        kno_lisp_type argtype = KNO_TYPEOF(args[i]);
        if ((argtype == kno_choice_type) ||
            (argtype == kno_prechoice_type))
	  return kno_ndcall(_stack,handler,n,args);
	else i++;}
    return kno_dcall(_stack,handler,n,args);}
  else return kno_err("Not applicable","kno_call",NULL,fp);
}

/* Apply wrappers (which finish calls) */

KNO_EXPORT lispval _kno_stack_apply
(struct KNO_STACK *stack,lispval fn,int n_args,kno_argvec args)
{
  return kno_stack_apply(stack,fn,n_args,args);
}
KNO_EXPORT lispval _kno_stack_dapply
(struct KNO_STACK *stack,lispval fn,int n_args,kno_argvec args)
{
  return kno_stack_dapply(stack,fn,n_args,args);
}
KNO_EXPORT lispval _kno_stack_ndapply
(struct KNO_STACK *stack,lispval fn,int n_args,kno_argvec args)
{
  return kno_stack_ndapply(stack,fn,n_args,args);
}

/* Tail calls */

KNO_EXPORT lispval kno_tail_call(kno_stack stack,lispval fcn,
				 int n,kno_argvec args)
{
  if (stack == NULL) {
    stack = kno_stackptr;
    if (stack == NULL)
      return kno_dcall(NULL,fcn,n,args);}
  kno_stack scan = stack, found = NULL;
  while (scan->stack_flags & KNO_STACK_TAILPOS) {
    if ( (scan->stack_buflen >= n) &&
	 (scan->stack_buf) )
      found = scan;
    scan = scan->stack_caller;}
  if (found == NULL)
    return kno_dcall(NULL,fcn,n,args);
  else {
    int flags = found->stack_flags;
    int i = 0, stack_width = found->stack_buflen;
    lispval *argbuf = found->stack_buf;
    if (KNO_FCNIDP(fcn)) fcn = kno_fcnid_ref(fcn);
    if ( found->stack_op != fcn) {
      if (flags & KNO_STACK_DECREF_OP) kno_decref(found->stack_op);
      found->stack_op = fcn;}
    while (i<n) {
      lispval arg = args[i];
      lispval old_arg = argbuf[i];
      if (arg != old_arg) {
	argbuf[i] = arg;
	kno_decref(old_arg);}
      i++;}
    while (i < stack_width) {
      lispval old_arg = argbuf[i];
      argbuf[i++] = KNO_VOID;
      kno_decref(old_arg);}
    found->stack_args = (kno_argvec) argbuf; /* found->stack_arglen = n; */
    return KNO_TAIL_CALL;}
}

/* Initializations */

static u8_condition DefnFailed=_("Definition Failed");

KNO_EXPORT void kno_defn(lispval table,lispval fcn)
{
  struct KNO_FUNCTION *f = kno_consptr(struct KNO_FUNCTION *,fcn,kno_cprim_type);
  if (kno_store(table,kno_getsym(f->fcn_name),fcn)<0)
    u8_raise(DefnFailed,"kno_defn",NULL);
  if ( (KNO_NULLP(f->fcn_moduleid)) || (KNO_VOIDP(f->fcn_moduleid)) ) {
    lispval moduleid = kno_get(table,moduleid_symbol,KNO_VOID);
    if (!(KNO_VOIDP(moduleid))) f->fcn_moduleid=moduleid;}
}

KNO_EXPORT void kno_idefn(lispval table,lispval fcn)
{
  struct KNO_FUNCTION *f = kno_consptr(struct KNO_FUNCTION *,fcn,kno_cprim_type);
  if (kno_store(table,kno_getsym(f->fcn_name),fcn)<0)
    u8_raise(DefnFailed,"kno_defn",NULL);
  if ( (KNO_NULLP(f->fcn_moduleid)) || (KNO_VOIDP(f->fcn_moduleid)) ) {
    lispval moduleid = kno_get(table,moduleid_symbol,KNO_VOID);
    if (!(KNO_VOIDP(moduleid))) f->fcn_moduleid=moduleid;}
  kno_decref(fcn);
}

KNO_EXPORT void kno_defalias(lispval table,u8_string to,u8_string from)
{
  lispval to_symbol = kno_getsym(to);
  lispval from_symbol = kno_getsym(from);
  lispval v = kno_get(table,from_symbol,VOID);
  kno_store(table,to_symbol,v);
  kno_decref(v);
}

KNO_EXPORT void kno_defalias2(lispval table,
                              u8_string to,lispval src,
                              u8_string from)
{
  lispval to_symbol = kno_getsym(to);
  lispval from_symbol = kno_getsym(from);
  lispval v = kno_get(src,from_symbol,VOID);
  kno_store(table,to_symbol,v);
  kno_decref(v);
}

static lispval dapply(lispval f,int n_args,kno_argvec argvec)
{
  return kno_dapply(f,n_args,argvec);
}

void kno_init_cprims_c(void);
void kno_init_stacks_c(void);
void kno_init_lexenv_c(void);
void kno_init_ffi_c(void);

/* PROFILED config */

static lispval profiled_fcns = KNO_EMPTY;
static u8_mutex profiled_lock;

static int config_add_profiled(lispval var,lispval val,void *data)
{
  if (KNO_FUNCTIONP(val)) {
    struct KNO_FUNCTION *fcn = KNO_GETFUNCTION(val);
    if (fcn->fcn_profile)
      return 0;
    u8_lock_mutex(&profiled_lock);
    struct KNO_PROFILE *profile = kno_make_profile(fcn->fcn_name);
    lispval *ptr = (lispval *) data;
    lispval cur = *ptr;
    kno_incref(val);
    KNO_ADD_TO_CHOICE(cur,val);
    fcn->fcn_profile=profile;
    *ptr = cur;
    return 1;}
  else return KNO_ERR(-1,"Not a function","config_add_profiled",NULL,val);
}

/* External functional versions of common macros */

static int APPLICABLE_TYPEP(int typecode)
{
  if ( ((typecode) >= kno_cprim_type) &&
       ((typecode) <= kno_rpcproc_type) )
    return 1;
  else return ( (kno_applyfns[typecode]) != NULL);
}
KNO_EXPORT int _KNO_APPLICABLE_TYPEP(int typecode)
{
  return APPLICABLE_TYPEP(typecode);
}
KNO_EXPORT int _KNO_APPLICABLEP(lispval x)
{
  if (KNO_TYPEP(x,kno_fcnid_type))
    return (APPLICABLE_TYPEP(KNO_FCNID_TYPE(x)));
  else return APPLICABLE_TYPEP(KNO_PRIM_TYPE(x));
}

static int FUNCTION_TYPEP(int typecode)
{
  if ( ((typecode) == kno_cprim_type) ||
       ((typecode) == kno_lambda_type) )
    return 1;
  else return kno_function_types[typecode];
}
KNO_EXPORT int _KNO_FUNCTION_TYPEP(int typecode)
{
  return APPLICABLE_TYPEP(typecode);
}
KNO_EXPORT int _KNO_FUNCTIONP(lispval x)
{
  if (KNO_TYPEP(x,kno_fcnid_type))
    return (FUNCTION_TYPEP(KNO_FCNID_TYPE(x)));
  else return FUNCTION_TYPEP(KNO_PRIM_TYPE(x));
}

KNO_EXPORT int _KNO_LAMBDAP(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  return (KNO_TYPEP((x),kno_lambda_type));
}							 \

KNO_EXPORT kno_function _KNO_GETFUNCTION(lispval x)
{
  if (KNO_FCNIDP(x)) {
    x = kno_fcnid_ref(x);
    return (kno_function) x;}
  else return (kno_function)x;;
}

KNO_EXPORT kno_function _KNO_XFUNCTION(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  if (KNO_FUNCTIONP(x))
    return (kno_function) x;
  else return KNO_ERR(NULL,kno_NotAFunction,NULL,NULL,x);
}

/* Initializations */

KNO_EXPORT void kno_init_apply_c()
{
  int i = 0; while (i < KNO_TYPE_MAX) kno_applyfns[i++]=NULL;
  i = 0; while (i < KNO_TYPE_MAX) kno_function_types[i++]=0;

  moduleid_symbol = kno_getsym("%MODULEID");

  kno_function_types[kno_fcnid_type]=1;

  kno_applyfns[kno_cprim_type]=dapply;

  u8_register_source_file(_FILEINFO);
  u8_register_source_file(KNO_APPLY_H_INFO);

#if (KNO_USE_TLS)
  u8_new_threadkey(&kno_stack_limit_key,NULL);
#endif

#if 0
  kno_unparsers[kno_tailcall_type]=unparse_tail_call;
  kno_recyclers[kno_tailcall_type]=recycle_tail_call;
  kno_type_names[kno_tailcall_type]="tailcall";
#endif

  u8_register_threadinit(init_thread_stack_limit);
  u8_init_mutex(&profiled_lock);

  kno_register_config
    ("STACKLIMIT",
     _("Size of the stack (in bytes or as a factor of the current size)"),
     kno_sizeconfig_get,kno_sizeconfig_set,&kno_default_stackspec);

  kno_register_config
    ("XPROFILING",_("Whether to use extended apply profiling"),
     kno_boolconfig_get,kno_boolconfig_set,&kno_extended_profiling);

  kno_register_config
    ("PROFILED",_("Functions declared for profiling"),
     kno_lconfig_get,config_add_profiled,&profiled_fcns);

  u8_register_threadinit(init_thread_stack_limit);

  kno_init_cprims_c();
  kno_init_ffi_c();
  kno_init_stacks_c();
  kno_init_lexenv_c();
}

