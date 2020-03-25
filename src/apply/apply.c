/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
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

#include "apply_internals.h"

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

u8_string kno_ndcallstack_type = "ndapply";
u8_string kno_callstack_type   = "apply";

#define KNO_APPLY_STACK(name,fname,fn)			\
  KNO_PUSH_STACK(name,kno_callstack_type,fname,fn)

u8_condition kno_NotAFunction=_("calling a non function");
u8_condition kno_TooManyArgs=_("too many arguments");
u8_condition kno_TooFewArgs=_("too few arguments");
u8_condition kno_NoDefault=_("no default value for #default argument");
u8_condition kno_ProfilingDisabled=_("profiling not built");
u8_condition kno_VoidArgument=_("VOID result passed as argument");


static lispval moduleid_symbol;

/* Whether to always use extended apply profiling */
int kno_extended_profiling = 0;

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
#else
#define stackcheck() (1)
#endif

/* Generic calling function */

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
  else if (KNO_FCN_CPRIMP(f))
    return cprim_call(name,(kno_cprim)f,n,argvec,stack);
  else if (FCN_XCALLP(f))
    return f->fcn_handler.xcalln(stack,f,n,argvec);
  else return f->fcn_handler.calln(n,argvec);
}

KNO_FASTOP lispval core_call(kno_stack stack,
			     lispval fn,kno_function f,
			     int n,kno_argvec argvec)
{
  int width = ( (f) && (f->fcn_call_width > n) )? (f->fcn_call_width) : (n);
  int argcheck = -1;
  kno_lisp_type fntype = KNO_PRIM_TYPE(fn);
  if ( (width == n) && (argcheck=check_args(n,argvec) == 0) ) {
    KNO_STACK_SET_CALL(stack,fn,n,argvec,KNO_STACK_STATIC_CALL);
    if (f)
      return function_call(stack->stack_label,f,n,argvec,stack);
    else return kno_applyfns[fntype](fn,width,argvec);}
  else {
    lispval callbuf[width];
    argcheck = setup_call(stack,fn,width,callbuf,n,argvec);
    if (argcheck<0) return KNO_ERROR;
    KNO_STACK_SET_CALL(stack,fn,width,(kno_argvec)callbuf,
		       KNO_STACK_STATIC_CALL);
    if (f)
      return function_call(stack->stack_label,f,
			   n,(kno_argvec)callbuf,
			   stack);
    else return kno_applyfns[fntype](fn,n,(kno_argvec)callbuf);}
}

static lispval profiled_call(kno_stack stack,
			     lispval fn,kno_function f,
			     int n,kno_argvec argvec,
			     kno_profile profile)
{
  lispval result = KNO_VOID;
#if (KNO_EXTENDED_PROFILING)
  struct rusage before = { 0 };
  if (kno_extended_profiling)
#if (HAVE_DECL_RUSAGE_THREAD)
    getrusage(RUSAGE_THREAD,&before);
#else
    getrusage(RUSAGE_SELF,&before);
#endif
#endif
#if HAVE_CLOCK_GETTIME
    struct timespec start;
  clock_gettime(CLOCK_MONOTONIC,&start);
#endif

  /* Here's where we actually apply the function */
  result = core_call(stack,fn,f,n,argvec);

  long long nsecs = 0;
  long long stime = 0, utime = 0;
  long long n_waits = 0, n_pauses = 0, n_faults = 0;
#if HAVE_CLOCK_GETTIME
  struct timespec end;
  clock_gettime(CLOCK_MONOTONIC,&end);
  nsecs = ((end.tv_sec*1000000000)+(end.tv_nsec)) -
    ((start.tv_sec*1000000000)+(start.tv_nsec));
#endif
#if ( (KNO_EXTENDED_PROFILING) && (HAVE_DECL_RUSAGE_THREAD) )
  if (kno_extended_profiling) {
    struct rusage after = { 0 };
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

static void badptr_err(lispval result,lispval fn)
{
  u8_byte numbuf[64];
  if (errno) u8_graberrno("badptr",NULL);
  kno_seterr( kno_get_pointer_exception(result),"kno_dcall",
	      u8_uitoa16(KNO_LONGVAL(result),numbuf),
	      fn);
}

KNO_FASTOP int setup_call_stack(kno_stack stack,
				lispval fn,int n,kno_argvec args,
				kno_function *fcnp)
{
  kno_lisp_type ftype=KNO_TYPEOF(fn);
  if (ftype==kno_fcnid_type) {
    fn=kno_fcnid_ref(fn);
    ftype=KNO_TYPEOF(fn);}

  if ( (KNO_FUNCTION_TYPEP(ftype)) || (kno_isfunctionp[ftype]) ) {
    kno_function f = (kno_function) fn;
    int min_arity = f->fcn_min_arity;
    int max_arity = f->fcn_arity;
    u8_string label = stack->stack_label;
    if (fcnp) *fcnp = f;
    if ( (label) && (stack->stack_bits & (KNO_STACK_FREE_LABEL) ) ) {
      U8_CLEARBITS(stack->stack_bits,(KNO_STACK_FREE_LABEL));
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

  KNO_STACK_SET_CALL(stack,fn,n,args,KNO_STACK_STATIC_CALL);

  return 1;
}

static lispval profiled_dcall
(struct KNO_STACK *caller,lispval fn,
 int n,kno_argvec argvec)
{
  u8_string fname="apply";
  if (caller == NULL) caller = kno_stackptr;

  /* Make the call */
  if (stackcheck()) {
    lispval result=VOID;
    struct KNO_FUNCTION *f = NULL;
    struct KNO_PROFILE *profile = NULL;
    struct KNO_STACK stack = { 0 };
    KNO_SETUP_STACK((&stack),NULL,kno_apply_stack);
    int setup = setup_call_stack(&stack,fn,n,argvec,&f);
    if (setup < 0) return KNO_ERROR;
    if (f) profile = f->fcn_profile;
    if ( (profile) && (profile->prof_disabled) ) profile=NULL;
    KNO_PUSH_STACK(&stack);

    U8_WITH_CONTOUR(fname,0) {
      if (profile == NULL)
	result = core_call(&stack,fn,f,n,argvec);
      else result = profiled_call(&stack,fn,f,n,argvec,profile);
      if (!(KNO_CHECK_PTR(result))) {
	result = KNO_ERROR;
	badptr_err(result,fn);}
      if (errno) errno_warning(stack.stack_label);}
    U8_ON_EXCEPTION {
      U8_CLEAR_CONTOUR();
      result = KNO_ERROR;}
    U8_END_EXCEPTION;
    if (KNO_PRECHOICEP(result)) {
      result = kno_simplify_choice(result);
      kno_return_from(&stack,result);}
    else kno_return_from(&stack,result);}
  else {
    u8_string limit=u8_mkstring("%lld",kno_stack_limit);
    lispval depth=KNO_INT2LISP(u8_stack_depth());
    return kno_err(kno_StackOverflow,fname,limit,depth);}
}

static lispval contoured_dcall
(struct KNO_STACK *caller,lispval fn,
 int n,kno_argvec argvec)
{
  u8_string fname="apply";
  if (caller == NULL) caller = kno_stackptr;

  /* Make the call */
  if (stackcheck()) {
    lispval result=VOID;

    struct KNO_FUNCTION *f = NULL;
    struct KNO_STACK stack = { 0 };
    KNO_SETUP_STACK((&stack),NULL,kno_apply_stack);
    int setup = setup_call_stack(&stack,fn,n,argvec,&f);
    if (setup < 0) return KNO_ERROR;
    KNO_PUSH_STACK(&stack);

    U8_WITH_CONTOUR(fname,0) {
      result = core_call(&stack,fn,f,n,argvec);}
    U8_ON_EXCEPTION {
      U8_CLEAR_CONTOUR();
      result = KNO_ERROR;}
    U8_END_EXCEPTION;
    if (KNO_PRECHOICEP(result)) {
      result = kno_simplify_choice(result);
      kno_return_from(&stack,result);}
    else kno_return_from(&stack,result);}
  else {
    u8_string limit=u8_mkstring("%lld",kno_stack_limit);
    lispval depth=KNO_INT2LISP(u8_stack_depth());
    return kno_err(kno_StackOverflow,fname,limit,depth);}
}

static lispval reckless_dcall
(struct KNO_STACK *caller,lispval fn,
 int n,kno_argvec argvec)
{
  struct KNO_FUNCTION *f = NULL;
  struct KNO_STACK stack = { 0 };
  KNO_SETUP_STACK((&stack),NULL,kno_apply_stack);
  int setup = setup_call_stack(&stack,fn,n,argvec,&f);
  if (setup < 0) return KNO_ERROR;
  KNO_PUSH_STACK(&stack);

  lispval result = core_call(&stack,fn,f,n,argvec);

  if (!(KNO_CHECK_PTR(result))) {
    result = KNO_ERROR;
    badptr_err(result,fn);}

  if (KNO_PRECHOICEP(result)) {
    result = kno_simplify_choice(result);
    kno_return_from(&stack,result);}
  else kno_return_from(&stack,result);
}

KNO_EXPORT lispval kno_dcall(struct KNO_STACK *caller,
			     lispval fn,int n,kno_argvec argvec)
{
  if (caller == NULL) caller = kno_stackptr;
  if (caller == NULL)
    return reckless_dcall(caller,fn,n,argvec);
  int call_flags = ( (caller->stack_flags) & (KNO_STACK_CALL_MASK) );
  switch (call_flags) {
  case 0: return profiled_dcall(caller,fn,n,argvec);
  case KNO_STACK_NO_CONTOURS: return reckless_dcall(caller,fn,n,argvec);
  case KNO_STACK_NO_PROFILING: return contoured_dcall(caller,fn,n,argvec);
  default: return profiled_dcall(caller,fn,n,argvec);
  }
}

static lispval simple_dcall(lispval fn,int n,kno_argvec args)
{
  return kno_dcall(kno_stackptr,fn,n,args);
}

/* Calling non-deterministically */

#define KNO_ADD_RESULT(to,result)		\
  if (to == EMPTY) to = result;			\
  else {CHOICE_ADD(to,result);}

static lispval ndcall_loop
(struct KNO_STACK *_stack,
 lispval f,lispval *results,
 int i,int n,kno_argvec nd_args,lispval *d_args)
{
  if (i == n) {
    lispval result = kno_dcall(_stack,f,n,d_args);
    if (KNO_ABORTED(result))
      return result;
    else {KNO_ADD_TO_CHOICE(*results,result);}
    return result;}
  else {
    lispval nd_arg = nd_args[i],
      free_simple = KNO_VOID,
      retval = VOID;
    if (!(KNO_CONSP(nd_arg))) {
      d_args[i]=nd_arg;
      return ndcall_loop(_stack,f,results,i+1,n,nd_args,d_args);}
    if (PRECHOICEP(nd_arg)) {
      nd_arg = kno_make_simple_choice(nd_arg);
      free_simple = nd_arg;}
    if (CHOICEP(nd_arg)) {
      DO_CHOICES(elt,nd_args[i]) {
	d_args[i]=elt;
	retval = ndcall_loop(_stack,f,results,i+1,n,nd_args,d_args);
	if (ABORTED(retval)) break;}
      kno_decref(free_simple);
      return retval;}
    else if (QCHOICEP(nd_arg))
      d_args[i] = KNO_QCHOICEVAL(nd_arg);
    else d_args[i] = nd_arg;
    if (KNO_VOIDP(free_simple))
      return ndcall_loop(_stack,f,results,i+1,n,nd_args,d_args);
    retval = ndcall_loop(_stack,f,results,i+1,n,nd_args,d_args);
    kno_decref(free_simple);
    return retval;}
}

static lispval ndapply1(kno_stack _stack,
                        lispval fp,
                        lispval args1)
{
  lispval results = EMPTY;
  DO_CHOICES(arg1,args1) {
    lispval r = kno_dcall(_stack,fp,1,&arg1);
    if (KNO_ABORTP(r)) {
      KNO_STOP_DO_CHOICES;
      kno_decref(results);
      return r;}
    else {KNO_ADD_RESULT(results,r);}}
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
	lispval r = kno_dcall(_stack,fp,2,argv);
        if (KNO_ABORTP(r)) {
          kno_decref(results);
          results = r;
          KNO_STOP_DO_CHOICES;
          break;}
        else {KNO_ADD_RESULT(results,r);}}}
    if (KNO_ABORTP(results)) {
      KNO_STOP_DO_CHOICES;
      break;}}
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
	    lispval r = kno_dcall(_stack,fp,3,argv);
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
		lispval r = kno_dcall(_stack,fp,4,argv);
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
  return kno_simplify_choice(results);
}

static lispval ndcall(struct KNO_STACK *stack,lispval h,int n,kno_argvec args)
{
  kno_lisp_type fntype = KNO_TYPEOF(h);
  if (KNO_FUNCTION_TYPEP(fntype)) {
    struct KNO_FUNCTION *f = KNO_GETFUNCTION(h);
    if ( (f->fcn_arity==0) || (f->fcn_call & KNO_FCN_CALL_NDOP) )
      return kno_dcall(stack,h,n,args);
    else {
      if ((f->fcn_arity < 0) ?
	  (n >= (f->fcn_min_arity)) :
	  ((n <= (f->fcn_arity)) &&
	   (n >= (f->fcn_min_arity)))) {
	/* Check the arg count */
	int i = 0, no_choices = 1; while (i<n) {
	  lispval arg = args[i++];
	  if (KNO_EMPTYP(arg))
	    return KNO_EMPTY_CHOICE;
	  else if ( (KNO_CHOICEP(arg)) || (KNO_PRECHOICEP(arg)) ) {
	    no_choices = 0; break;}
	  else NO_ELSE;}
	if (no_choices)
	  return kno_dcall(stack,h,n,args);
	else if (n==1)
	  return ndapply1(stack,h,args[0]);
	else if (n==2)
	  return ndapply2(stack,h,args[0],args[1]);
	else if (n==3)
	  return ndapply3(stack,h,args[0],args[1],args[2]);
	else if (n==4)
	  return ndapply4(stack,h,args[0],args[1],args[2],args[3]);
	else {
	  lispval d_args[n], results = KNO_EMPTY;
	  lispval retval = ndcall_loop(stack,h,&results,0,n,args,d_args);
	  if (KNO_ABORTP(retval)) {
	    kno_decref(results);
	    return retval;}
	  else if (KNO_PRECHOICEP(results))
	    return kno_simplify_choice(results);
	  else return results;}}
      u8_condition ex = (n>f->fcn_arity) ? (kno_TooManyArgs) : (kno_TooFewArgs);
      return kno_err(ex,"kno_ndapply",f->fcn_name,LISP_CONS(f));}}
  else if (kno_applyfns[fntype])
    return kno_dcall(stack,h,n,args);
  else return kno_type_error("Applicable","ndcall",h);
}

static int nd_argp(int n,kno_argvec args)
{
  int i = 0; while (i<n) {
    lispval arg = args[i];
    if (KNO_CONSP(arg)) {
      kno_lisp_type argtype = KNO_TYPEOF(arg);
      if ( (argtype == kno_choice_type) ||
	   (argtype == kno_prechoice_type) )
	return 1;
      else i++;}
    else i++;}
  return 0;
}

KNO_EXPORT lispval kno_call(struct KNO_STACK *_stack,
			    lispval fp,int n,kno_argvec args)
{
  lispval handler = (KNO_FCNIDP(fp) ? (kno_fcnid_ref(fp)) : (fp));
  if (EMPTYP(handler)) return EMPTY;

  int iter_choices = 0;
  if ( (KNO_CHOICEP(handler)) || (KNO_PRECHOICEP(handler)) )
    iter_choices = 1;
  else if (KNO_FUNCTIONP(handler)) {
    kno_function f = (kno_function) handler;
    if (f->fcn_call & KNO_FCN_CALL_NDOP)
      iter_choices = 0;
    else if (nd_argp(n,args))
      iter_choices = 1;
    else NO_ELSE;}
  else if (kno_isndfunctionp[KNO_PRIM_TYPE(handler)])
    iter_choices = 0;
  else if (nd_argp(n,args))
    iter_choices = 1;
  else NO_ELSE;
  if (iter_choices) {
    struct KNO_STACK iter_stack = { 0 };
    KNO_SETUP_STACK(&iter_stack,"fnchoices",kno_iter_stacktype);
    int checked = setup_call_stack(&iter_stack,handler,n,args,NULL);
    if (checked < 0) return KNO_ERROR;
    KNO_PUSH_STACK(&iter_stack);
    lispval results=EMPTY;
    if (CHOICEP(handler)) {
      DO_CHOICES(h,handler) {
	lispval r = ndcall(&iter_stack,h,n,args);
	if (KNO_ABORTP(r)) {
	  kno_decref(results);
	  KNO_STOP_DO_CHOICES;
	  kno_pop_stack(&iter_stack);
	  return r;}
	else {CHOICE_ADD(results,r);}}}
    else results = ndcall(&iter_stack,handler,n,args);
    kno_pop_stack(&iter_stack);
    if (KNO_PRECHOICEP(results))
      return kno_simplify_choice(results);
    else return results;}
  else return kno_dcall(_stack,handler,n,args);
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

void kno_init_cprims_c(void);
void kno_init_stacks_c(void);
void kno_init_lexenv_c(void);
void kno_init_ffi_c(void);

/* PROFILED functions config */

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
  else return KNO_ERR(-1,"Not a standard function",
		      "config_add_profiled",
		      NULL,val);
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
  else return kno_isfunctionp[typecode];
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
  moduleid_symbol = kno_getsym("%MODULEID");

  kno_isfunctionp[kno_fcnid_type]=1;

  kno_applyfns[kno_cprim_type]=simple_dcall;

  u8_register_source_file(_FILEINFO);
  u8_register_source_file(KNO_APPLY_H_INFO);

#if (KNO_USE_TLS)
  u8_new_threadkey(&kno_stack_limit_key,NULL);
#endif

  u8_init_mutex(&profiled_lock);

  kno_register_config
    ("XPROFILING",_("Whether to use extended apply profiling"),
     kno_boolconfig_get,kno_boolconfig_set,&kno_extended_profiling);

  kno_register_config
    ("PROFILED",_("Functions declared for profiling"),
     kno_lconfig_get,config_add_profiled,&profiled_fcns);

  kno_init_cprims_c();
  kno_init_ffi_c();
  kno_init_stacks_c();
  kno_init_lexenv_c();
}

