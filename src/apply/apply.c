/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_STACKS 1
#define KNO_INLINE_LEXENV 1
#define KNO_INLINE_XTYPEP 1
#define KNO_INLINE_APPLY  1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/lexenv.h"
#include "kno/stacks.h"
#include "kno/apply.h"
#include "apply_internals.h"

#include <libu8/u8printf.h>
#include <libu8/u8logging.h>
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
#include "kno/profiles.h"

#include <stdarg.h>

#ifndef U8_MSG_NOTICE
#define U8_MSG_NOTICE (-(U8_LOG_NOTICE))
#endif

u8_condition kno_NotAFunction=_("calling a non function");
u8_condition kno_TooManyArgs=_("too many arguments");
u8_condition kno_TooFewArgs=_("too few arguments");
u8_condition kno_NoDefault=_("no default value for #default argument");
u8_condition kno_ProfilingDisabled=_("profiling not built");
u8_condition kno_VoidArgument=_("VOID result passed as argument");
u8_condition kno_SyntaxError=_("SCHEME expression syntax error");

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

KNO_FASTOP lispval function_call(u8_string name,lispval handler,
				 kno_function f,
				 int n,kno_argvec argvec,
				 struct KNO_STACK *stack)
{
  int call_width = f->fcn_call_width;
  const lispval *args, _args[call_width];
  KNO_STACK_SET_OP(stack,(lispval)f,0);
  if (f->fcn_filename) stack->stack_file = f->fcn_filename;
  if (f->fcn_name) stack->stack_label = f->fcn_name;
  if (call_width > n) {
    lispval *write = (lispval *) (args = _args);
    lspcpy(write,argvec,n); write=write+n;
    kno_lspset(write,KNO_VOID,call_width-n);}
  else args = argvec;
  int rv = (f->fcn_typeinfo) ?
    (check_argtypes(f,n,args)) :
    (check_args((lispval)f,n,args));
  U8_PAUSEPOINT();
  if (RARELY(rv<0)) return KNO_ERROR;
  if (RARELY(f->fcn_handler.fnptr == NULL)) {
    /* There's no explicit method on this function object, so we use
       the method associated with the lisp type (if there is one) */
    int ctype = KNO_CONS_TYPEOF(f);
    if (kno_applyfns[ctype])
      return kno_applyfns[ctype](handler,n,argvec);
    u8_string details = (f->fcn_name) ? (f->fcn_name) : kno_type_name(handler);
    return kno_err("NotApplicable","apply_fcn",details,handler);}
  else if (KNO_FCN_CPRIMP(f))
    return cprim_call(name,(kno_cprim)f,n,argvec,stack);
  else if (FCN_CXCALLP(f)) {
    if (KNO_TYPEP(handler,kno_closure_type))
      return f->fcn_handler.cxcalln(stack,handler,f,KNO_CDR(handler),n,argvec);
    else return f->fcn_handler.cxcalln(stack,handler,f,KNO_VOID,n,argvec);}
  else if (FCN_XCALLP(f))
    return f->fcn_handler.xcalln(stack,handler,n,argvec);
  else return f->fcn_handler.calln(n,argvec);
}

KNO_EXPORT int kno_trace_call(kno_stack,lispval,kno_function,int,kno_argvec);
KNO_EXPORT int kno_trace_exit(kno_stack,lispval,lispval,kno_function,int,kno_argvec);

static lispval traced_function_call(u8_string name,lispval handler,
				    kno_function f,
				    int n,kno_argvec argvec,
				    struct KNO_STACK *stack)
{
  int call_width = f->fcn_call_width, trace_bits = f->fcn_trace;
  const lispval *args, _args[call_width];
  KNO_STACK_SET_OP(stack,handler,0);
  if (f->fcn_filename) stack->stack_file = f->fcn_filename;
  if (f->fcn_name) stack->stack_label = f->fcn_name;
  if (call_width > n) {
    lispval *write = (lispval *) (args = _args);
    lspcpy(write,argvec,n); write=write+n;
    kno_lspset(write,KNO_VOID,call_width-n);}
  else args = argvec;
  kno_lisp_type fntype = FCN_TYPEOF(f);
  if (fntype==kno_lambda_type) {
    /* lambdas handle their own tracing in order to accurately
       report tail calls. */}
  else kno_trace_call(stack,handler,f,n,args);
  int rv = (f->fcn_typeinfo) ?
    (check_argtypes(f,n,args)) :
    (check_args((lispval)f,n,args));
  U8_PAUSEPOINT();
  if (RARELY(rv<0)) return KNO_ERROR;
  lispval result = KNO_VOID;
  if (RARELY(f->fcn_handler.fnptr == NULL)) {
    /* There's no explicit method on this function object, so we use
       the method associated with the lisp type (if there is one) */
    int ctype = KNO_CONS_TYPEOF(f);
    if (kno_applyfns[ctype])
      result=kno_applyfns[ctype](handler,n,argvec);
    else {
      u8_string details =
	(f->fcn_name) ? (f->fcn_name) : kno_type_name(handler);
      result=kno_err("NotApplicable","apply_fcn",details,handler);}}
  else if (KNO_FCN_CPRIMP(f))
    result=cprim_call(name,(kno_cprim)f,n,argvec,stack);
  else if (FCN_XCALLP(f))
    result=f->fcn_handler.xcalln(stack,(lispval)f,n,argvec);
  else result=f->fcn_handler.calln(n,argvec);
  if ( (trace_bits&KNO_FCN_TRACE_EXIT) &&
       (KNO_CONS_TYPEOF(f)!=kno_lambda_type) )
    /* lambdas handle their own tracing */
    kno_trace_exit(stack,result,handler,f,n,args);
  return result;
}

KNO_FASTOP lispval core_call(kno_stack stack,
			     lispval fn,kno_function f,
			     int n,kno_argvec argvec)
{
  lispval result = KNO_VOID;
  int width = ( (f) && (f->fcn_call_width > n) )? (f->fcn_call_width) : (n);
  kno_lisp_type fntype = KNO_PRIM_TYPE(fn);
  if ( (f) && (f->fcn_trace) )
    result = traced_function_call(f->fcn_name,fn,f,n,argvec,stack);
  if ( (f) && (fntype==kno_cprim_type) )
    result = cprim_call(f->fcn_name,(kno_cprim)f,n,argvec,stack);
  else if (f)
    result = function_call(f->fcn_name,fn,f,n,argvec,stack);
  else {
    KNO_STACK_SET_OP(stack,fn,0);
    KNO_STACK_SET_ARGS(stack,(lispval *)argvec,width,n,KNO_STATIC_ARGS);
    kno_applyfn handler = kno_applyfns[fntype];
    if (RARELY(handler==NULL))
      return kno_err(kno_NotAFunction,"core_call",kno_type_name(fn),fn);
    else result = kno_applyfns[fntype](fn,width,argvec);}
  if ( (KNO_ABORTED(result)) && ((u8_current_exception)==NULL) )
    kno_missing_error
      ( ( (f) && (f->fcn_name)) ? (f->fcn_name) : U8S("core_call"));
  return result;
}

static lispval profiled_call(kno_stack stack,
			     lispval fn,kno_function f,
			     int n,kno_argvec argvec,
			     kno_profile profile)
{
  lispval result = KNO_VOID;

  struct rusage before; struct timespec start;
  if (profile) kno_profile_start(&before,&start);

  /* Here's where we actually apply the function */
  result = core_call(stack,fn,f,n,argvec);

  kno_profile_update(profile,&before,&start,1);

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
  kno_function f = KNO_FUNCTION_INFO(fn);

  if (f) {
    u8_string label = stack->stack_label;
    int min_arity = f->fcn_min_arity;
    int max_arity = f->fcn_arity;
    if (fcnp) *fcnp = f;
    stack->stack_label = label = f->fcn_name;
    if ( (min_arity > 0) && (n < min_arity) )
      return too_few_args(fn,label,n,min_arity,max_arity);
    else if ( (max_arity >= 0) && (n > max_arity) )
      return too_many_args(fn,label,n,min_arity,max_arity);
    else NO_ELSE;}
  else if (fcnp)
    *fcnp = NULL;
  else NO_ELSE;

  KNO_STACK_SET_OP(stack,fn,0);
  KNO_STACK_SET_ARGS(stack,(lispvec)args,n,n,KNO_STATIC_ARGS);

  return 1;
}

static lispval profiled_dcall
(struct KNO_STACK *caller,lispval fn,
 int n,kno_argvec argvec)
{
  u8_string fname="dcall";
  if (caller == NULL) caller = kno_stackptr;

  /* Make the call */
  if (stackcheck()) {
    lispval result=VOID;
    struct KNO_FUNCTION *f = NULL;
    struct KNO_PROFILE *profile = NULL;
    struct KNO_STACK stack = { 0 };
    KNO_SETUP_STACK((&stack),NULL);
    int setup = setup_call_stack(&stack,fn,n,argvec,&f);
    if (setup < 0) return KNO_ERROR;
    else if ( (f) && (f->fcn_name) )
      fname = f->fcn_name;
    else NO_ELSE;
    if (f) profile = f->fcn_profile;
    if ( (profile) &&
	 ( (profile->prof_disabled) ||
	   ( KNO_CONS_TYPEOF(f) == kno_lambda_type ) ) )
      /* lambda calls handle their own profiling too */
      profile=NULL;
    KNO_STACK_SET_CALLER(&stack,caller);
    KNO_PUSH_STACK(&stack);

    U8_WITH_CONTOUR(fname,0) {
      if (profile == NULL)
	result = core_call(&stack,fn,f,n,argvec);
      else result = profiled_call(&stack,fn,f,n,argvec,profile);
      if (!(KNO_CHECK_PTR(result))) {
	badptr_err(result,fn);
	result = KNO_ERROR;}
      if (errno) errno_warning(stack.stack_label);}
    U8_ON_EXCEPTION {
      U8_CLEAR_CONTOUR();
      result = KNO_ERROR;}
    U8_END_EXCEPTION;

    if (KNO_PRECHOICEP(result))
      result = kno_simplify_choice(result);
    kno_pop_stack(&stack);
    return result;}
  else {
    u8_string limit=u8_mkstring("%lld",kno_stack_limit);
    lispval depth=KNO_INT2LISP(u8_stack_depth());
    lispval result = kno_err(kno_StackOverflow,fname,limit,depth);
    u8_free(limit);
    return result;}
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
    KNO_SETUP_STACK((&stack),NULL);
    int setup = setup_call_stack(&stack,fn,n,argvec,&f);
    if (setup < 0) return KNO_ERROR;
    KNO_STACK_SET_CALLER(&stack,caller);
    KNO_PUSH_STACK(&stack);

    U8_WITH_CONTOUR(fname,0) {
      result = core_call(&stack,fn,f,n,argvec);}
    U8_ON_EXCEPTION {
      U8_CLEAR_CONTOUR();
      result = KNO_ERROR;}
    U8_END_EXCEPTION;
    if (KNO_PRECHOICEP(result))
      result = kno_simplify_choice(result);
    kno_pop_stack(&stack);
    return result;}
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
  KNO_SETUP_STACK((&stack),NULL);
  int setup = setup_call_stack(&stack,fn,n,argvec,&f);
  if (setup < 0) return KNO_ERROR;
  KNO_STACK_SET_CALLER(&stack,caller);
  KNO_PUSH_STACK(&stack);

  lispval result = core_call(&stack,fn,f,n,argvec);

  if (!(KNO_CHECK_PTR(result))) {
    result = KNO_ERROR;
    badptr_err(result,fn);}

  if (KNO_PRECHOICEP(result))
    result = kno_simplify_choice(result);
  kno_pop_stack(&stack);
  return result;
}

KNO_EXPORT lispval kno_dcall(struct KNO_STACK *caller,
			     lispval fn,int n,kno_argvec argvec)
{
  if (caller == NULL) caller = kno_stackptr;
  if (caller == NULL)
    return reckless_dcall(caller,fn,n,argvec);
  else {
    int call_flags = ( (caller->stack_flags) & (KNO_STACK_CALL_MASK) );
    switch (call_flags) {
    case 0: return profiled_dcall(caller,fn,n,argvec);
    case KNO_STACK_NO_CONTOURS: return reckless_dcall(caller,fn,n,argvec);
    case KNO_STACK_NO_PROFILING: return contoured_dcall(caller,fn,n,argvec);
    default: return profiled_dcall(caller,fn,n,argvec);}}
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
      ITER_CHOICES(scan,limit,nd_args[i]);
      while (scan<limit) {
	d_args[i]=*scan++;
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
  ITER_CHOICES(scan,limit,args1);
  while (scan<limit) {
    lispval arg1 = *scan++;
    lispval r = kno_dcall(_stack,fp,1,&arg1);
    if (KNO_ABORTED(r)) {
      kno_decref(results);
      return r;}
    else {KNO_ADD_RESULT(results,r);}}
  return kno_simplify_choice(results);
}

static lispval ndapply2(kno_stack _stack,
			lispval fp,
			lispval args1,lispval args2)
{
  lispval results = EMPTY;
  ITER_CHOICES(scan1,limit1,args1);
  ITER_CHOICES(start2,limit2,args2);
  lispval args[2];
  while (scan1<limit1) {
    args[0] = *scan1++;
    const lispval *scan2 = start2; while (scan2<limit2) {
      args[1] = *scan2++;
      lispval r = kno_dcall(_stack,fp,2,args);
      if (KNO_ABORTED(r)) {
	kno_decref(results);
	return r;}
      else {KNO_ADD_RESULT(results,r);}}}
  return kno_simplify_choice(results);
}

static lispval ndapply3(kno_stack _stack,lispval fp,
			lispval args1,
			lispval args2,
			lispval args3)
{
  lispval results = EMPTY;
  ITER_CHOICES(scan1,limit1,args1);
  ITER_CHOICES(start2,limit2,args2);
  ITER_CHOICES(start3,limit3,args3);
  lispval args[3];
  while (scan1<limit1) {
    args[0] = *scan1++;
    const lispval *scan2 = start2; while (scan2<limit2) {
      args[1] = *scan2++;
      const lispval *scan3 = start3; while (scan3<limit3) {
	args[2] = *scan3++;
	lispval r = kno_dcall(_stack,fp,3,args);
	if (KNO_ABORTED(r)) {
	  kno_decref(results);
	  return r;}
	else {KNO_ADD_RESULT(results,r);}}}}
  return kno_simplify_choice(results);
}

static lispval ndapply4(kno_stack _stack,
			lispval fp,
			lispval args1,lispval args2,
			lispval args3,lispval args4)
{
  lispval results = EMPTY;
  ITER_CHOICES(scan1,limit1,args1);
  ITER_CHOICES(start2,limit2,args2);
  ITER_CHOICES(start3,limit3,args3);
  ITER_CHOICES(start4,limit4,args4);
  lispval args[4];
  while (scan1<limit1) {
    args[0] = *scan1++;
    const lispval *scan2 = start2; while (scan2<limit2) {
      args[1] = *scan2++;
      const lispval *scan3 = start3; while (scan3<limit3) {
	args[2] = *scan3++;
	const lispval *scan4 = start4; while (scan4<limit4) {
	  args[3] = *scan4++;
	  lispval r = kno_dcall(_stack,fp,4,args);
	  if (KNO_ABORTED(r)) {
	    kno_decref(results);
	    return r;}
	  else {KNO_ADD_RESULT(results,r);}}}}}
  return kno_simplify_choice(results);
}

static lispval ndcall(struct KNO_STACK *stack,lispval h,int n,kno_argvec args)
{
  kno_lisp_type fntype = KNO_TYPEOF(h);
  if (KNO_FUNCTION_TYPEP(fntype)) {
    struct KNO_FUNCTION *f = KNO_FUNCTION_INFO(h);
    int ndop = (f->fcn_call & KNO_CALL_NDCALL);
    if ( (f->fcn_arity==0) || (ndop) )
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
	  else return kno_simplify_choice(results);}}
      u8_condition ex = (n>f->fcn_arity) ? (kno_TooManyArgs) : (kno_TooFewArgs);
      return kno_err(ex,"kno_ndapply",f->fcn_name,LISP_CONS(f));}}
  else if (kno_applyfns[fntype])
    return kno_dcall(stack,h,n,args);
  else return kno_type_error("Applicable","ndcall",h);
}

KNO_EXPORT lispval kno_call(struct KNO_STACK *_stack,
			    lispval handler,int n,kno_argvec args)
{
  if (EMPTYP(handler)) return EMPTY;
  int iter_choices = 1, qchoice_args = 0, prune_calls = 1;
  int ambig_args   = ( (KNO_CHOICEP(handler)) || (KNO_PRECHOICEP(handler)) );
  kno_function f = KNO_FUNCTION_INFO(handler);
  if (f) {
    if (f->fcn_call & KNO_CALL_NDCALL)
      iter_choices = 0;
    if (f->fcn_call & KNO_CALL_XPRUNE)
      prune_calls = 0;
    NO_ELSE;}
  else if ((kno_type_call_info[KNO_PRIM_TYPE(handler)])&(KNO_CALL_NDCALL)) {
    iter_choices = 0;}
  else NO_ELSE;
  const lispval *scan = args, *limit=scan+n;
  while (scan<limit) {
    lispval arg = *scan++;
    if ( (EMPTYP(arg)) && (prune_calls) )
      return KNO_EMPTY;
    else if (CONSP(arg)) {
      if ( (ambig_args == 0) && (KNO_CHOICEP(arg)) ) ambig_args = 1;
      if ( (qchoice_args == 0) && (KNO_QCHOICEP(arg)) ) qchoice_args = 1;}
    else NO_ELSE;}
  if ( (iter_choices) && (ambig_args) ) {
    struct KNO_STACK iter_stack = { 0 };
    KNO_SETUP_STACK(&iter_stack,"fnchoices");
    int checked = setup_call_stack(&iter_stack,handler,n,args,NULL);
    if (checked < 0) return KNO_ERROR;
    KNO_STACK_SET_CALLER(&iter_stack,_stack);
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
  else if (qchoice_args) {
    lispval unwrapped[n], *write = unwrapped;
    const lispval *scan=args, *limit = scan+n;
    while (scan<limit) {
      lispval arg = *scan++;
      if (KNO_QCHOICEP(arg)) {
	lispval qval = KNO_QCHOICEVAL(arg);
	*write++=qval;}
      else *write++=arg;}
    return kno_dcall(_stack,handler,n,unwrapped);}
  else return kno_dcall(_stack,handler,n,args);
}

/* Initializations */

static u8_condition DefnFailed=_("Definition Failed");

KNO_EXPORT void kno_defn(lispval table,lispval fcn)
{
  struct KNO_FUNCTION *f = kno_consptr(struct KNO_FUNCTION *,fcn,kno_cprim_type);
  if (kno_store(table,kno_getsym(f->fcn_name),fcn)<0)
    kno_raisex(DefnFailed,"kno_defn",NULL);
  if ( (KNO_NULLP(f->fcn_moduleid)) || (KNO_VOIDP(f->fcn_moduleid)) ) {
    lispval moduleid = kno_get(table,KNOSYM_MODULEID,KNO_VOID);
    if (!(KNO_VOIDP(moduleid))) f->fcn_moduleid=moduleid;}
}

KNO_EXPORT void kno_idefn(lispval table,lispval fcn)
{
  struct KNO_FUNCTION *f = kno_consptr(struct KNO_FUNCTION *,fcn,kno_cprim_type);
  if (kno_store(table,kno_getsym(f->fcn_name),fcn)<0)
    kno_raisex(DefnFailed,"kno_defn",NULL);
  if ( (KNO_NULLP(f->fcn_moduleid)) || (KNO_VOIDP(f->fcn_moduleid)) ) {
    lispval moduleid = kno_get(table,KNOSYM_MODULEID,KNO_VOID);
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
void kno_init_exec_c(void);
void kno_init_dispatch_c(void);
void kno_init_services_c(void);

/* PROFILED functions config */

static lispval profiled_fcns = KNO_EMPTY;
static u8_mutex profiled_lock;

static int config_add_profiled(lispval var,lispval val,void *data)
{
  if (KNO_FUNCTIONP(val)) {
    struct KNO_FUNCTION *fcn = KNO_FUNCTION_INFO(val);
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
       ((typecode) <= kno_closure_type) )
    return 1;
  else return ( (kno_applyfns[typecode]) != NULL);
}
KNO_EXPORT int _KNO_APPLICABLE_TYPEP(int typecode)
{
  return APPLICABLE_TYPEP(typecode);
}
KNO_EXPORT int _KNO_APPLICABLEP(lispval x)
{
  return APPLICABLE_TYPEP(KNO_PRIM_TYPE(x));
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
  return FUNCTION_TYPEP(typecode);
}
KNO_EXPORT int _KNO_FUNCTIONP(lispval x)
{
  return FUNCTION_TYPEP(KNO_PRIM_TYPE(x));
}

KNO_EXPORT int _KNO_LAMBDAP(lispval x)
{
  return ( (KNO_TYPEP(x,kno_lambda_type)) ||
	   ( (KNO_TYPEP(x,kno_closure_type)) &&
	     (KNO_TYPEP(((kno_pair)x)->car,kno_lambda_type)) ) );
}

KNO_EXPORT kno_function _KNO_FUNCTION_INFO(lispval x)
{
  if (KNO_TYPEP(x,kno_closure_type))
    return (kno_function) ( (kno_pair) x)->car;
  else if (KNO_FUNCTIONP(x))
    return (kno_function) x;
  else return KNO_ERR(NULL,kno_NotAFunction,NULL,NULL,x);
}

KNO_EXPORT int _KNO_FUNCTION_INFOP(lispval x)
{
  if (KNO_TYPEP(x,kno_closure_type))
    return KNO_FUNCTION_INFO_TYPEP(( (kno_pair) x)->car);
  else return KNO_FUNCTION_INFO_TYPEP(x);
}

/* Support for profiling */

KNO_EXPORT struct KNO_PROFILE *kno_make_profile(u8_string name)
{
  struct KNO_PROFILE *result = u8_alloc(struct KNO_PROFILE);
  result->prof_label    = (name) ? (u8_strdup(name)) : (NULL);
  result->prof_disabled = 0;
#if HAVE_STDATOMIC_H
  result->prof_calls        = ATOMIC_VAR_INIT(0);
  result->prof_items        = ATOMIC_VAR_INIT(0);
  result->prof_nsecs        = ATOMIC_VAR_INIT(0);
#if KNO_EXTENDED_PROFILING
  result->prof_nsecs_user   = ATOMIC_VAR_INIT(0);
  result->prof_nsecs_system = ATOMIC_VAR_INIT(0);
  result->prof_n_waits      = ATOMIC_VAR_INIT(0);
  result->prof_n_pauses      = ATOMIC_VAR_INIT(0);
  result->prof_n_faults     = ATOMIC_VAR_INIT(0);
#endif
#else
  u8_init_mutex(&(result->prof_lock));
#endif
  return result;
}

KNO_EXPORT void kno_profile_update
(struct KNO_PROFILE *profile,struct rusage *before,struct timespec *start,
 int calls)
{
  if (profile->prof_disabled) return;

  long long nsecs = 0;
  long long stime = 0, utime = 0;
  long long n_waits = 0, n_pauses = 0, n_faults = 0;
#if HAVE_CLOCK_GETTIME
  struct timespec end;
  clock_gettime(CLOCK_MONOTONIC,&end);
  nsecs = ((end.tv_sec*1000000000)+(end.tv_nsec)) -
    ((start->tv_sec*1000000000)+(start->tv_nsec));
#endif
#if ( (KNO_EXTENDED_PROFILING) && (HAVE_DECL_RUSAGE_THREAD) )
  if (kno_extended_profiling) {
    struct rusage after = { 0 };
    getrusage(RUSAGE_THREAD,&after);
    utime = (after.ru_utime.tv_sec*1000000000+after.ru_utime.tv_usec*1000)-
      (before->ru_utime.tv_sec*1000000000+before->ru_utime.tv_usec*1000);
    stime = (after.ru_stime.tv_sec*1000000000+after.ru_stime.tv_usec*1000)-
      (before->ru_stime.tv_sec*1000000000+before->ru_stime.tv_usec*1000);
#if HAVE_STRUCT_RUSAGE_RU_NVCSW
    n_waits = after.ru_nvcsw - before->ru_nvcsw;
#endif
#if HAVE_STRUCT_RUSAGE_RU_MAJFLT
    n_faults = after.ru_majflt - before->ru_majflt;
#endif
#if HAVE_STRUCT_RUSAGE_RU_NIVCSW
    n_pauses = after.ru_nivcsw - before->ru_nivcsw;
#endif
  }
#endif

  kno_profile_record(profile,0,nsecs,utime,stime,
		     n_waits,n_pauses,n_faults,
		     calls);
  return;
}

#if HAVE_STDATOMIC_H
KNO_EXPORT void kno_profile_record
(struct KNO_PROFILE *p,long long items,
 long long nsecs,long long nsecs_user,long long nsecs_system,
 long long n_waits,long long n_pauses,long long n_faults,
 int calls)
{
  if (p->prof_disabled) return;
  if (items) atomic_fetch_add(&(p->prof_items),items);
  if (calls) atomic_fetch_add(&(p->prof_calls),calls);
  atomic_fetch_add(&(p->prof_nsecs),nsecs);
#if KNO_EXTENDED_PROFILING
  atomic_fetch_add(&(p->prof_nsecs_user),nsecs_user);
  atomic_fetch_add(&(p->prof_nsecs_system),nsecs_system);
  atomic_fetch_add(&(p->prof_n_waits),n_waits);
  atomic_fetch_add(&(p->prof_n_pauses),n_pauses);
  atomic_fetch_add(&(p->prof_n_faults),n_faults);
#endif
}
#else
KNO_EXPORT void kno_profile_record
(struct KNO_PROFILE *p,long long items,
 long long nsecs,long long nsecs_user,long long nsecs_system,
 long long n_waits,long long n_pauses,long long n_faults,
 int calls)
{
  if (p->prof_disabled) return;
  u8_lock_mutex(&(p->prof_lock));
  if (items) p->prof_items += items;
  p->prof_calls += calls;
  p->prof_nsecs += nsecs;
#if KNO_EXTENDED_PROFILING
  p->prof_nsecs_user += nsecs_user;
  p->prof_nsecs_system += nsecs_system;
  p->prof_n_waits += n_waits;
  p->prof_n_pauses += n_pauses;
  p->prof_n_nfaults += n_faults;
#endif
  u8_lock_mutex(&(p->prof_lock));
}
#endif

KNO_EXPORT void kno_profile_start(struct rusage *before,struct timespec *start)
{
#if HAVE_CLOCK_GETTIME
  clock_gettime(CLOCK_MONOTONIC,start);
#endif
#if ( (KNO_EXTENDED_PROFILING) && (HAVE_DECL_RUSAGE_THREAD) )
  if (kno_extended_profiling) getrusage(RUSAGE_THREAD,before);
#elif (KNO_EXTENDED_PROFILING)
  if (kno_extended_profiling) getrusage(RUSAGE_SELF,before);
#else
  return;
#endif
}

/* Tracing */

static void output_args(u8_output out,kno_function f,int n,kno_argvec args)
{
  lispval *schema = f->fcn_argnames;
  int i = 0; while (i<n) {
    lispval arg = args[i];
    int start_pos = out->u8_write-out->u8_outbuf;
    if (schema) {
      u8_printf(out,"\n  #%d %q  ",i,schema[i]);
      int cur_pos = out->u8_write-out->u8_outbuf;
      if ((cur_pos-start_pos)<20) {
	int j = 0, lim = 20-(cur_pos-start_pos);
	while (j<lim) {u8_putc(out,' '); j++;}}}
    else u8_printf(out,"\n  #%d  ",i);
    if (KNO_CONSP(arg)) {
      u8_string type_name = kno_type_name(arg);
      int size = (KNO_CHOICEP(arg)) ? (KNO_CHOICE_SIZE(arg)) :
	(KNO_SEQUENCEP(arg)) ? (kno_seq_length(arg)) : (-1);
      if (size>=0)
	u8_printf(out,"#!%p %s size=%d\n\t%q",arg,type_name,size,arg);
      else u8_printf(out,"#!%p %s\n\t%q",arg,type_name,arg);}
    else u8_printf(out,"%q",arg);
    i++;}
}

KNO_EXPORT int kno_trace_call
(kno_stack s,lispval fn,kno_function f,int n,kno_argvec args)
{
  int bits = f->fcn_trace;
  u8_byte buf[100];
  u8_string name = (KNO_TYPEP(fn,kno_closure_type)) ?
    ( (f->fcn_name) ?
      (u8_bprintf(buf,"closure:%s:%p",f->fcn_name,fn)) :
      (u8_bprintf(buf,"closure:%s:%p",kno_type_name(fn),fn)) ) :
    (f->fcn_name) ? (f->fcn_name) :
    (u8_bprintf(buf,"%s:%p",kno_type_name(fn),fn));
  if ( (bits) & (KNO_FCN_TRACE_BREAK) ) {
  breakpoint:
    u8_log(LOGNOTICE,"CallEntryBreak",
	   "%s[%d] @%p",name,n,s);}
  if ( ( (bits) & (KNO_FCN_TRACE_ENTER) ) ) {
    if ( ( (bits) & (KNO_FCN_TRACE_ARGS) ) && (n>0) && (args) ) {
      U8_STATIC_OUTPUT(argsout,300);
      output_args(&argsout,f,n,args);
      u8_log(U8_MSG_NOTICE,"CallEnter",
	     "%s[%d] @%p:%s",name,n,s,argsout.u8_outbuf);
      u8_close_output(&argsout);}
    else u8_log(U8_MSG_NOTICE,"CallEnter","%s[%d] @%p",name,n,s);}
  return 0;
}
KNO_EXPORT int kno_trace_exit
(kno_stack s,lispval r,lispval fn,kno_function f,int n,kno_argvec args)
{
  u8_byte buf[100];
  int bits = f->fcn_trace;
  u8_string name = (f->fcn_name) ? (f->fcn_name) :
    (u8_bprintf(buf,"%s:%p",kno_type_name(fn),f));
  if ( (bits) & (KNO_FCN_TRACE_BREAK) ) {
  breakpoint:
    u8_log(LOGNOTICE,"CallDoneBreak",
	   "%s[%d] @%p",name,n,s);}
  if ( ( (bits) & (KNO_FCN_TRACE_EXIT) ) ) {
    if (CONSP(r)) {
      u8_string type_name = kno_type_name(r);
      int size = (KNO_CHOICEP(r)) ? (KNO_CHOICE_SIZE(r)) :
	(KNO_SEQUENCEP(r)) ? (kno_seq_length(r)) : (-1);
      if (size<0)
	u8_log(U8_MSG_NOTICE,"CallExit",
	       "From %s[%d] @%p result %s size=%d =>\n\t %q",
	       name,n,s,type_name,size,r);
      else u8_log(U8_MSG_NOTICE,"CallExit",
		  "%s[%d] @%p result %s =>\n\t %q",
		  name,n,s,type_name,r);}
    else if (r == KNO_TAIL)
      u8_log(U8_MSG_NOTICE,"TailReturn","From %s[%d] @%p",name,n,s);
    else u8_log(U8_MSG_NOTICE,"CallExit",
		"From %s[%d] @%p, result => %q",name,n,s,r);
    if ( ( (bits) & (KNO_FCN_TRACE_ARGS) ) &&
	 (!( (bits) & (KNO_FCN_TRACE_ENTER) )) &&
	 (n>0) && (args)) {
      U8_STATIC_OUTPUT(argsout,300);
      output_args(&argsout,f,n,args);
      if (r == KNO_TAIL)
	u8_log(U8_MSG_NOTICE,"TailReturn","From %s[%d] @%p:%s",
	       name,n,s,argsout.u8_outbuf);
      else u8_log(U8_MSG_NOTICE,"Returning","From %s[%d] @%p:%s",
		  name,n,s,argsout.u8_outbuf);
      u8_close_output(&argsout);}}
  return 0;
}

/* Initializations */

KNO_EXPORT void kno_init_apply_c()
{
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
  kno_init_exec_c();
  kno_init_dispatch_c();
  kno_init_services_c();
}

