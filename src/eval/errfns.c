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
#include "kno/compounds.h"
#include "kno/support.h"
#include "kno/errobjs.h"
#include "kno/eval.h"
#include "kno/ports.h"
#include "kno/sequences.h"
#include "kno/cprims.h"


static u8_condition SchemeError=_("Undistinguished Scheme Error");

static lispval stack_entry_symbol;

/* Returning error objects when troubled */

static lispval catcherr_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval toeval = kno_get_arg(expr,1);
  lispval value = kno_stack_eval(toeval,env,_stack,0);
  if (KNO_THROWP(value))
    return value;
  else if (KNO_ABORTP(value)) {
    u8_exception ex = u8_erreify();
    lispval exception_object = kno_wrap_exception(ex);
    u8_free_exception(ex,1);
    return exception_object;}
  else return value;
}

/* Returning errors */

static lispval error_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  u8_condition ex = SchemeError, cxt = NULL;
  lispval head = kno_get_arg(expr,0);
  lispval arg1 = kno_get_arg(expr,1);
  lispval arg2 = kno_get_arg(expr,2);
  lispval printout_body;
  if ((SYMBOLP(arg1)) && (SYMBOLP(arg2))) {
    ex = (u8_condition)(SYM_NAME(arg1));
    cxt = (u8_context)(SYM_NAME(arg2));
    printout_body = kno_get_body(expr,3);}
  else if (SYMBOLP(arg1)) {
    ex = (u8_condition)(SYM_NAME(arg1));
    printout_body = kno_get_body(expr,2);}
  else if (SYMBOLP(head)) {
    ex = (u8_condition)(SYM_NAME(head));
    printout_body = kno_get_body(expr,1);}
  else printout_body = kno_get_body(expr,1);

  if (KNO_EMPTY_LISTP(printout_body))
    return kno_err(ex,cxt,NULL,VOID);
  else {
    U8_OUTPUT out; U8_INIT_OUTPUT(&out,256);
    kno_printout_to(&out,printout_body,env);
    if (out.u8_write > out.u8_outbuf)
      kno_seterr(ex,cxt,out.u8_outbuf,VOID);
    else kno_seterr(ex,cxt,NULL,VOID);
    u8_close_output(&out);
    return KNO_ERROR;}
}

DEFPRIM4("%err",error_prim,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	 "(%err *cond* [*caller*] [*details*] [*irritant*]) "
	 "returns an error object with condition *cond* (a "
	 "symbol), a *caller* (also a symbol), *details* "
	 "(usually a string), and *irritant* (a lisp "
	 "object).",
	 kno_symbol_type,KNO_VOID,kno_symbol_type,KNO_VOID,
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval error_prim(lispval condition,lispval caller,
			  lispval details_arg,
			  lispval irritant)
{
  u8_string details = NULL; int free_details=0;
  if ((KNO_VOIDP(details_arg)) || (KNO_DEFAULTP(details_arg)))
    details=NULL;
  else if (KNO_STRINGP(details_arg))
    details=KNO_CSTRING(details_arg);
  else if (KNO_SYMBOLP(details_arg))
    details=KNO_SYMBOL_NAME(details_arg);
  else {
    details=kno_lisp2string(details_arg);
    free_details=1;}
  kno_seterr(KNO_SYMBOL_NAME(condition),
	     ((KNO_VOIDP(caller)) ? (NULL) : (KNO_SYMBOL_NAME(caller))),
	     details,irritant);
  if (free_details) u8_free(details);
  return KNO_ERROR;
}

static lispval irritant_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  u8_condition ex = SchemeError, cxt = NULL;
  lispval head = kno_get_arg(expr,0);
  lispval irritant_expr = kno_get_arg(expr,1), irritant=VOID;
  lispval arg1 = kno_get_arg(expr,2);
  lispval arg2 = kno_get_arg(expr,3);
  lispval printout_body;

  if ((SYMBOLP(arg1)) && (SYMBOLP(arg2))) {
    ex = (u8_condition)(SYM_NAME(arg1));
    cxt = (u8_context)(SYM_NAME(arg2));
    printout_body = kno_get_body(expr,4);}
  else if (SYMBOLP(arg1)) {
    ex = (u8_condition)(SYM_NAME(arg1));
    printout_body = kno_get_body(expr,3);}
  else if (STRINGP(arg1)) {
    u8_string pname = CSTRING(arg1);
    lispval sym = kno_intern(pname);
    ex = (u8_condition)(SYM_NAME(sym));
    printout_body = kno_get_body(expr,3);}
  else if (SYMBOLP(head)) {
    ex = (u8_condition)"Bad irritant arg";
    printout_body = kno_get_body(expr,2);}
  else printout_body = kno_get_body(expr,2);

  if (VOIDP(irritant))
    irritant = kno_eval(irritant_expr,env);

  if (KNO_ABORTP(irritant)) {
    u8_exception ex = u8_erreify();
    if (ex)
      u8_log(LOG_CRIT,"RecursiveError",
	     "Error %s (%s:%s) evaluating irritant arg %q",
	     ex->u8x_cond,ex->u8x_context,ex->u8x_details,
	     irritant_expr);
    else u8_log(LOG_CRIT,"RecursiveError",
		"Error evaluating irritant arg %q",
		irritant_expr);
    u8_free_exception(ex,0);
    irritant=KNO_VOID;}

  if (cxt == NULL) {
    kno_stack cur = kno_stackptr;
    while ( (cur) && (cxt==NULL) ) {
      lispval op = cur->stack_op;
      if (SYMBOLP(op))
	cxt=KNO_SYMBOL_NAME(op);
      else if (KNO_FUNCTIONP(op)) {
	struct KNO_FUNCTION *fcn = KNO_GETFUNCTION(op);
	if (fcn->fcn_name)
	  cxt=fcn->fcn_name;
	else cxt="FUNCTIONCALL";}
      else {}
      cur=cur->stack_caller;}}

  u8_string details=NULL;
  U8_OUTPUT out; U8_INIT_OUTPUT(&out,256);
  kno_printout_to(&out,printout_body,env);
  if (out.u8_outbuf == out.u8_write) {
    u8_close_output(&out);
    details=NULL;}
  else details=out.u8_outbuf;

  lispval err_result=kno_err(ex,cxt,details,irritant);
  kno_decref(irritant);
  if (details) u8_close_output(&out);
  return err_result;
}

static lispval onerror_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval toeval = kno_get_arg(expr,1);
  lispval error_handler = kno_get_arg(expr,2);
  lispval default_handler = kno_get_arg(expr,3);
  lispval value = kno_stack_eval(toeval,env,_stack,0);
  if (KNO_THROWP(value))
    return value;
  else if (KNO_BREAKP(value))
    return value;
  else if (KNO_ABORTP(value)) {
    u8_exception ex = u8_erreify();
    lispval handler = kno_stack_eval(error_handler,env,_stack,0);
    if (KNO_ABORTP(handler)) {
      u8_restore_exception(ex);
      return handler;}
    else if (KNO_APPLICABLEP(handler)) {
      lispval err_value = kno_wrap_exception(ex);
      lispval handler_result = kno_apply(handler,1,&err_value);
      if (KNO_ABORTP(handler_result)) {
	u8_exception handler_ex = u8_erreify();
	/* Clear this field so we can decref err_value while leaving
	   the exception object current. */
	if (handler_ex)
	  u8_log(LOG_WARN,"Recursive error",
		 "Error %m handling error during %q",
		 handler_ex->u8x_cond,toeval);
	else u8_log(LOG_WARN,"Obscure Recursive error",
		    "Obscure error caught in %q",toeval);
	kno_log_backtrace(handler_ex,LOG_WARN,"RecursiveError",128);
	u8_restore_exception(ex);
	u8_restore_exception(handler_ex);
	kno_decref(value);
	kno_decref(handler);
	kno_decref(err_value);
	return handler_result;}
      else {
	kno_decref(value);
	kno_decref(handler);
	kno_decref(err_value);
	kno_clear_errors(1);
	u8_free_exception(ex,1);
	return handler_result;}}
    else {
      u8_free_exception(ex,1);
      kno_decref(value);
      return handler;}}
  else if (VOIDP(default_handler))
    return value;
  else {
    lispval handler = kno_stack_eval(default_handler,env,_stack,0);
    if (KNO_ABORTP(handler))
      return handler;
    else if (KNO_APPLICABLEP(handler)) {
      lispval result;
      if (VOIDP(value))
	result = kno_dapply(handler,0,&value);
      else result = kno_dapply(handler,1,&value);
      kno_decref(value);
      kno_decref(handler);
      return result;}
    else {
      kno_decref(value);
      return handler;}}
}

/* Report/clear errors */

static lispval report_errors_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval toeval = kno_get_arg(expr,1);
  lispval value = kno_stack_eval(toeval,env,_stack,0);
  if (KNO_THROWP(value))
    return value;
  else if (KNO_ABORTP(value)) {
    u8_exception ex = u8_current_exception;
    kno_log_backtrace(ex,LOG_NOTICE,"CaughtError",128);
    kno_clear_errors(0);
    return KNO_FALSE;}
  else return value;
}

static lispval ignore_errors_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval toeval = kno_get_arg(expr,1);
  lispval dflt = kno_get_arg(expr,2);
  lispval value = kno_stack_eval(toeval,env,_stack,0);
  if (KNO_THROWP(value))
    return value;
  else if (KNO_ABORTP(value)) {
    kno_clear_errors(0);
    if (KNO_VOIDP(dflt))
      return KNO_FALSE;
    else {
      lispval to_return = kno_stack_eval(dflt,env,_stack,0);
      if (KNO_ABORTP(to_return)) {
	kno_clear_errors(0);
	return KNO_FALSE;}
      else return to_return;}
    return KNO_FALSE;}
  else return value;
}

DEFPRIM("clear-errors!",clear_errors,KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
	"`(CLEAR-ERRORS!)` **undocumented**");
static lispval clear_errors()
{
  int n_errs = kno_clear_errors(1);
  if (n_errs) return KNO_INT(n_errs);
  else return KNO_FALSE;
}

/* Primitives on exception objects */

DEFPRIM1("exception-condition",exception_condition,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns the condition name (a symbol) for an "
	 "exception",
	 kno_exception_type,KNO_VOID);
static lispval exception_condition(lispval x)
{
  struct KNO_EXCEPTION *xo=
    kno_consptr(struct KNO_EXCEPTION *,x,kno_exception_type);
  if (xo->ex_condition)
    return kno_intern(xo->ex_condition);
  else return KNO_FALSE;
}

DEFPRIM1("exception-caller",exception_caller,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns the immediate context (caller) for an "
	 "exception",
	 kno_exception_type,KNO_VOID);
static lispval exception_caller(lispval x)
{
  struct KNO_EXCEPTION *xo=
    kno_consptr(struct KNO_EXCEPTION *,x,kno_exception_type);
  if (xo->ex_caller)
    return kno_intern(xo->ex_caller);
  else return KNO_FALSE;
}

DEFPRIM1("exception-details",exception_details,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns any descriptive details (a string) for "
	 "the exception",
	 kno_exception_type,KNO_VOID);
static lispval exception_details(lispval x)
{
  struct KNO_EXCEPTION *xo=
    kno_consptr(struct KNO_EXCEPTION *,x,kno_exception_type);
  if (xo->ex_details)
    return kno_mkstring(xo->ex_details);
  else return KNO_FALSE;
}

DEFPRIM1("exception-irritant",exception_irritant,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns the LISP object (if any) which 'caused' "
	 "the exception",
	 kno_exception_type,KNO_VOID);
static lispval exception_irritant(lispval x)
{
  struct KNO_EXCEPTION *xo=
    kno_consptr(struct KNO_EXCEPTION *,x,kno_exception_type);
  if (KNO_VOIDP(xo->ex_irritant))
    return KNO_FALSE;
  else return kno_incref(xo->ex_irritant);
}

DEFPRIM1("exception-context",exception_context,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns aspects of where the error occured",
	 kno_exception_type,KNO_VOID);
static lispval exception_context(lispval x)
{
  struct KNO_EXCEPTION *xo=
    kno_consptr(struct KNO_EXCEPTION *,x,kno_exception_type);
  if (KNO_VOIDP(xo->ex_context))
    return KNO_FALSE;
  else return kno_incref(xo->ex_context);
}

DEFPRIM1("exception-irritant?",exception_has_irritant,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "(EXCEPTION-IRRITANT? *ex*) "
	 "Returns true if *ex* is an exception and has an "
	 "'irritant'",
	 kno_exception_type,KNO_VOID);
static lispval exception_has_irritant(lispval x)
{
  struct KNO_EXCEPTION *xo=
    kno_consptr(struct KNO_EXCEPTION *,x,kno_exception_type);
  if (KNO_VOIDP(xo->ex_irritant))
    return KNO_FALSE;
  else return KNO_TRUE;
}

DEFPRIM1("exception-moment",exception_moment,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns the time an exception was initially "
	 "recorded",
	 kno_exception_type,KNO_VOID);
static lispval exception_moment(lispval x)
{
  struct KNO_EXCEPTION *xo=
    kno_consptr(struct KNO_EXCEPTION *,x,kno_exception_type);
  if (xo->ex_moment >= 0)
    return kno_make_flonum(xo->ex_moment);
  else return KNO_FALSE;
}

DEFPRIM1("exception-threadno",exception_threadno,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns an integer ID for the thread where the "
	 "error occurred",
	 kno_exception_type,KNO_VOID);
static lispval exception_threadno(lispval x)
{
  struct KNO_EXCEPTION *xo=
    kno_consptr(struct KNO_EXCEPTION *,x,kno_exception_type);
  if (xo->ex_thread >= 0)
    return KNO_INT(xo->ex_thread);
  else return KNO_FALSE;
}

DEFPRIM1("exception-timebase",exception_timebase,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns the time from which exception moments are "
	 "measured",
	 kno_exception_type,KNO_VOID);
static lispval exception_timebase(lispval x)
{
  struct KNO_EXCEPTION *xo=
    kno_consptr(struct KNO_EXCEPTION *,x,kno_exception_type);
  if (xo->ex_timebase >= 0)
    return kno_time2timestamp(xo->ex_timebase);
  else return KNO_FALSE;
}

DEFPRIM1("exception-sessionid",exception_sessionid,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns the sessionid when the error occurred",
	 kno_exception_type,KNO_VOID);
static lispval exception_sessionid(lispval x)
{
  struct KNO_EXCEPTION *xo=
    kno_consptr(struct KNO_EXCEPTION *,x,kno_exception_type);
  if (xo->ex_session >= 0)
    return kno_mkstring(xo->ex_session);
  else return KNO_FALSE;
}

DEFPRIM3("exception/context!",exception_add_context,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	 "Creates an exception object",
	 kno_exception_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_VOID);
static lispval exception_add_context(lispval x,lispval slotid,lispval value)
{
  struct KNO_EXCEPTION *xo=
    kno_consptr(struct KNO_EXCEPTION *,x,kno_exception_type);
  lispval context = xo->ex_context;
  lispval slotmap = KNO_VOID;
  if ( (KNO_NULLP(context)) || (KNO_VOIDP(context)) || (KNO_FALSEP(context)) )
    context = KNO_EMPTY;
  if (KNO_VOIDP(value)) {}
  else if (KNO_SLOTMAPP(context))
    slotmap = context;
  else if (KNO_CHOICEP(context)) {
    KNO_DO_CHOICES(cxt,context) {
      if (KNO_SLOTMAPP(context))  {
	slotmap = cxt;
	KNO_STOP_DO_CHOICES;}}}
  else NO_ELSE;
  if (KNO_VOIDP(value)) {
    kno_incref(slotid);
    KNO_ADD_TO_CHOICE(context,slotid);}
  else if (!(KNO_SLOTMAPP(slotmap))) {
    slotmap = kno_make_slotmap(4,0,NULL);
    KNO_ADD_TO_CHOICE(context,slotmap);}
  else NO_ELSE;
  if (KNO_SLOTMAPP(slotmap))
    kno_add(slotmap,slotid,value);
  xo->ex_context = context;
  return KNO_VOID;
}

static lispval condition_symbol, caller_symbol, timebase_symbol, moment_symbol,
  thread_symbol, details_symbol, session_symbol, context_symbol,
  irritant_symbol, stack_symbol, env_tag, args_tag;


DEFPRIM2("exception->slotmap",exception2slotmap,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "(EXCEPTION->SLOTMAP *ex*) "
	 "Breaks out the elements of the exception *ex* "
	 "into a slotmap",
	 kno_exception_type,KNO_VOID,kno_any_type,KNO_FALSE);
static lispval exception2slotmap(lispval x,lispval with_stack_arg)
{
  struct KNO_EXCEPTION *xo=
    kno_consptr(struct KNO_EXCEPTION *,x,kno_exception_type);
  lispval result = kno_make_slotmap(7,0,NULL);
  if (xo->ex_condition)
    kno_store(result,condition_symbol,kno_intern(xo->ex_condition));
  if (xo->ex_caller)
    kno_store(result,caller_symbol,kno_intern(xo->ex_caller));
  if (xo->ex_timebase > 0) {
    lispval timebase = kno_time2timestamp(xo->ex_timebase);
    kno_store(result,timebase_symbol,timebase);
    kno_decref(timebase);}
  if (xo->ex_moment > 0) {
    lispval moment = kno_make_flonum(xo->ex_moment);
    kno_store(result,moment_symbol,moment);
    kno_decref(moment);}
  if (xo->ex_thread > 0) {
    lispval threadnum = KNO_INT(xo->ex_thread);
    kno_store(result,thread_symbol,threadnum);
    kno_decref(threadnum);}
  if (xo->ex_details) {
    lispval details_string = kno_mkstring(xo->ex_details);
    kno_store(result,details_symbol,details_string);
    kno_decref(details_string);}
  if (xo->ex_session) {
    lispval details_string = kno_mkstring(xo->ex_session);
    kno_store(result,session_symbol,details_string);
    kno_decref(details_string);}
  if (!(KNO_VOIDP(xo->ex_context)))
    kno_store(result,context_symbol,xo->ex_context);
  if (!(KNO_VOIDP(xo->ex_irritant)))
    kno_store(result,irritant_symbol,xo->ex_irritant);
  if (KNO_VECTORP(xo->ex_stack)) {
    lispval vec = xo->ex_stack;
    int len = KNO_VECTOR_LENGTH(vec);
    lispval new_stack = kno_init_vector(NULL,len,0);
    int i = 0; while (i < len) {
      lispval entry = KNO_VECTOR_REF(vec,i);
      if (KNO_EXCEPTIONP(entry)) {
	KNO_VECTOR_SET(new_stack,i,exception2slotmap(entry,KNO_FALSE));}
      if ( (KNO_COMPOUND_TYPEP(entry,stack_entry_symbol)) &&
	   ( (KNO_COMPOUND_LENGTH(entry)) >= 5) ) {
	lispval copied = kno_copier(entry,0);
	int len = KNO_COMPOUND_LENGTH(entry);
	if (len >= 7) {
	  lispval argvec = KNO_COMPOUND_REF(entry,6);
	  if (KNO_VECTORP(argvec)) {
	    lispval wrapped = kno_init_compound_from_elts
	      (NULL,args_tag,KNO_COMPOUND_INCREF,
	       KNO_VECTOR_LENGTH(argvec),
	       KNO_VECTOR_ELTS(argvec));
	    KNO_COMPOUND_REF(entry,6) = wrapped;
	    kno_decref(argvec);}}
	if (len >= 9) {
	  lispval env = KNO_COMPOUND_REF(entry,8);
	  if (KNO_TABLEP(env)) {
	    lispval wrapped = kno_init_compound
	      (NULL,env_tag,KNO_COMPOUND_INCREF,1,env);
	    KNO_COMPOUND_REF(entry,8) = wrapped;
	    kno_decref(env);}}
	KNO_VECTOR_SET(new_stack,i,copied);}
      else {
	kno_incref(entry);
	KNO_VECTOR_SET(new_stack,i,entry);}
      i++;}
    kno_store(result,stack_symbol,new_stack);
    kno_decref(new_stack);}
  return result;
}

DEFPRIM3("exception-stack",exception_stack,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
	 "(EXCEPTION-STACK *ex* [*len*] [*start*]) "
	 "returns the call stack (or a slice of it) where "
	 "exception object *ex* occurred",
	 kno_exception_type,KNO_VOID,kno_fixnum_type,KNO_VOID,
	 kno_any_type,KNO_VOID);
static lispval exception_stack(lispval x,lispval arg1,lispval arg2)
{
  struct KNO_EXCEPTION *xo=
    kno_consptr(struct KNO_EXCEPTION *,x,kno_exception_type);
  if (!(KNO_VECTORP(xo->ex_stack)))
    return kno_empty_vector(0);
  else {
    struct KNO_VECTOR *stack = (kno_vector) (xo->ex_stack);
    size_t stack_len   = stack->vec_length;
    if (KNO_VOIDP(arg1))
      return kno_incref(xo->ex_stack);
    else if ( (KNO_VOIDP(arg2)) && ((KNO_FIX2INT(arg1)) > 0) ) {
      long long off = KNO_FIX2INT(arg1);
      if (off < 0) off = stack_len+off;
      if (off < 0) off = 0;
      else if (off >= stack_len) off = stack_len-1;
      lispval entry = KNO_VECTOR_REF(xo->ex_stack,off);
      return kno_incref(entry);}
    else {
      long long start, end;
      if (KNO_VOIDP(arg2)) {
	end = -(KNO_FIX2INT(arg1));
	start = 0;}
      else {
	start = KNO_FIX2INT(arg1);
	end = KNO_FIX2INT(arg2);}
      if (start<0) start = stack_len-start;
      if (end<0) end = stack_len+end;
      if (start < 0) start = 0;
      else if (start >= stack_len)
	start = stack_len;
      else NO_ELSE;
      if (end < 0) end = 0;
      else if (end >= stack_len)
	end = stack_len;
      else NO_ELSE;
      if (start == end) {
	lispval entry = KNO_VECTOR_REF(xo->ex_stack,start);
	return kno_incref(entry);}
      else if (end > start)
	return kno_slice(xo->ex_stack,start,end);
      else {
	lispval slice = kno_slice(xo->ex_stack,end,start);
	lispval reversed = kno_reverse(slice);
	kno_decref(slice);
	return reversed;}}}
}

DEFPRIM2("exception-summary",exception_summary,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "Returns a shortened backtrace for an exception",
	 kno_exception_type,KNO_VOID,kno_any_type,KNO_FALSE);
static lispval exception_summary(lispval x,lispval with_irritant)
{
  struct KNO_EXCEPTION *xo=
    kno_consptr(struct KNO_EXCEPTION *,x,kno_exception_type);
  u8_condition cond = xo->ex_condition;
  u8_context caller = xo->ex_caller;
  u8_string details = xo->ex_details;
  lispval irritant = xo->ex_irritant;
  int irritated=
    (!((VOIDP(with_irritant))||
       (FALSEP(with_irritant))||
       (VOIDP(irritant))));
  u8_string summary;
  lispval return_value;
  if ((cond)&&(caller)&&(details)&&(irritated))
    summary = u8_mkstring("#@%%! %s [%s] (%s): %q",
			  cond,caller,details,irritant);
  else if ((cond)&&(caller)&&(details))
    summary = u8_mkstring("#@%%! %s [%s] (%s)",cond,caller,details);
  else if ((cond)&&(caller)&&(irritated))
    summary = u8_mkstring("#@%%! %s [%s]: %q",cond,caller,irritant);
  else if ((cond)&&(details)&&(irritated))
    summary = u8_mkstring("#@%%! %s (%s): %q",cond,details,irritant);
  else if ((cond)&&(caller))
    summary = u8_mkstring("#@%%! %s [%s]",cond,caller);
  else if ((cond)&&(details))
    summary = u8_mkstring("#@%%! %s (%s)",cond,details);
  else if ((cond)&&(irritated))
    summary = u8_mkstring("#@%%! %s: %q",cond,irritant);
  else if (cond)
    summary = u8_mkstring("#@%%! %s",cond);
  else summary = u8_mkstring("#@%%! %s","Weird anonymous error");
  return_value = kno_mkstring(summary);
  u8_free(summary);
  return return_value;
}

static int thunkp(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  if (PRED_TRUE(KNO_FUNCTIONP(x))) {
    struct KNO_FUNCTION *f = KNO_GETFUNCTION(x);
    if (f->fcn_arity==0)
      return 1;
    else if ((f->fcn_arity<0) && (f->fcn_min_arity==0))
      return 1;
    else return 0;}
  else if (kno_applyfns[KNO_TYPEOF(x)])
    return 1;
  else return 0;
}
static lispval dynamic_wind_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval wind = kno_get_arg(expr,1);
  lispval doit = kno_get_arg(expr,2);
  lispval unwind = kno_get_arg(expr,3);
  if ((VOIDP(wind)) || (VOIDP(doit)) || (VOIDP(unwind)))
    return kno_err(kno_SyntaxError,"dynamic_wind_evalfn",NULL,expr);
  else {
    wind = kno_stack_eval(wind,env,_stack,0);
    if (KNO_ABORTP(wind))
      return wind;
    else if (!(thunkp(wind))) {
      lispval err=kno_type_error("thunk","dynamic_wind_evalfn",wind);
      kno_decref(wind);
      return err;}
    else doit = kno_stack_eval(doit,env,_stack,0);
    if (KNO_ABORTP(doit)) {
      kno_decref(wind);
      return doit;}
    else if (!(thunkp(doit))) {
      lispval err=kno_type_error("thunk","dynamic_wind_evalfn",doit);
      kno_decref(wind); kno_decref(doit);
      return err;}
    else unwind = kno_stack_eval(unwind,env,_stack,0);
    if (KNO_ABORTP(unwind)) {
      kno_decref(wind);
      kno_decref(doit);
      return unwind;}
    else if (!(thunkp(unwind))) {
      lispval err= kno_type_error("thunk","dynamic_wind_evalfn",unwind);
      kno_decref(wind); kno_decref(doit); kno_decref(unwind);
      return err;}
    else {
      lispval retval = kno_apply(wind,0,NULL);
      if (KNO_ABORTP(retval)) {}
      else {
	kno_decref(retval);
	retval = kno_apply(doit,0,NULL);
	if (KNO_ABORTP(retval)) {
	  u8_exception saved = u8_erreify();
	  lispval unwindval = kno_apply(unwind,0,NULL);
	  u8_restore_exception(saved);
	  if (KNO_ABORTP(unwindval)) {
	    kno_decref(retval);
	    retval = unwindval;}
	  else kno_decref(unwindval);}
	else {
	  lispval unwindval = kno_apply(unwind,0,NULL);
	  if (KNO_ABORTP(unwindval)) {
	    kno_decref(retval);
	    retval = unwindval;}
	  else kno_decref(unwindval);}}
      kno_decref(wind); kno_decref(doit); kno_decref(unwind);
      return retval;}}
}

static lispval unwind_protect_evalfn(lispval uwp,kno_lexenv env,kno_stack _stack)
{
  lispval heart = kno_get_arg(uwp,1);
  lispval result;
  {U8_WITH_CONTOUR("UNWIND-PROTECT(body)",0)
      result = kno_stack_eval(heart,env,_stack,0);
    U8_ON_EXCEPTION {
      U8_CLEAR_CONTOUR();
      result = KNO_ERROR;}
    U8_END_EXCEPTION;}
  {U8_WITH_CONTOUR("UNWIND-PROTECT(unwind)",0)
      {lispval unwinds = kno_get_body(uwp,2);
	KNO_DOLIST(expr,unwinds) {
	  lispval uw_result = kno_stack_eval(expr,env,_stack,0);
	  if (KNO_ABORTP(uw_result))
	    if (KNO_ABORTP(result)) {
	      kno_interr(result); kno_interr(uw_result);
	      return u8_return(KNO_ERROR);}
	    else {
	      kno_decref(result);
	      result = uw_result;
	      break;}
	  else kno_decref(uw_result);}}
    U8_ON_EXCEPTION {
      U8_CLEAR_CONTOUR();
      result = KNO_ERROR;}
    U8_END_EXCEPTION;}
  return result;
}

/* Testing raise() which will be invoked by */

DEFPRIM1("test-u8raise",test_u8raise_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Has the kno_consptr macro signal a type error with u8_raise.",
	 kno_any_type,KNO_VOID);
static lispval test_u8raise_prim(lispval obj)
{
  struct KNO_STRING *string =
    kno_consptr(struct KNO_STRING *,obj,kno_string_type);
  int len = string->str_bytelen;
  return KNO_INT(len);
}

/* Reraising exceptions */

DEFPRIM1("reraise",reraise_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Reraises the exception represented by an object",
	 kno_exception_type,KNO_VOID);
static lispval reraise_prim(lispval exo)
{
  struct KNO_EXCEPTION *xo=
    kno_consptr(struct KNO_EXCEPTION *,exo,kno_exception_type);
  kno_restore_exception(xo);
  return KNO_ERROR_VALUE;
}

/* Operations on stack objects */

DEFPRIM1("stack-depth",stack_entry_depth,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns the depth of a stack entry",
	 kno_compound_type,KNO_VOID);
static lispval stack_entry_depth(lispval stackobj)
{
  return kno_compound_ref(stackobj,stack_entry_symbol,0,KNO_FALSE);
}

DEFPRIM1("stack-type",stack_entry_type,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns the type of a stack entry",
	 kno_compound_type,KNO_VOID);
static lispval stack_entry_type(lispval stackobj)
{
  return kno_compound_ref(stackobj,stack_entry_symbol,1,KNO_FALSE);
}

DEFPRIM1("stack-label",stack_entry_label,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns the label of a stack entry",
	 kno_compound_type,KNO_VOID);
static lispval stack_entry_label(lispval stackobj)
{
  return kno_compound_ref(stackobj,stack_entry_symbol,2,KNO_FALSE);
}

DEFPRIM1("stack-op",stack_entry_op,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns the op of a stack entry",
	 kno_compound_type,KNO_VOID);
static lispval stack_entry_op(lispval stackobj)
{
  return kno_compound_ref(stackobj,stack_entry_symbol,3,KNO_FALSE);
}

DEFPRIM1("stack-filename",stack_entry_filename,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns the label of a stack entry",
	 kno_compound_type,KNO_VOID);
static lispval stack_entry_filename(lispval stackobj)
{
  return kno_compound_ref(stackobj,stack_entry_symbol,4,KNO_FALSE);
}

DEFPRIM1("stack-crumb",stack_entry_crumb,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns a probably unique integer identifier for "
	 "the stack frame",
	 kno_compound_type,KNO_VOID);
static lispval stack_entry_crumb(lispval stackobj)
{
  return kno_compound_ref(stackobj,stack_entry_symbol,5,KNO_FALSE);
}

DEFPRIM1("stack-args",stack_entry_args,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns the args of a stack entry",
	 kno_compound_type,KNO_VOID);
static lispval stack_entry_args(lispval stackobj)
{
  return kno_compound_ref(stackobj,stack_entry_symbol,6,KNO_FALSE);
}

DEFPRIM1("stack-source",stack_entry_source,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns the source of a stack entry",
	 kno_compound_type,KNO_VOID);
static lispval stack_entry_source(lispval stackobj)
{
  return kno_compound_ref(stackobj,stack_entry_symbol,7,KNO_FALSE);
}

DEFPRIM1("stack-env",stack_entry_env,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns the env of a stack entry",
	 kno_compound_type,KNO_VOID);
static lispval stack_entry_env(lispval stackobj)
{
  return kno_compound_ref(stackobj,stack_entry_symbol,8,KNO_FALSE);
}

DEFPRIM1("stack-status",stack_entry_status,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns the status of a stack entry",
	 kno_compound_type,KNO_VOID);
static lispval stack_entry_status(lispval stackobj)
{
  return kno_compound_ref(stackobj,stack_entry_symbol,9,KNO_FALSE);
}

static u8_string static_string(lispval x,int err)
{
  if (KNO_SYMBOLP(x))
    return KNO_SYMBOL_NAME(x);
  else if (KNO_STRINGP(x)) {
    u8_string s = KNO_CSTRING(x);
    lispval sym = kno_intern(s);
    return KNO_SYMBOL_NAME(sym);}
  else if (err)
    return "BadExceptionArgument";
  else return NULL;
}

DEFPRIM10("make-exception",make_exception,KNO_MAX_ARGS(10)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "Creates an exception object",
	  kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	  kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	  kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	  kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	  kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval make_exception(lispval cond,lispval caller,lispval details,
			      lispval threadid,lispval sessionid,
			      lispval moment,lispval timebase,
			      lispval stack,lispval context,
			      lispval irritant)
{
  double secs = (KNO_FLONUMP(moment)) ? (KNO_FLONUM(moment)) : (-1);
  struct KNO_TIMESTAMP *tstamp = (KNO_TYPEP(timebase,kno_timestamp_type)) ?
    ((kno_timestamp) timebase) : (NULL);
  time_t tick = (KNO_FIXNUMP(timebase)) ? (KNO_FIX2INT(timebase)) :
    (tstamp) ? (tstamp->u8xtimeval.u8_tick) : (-1);
  return kno_init_exception
    (NULL,(u8_condition) static_string(cond,1),
     (u8_condition) static_string(caller,0),
     (KNO_STRINGP(details)) ? (u8_strdup(KNO_CSTRING(details))) : (NULL),
     (KNO_DEFAULTP(irritant)) ? (KNO_VOID) : (kno_incref(irritant)),
     kno_incref(stack),kno_incref(context),
     (KNO_STRINGP(sessionid)) ? (u8_strdup(KNO_CSTRING(sessionid))) : (NULL),
     secs,tick,
     (KNO_FIXNUMP(threadid)) ? (KNO_FIX2INT(threadid)) : (-1));
}

/* Clear errors */

KNO_EXPORT void kno_init_errfns_c()
{
  u8_register_source_file(_FILEINFO);

  link_local_cprims();

  kno_def_evalfn(kno_scheme_module,"ERROR",error_evalfn,
		 "(error *condition* *caller* [*irritant*] details...) "
		 "signals an error\n"
		 "*condition* and *caller* should be symbols and are not "
		 "evaluated. *irritant* is a lisp object (evaluated) and "
		 "a details message is generated by PRINTOUT ..details");
  kno_def_evalfn(kno_scheme_module,"IRRITANT",irritant_evalfn,
		 "(irritant *irritant* *condition* *caller* details...) "
		 "signals an error with the designated *irritant* \n"
		 "*condition* and *caller* should be symbols and are not "
		 "evaluated. A details message is generated by "
		 "PRINTOUT ..details");

  kno_def_evalfn(kno_scheme_module,"ONERROR",onerror_evalfn,
		 "(ONERROR *expr* *onerr* [*normally*])\n"
		 "Evaluates *expr*, catching errors during the evaluation. "
		 "If there are errors, either apply *onerr* to an exception "
		 "object or simply return *onerr* if it's not applicable. "
		 "If there are no errors and *normal* is not specified, "
		 "return the result. If *normal* is specified, apply it to "
		 "the result and return it's result.");
  kno_def_evalfn(kno_scheme_module,"CATCHERR",catcherr_evalfn,
		 "(CATCHERR *expr*) returns the result of *expr* "
		 "or an exception object describing any signalled errors");
  kno_def_evalfn(kno_scheme_module,"ERREIFY",catcherr_evalfn,
		 "(ERREIFY *expr*) returns the result of *expr* "
		 "or an exception object describing any signalled errors");

  kno_def_evalfn(kno_scheme_module,"REPORT-ERRORS",report_errors_evalfn,
		 "(REPORT-ERRORS *expr* [*errval*]) evaluates *expr*, "
		 "reporting and clearing any errors.\n"
		 "Returns *errval* or #f if any errors occur.");
  kno_def_evalfn(kno_scheme_module,"IGNORE-ERRORS",ignore_errors_evalfn,
		 "(REPORT-ERRORS *expr* [*errval*]) evaluates *expr*, "
		 "reporting and clearing any errors.\n"
		 "Returns *errval* or #f if any errors occur.");
  kno_def_evalfn(kno_scheme_module,"DYNAMIC-WIND",dynamic_wind_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"UNWIND-PROTECT",unwind_protect_evalfn,
		 "*undocumented*");
  /* Deprecated, from old error system */
  kno_def_evalfn(kno_scheme_module,"NEWERR",irritant_evalfn,
		 "*undocumented*");

  stack_entry_symbol = kno_intern("_stack");
  condition_symbol = kno_intern("condition");
  caller_symbol = kno_intern("caller");
  timebase_symbol = kno_intern("timebase");
  moment_symbol = kno_intern("moment");
  thread_symbol = kno_intern("thread");
  details_symbol = kno_intern("details");
  session_symbol = kno_intern("session");
  context_symbol = kno_intern("context");
  irritant_symbol = kno_intern("irritant");
  stack_symbol = kno_intern("stack");
  env_tag = kno_intern("%env");
  args_tag = kno_intern("%args");

}

static void link_local_cprims()
{
  lispval scheme_module = kno_scheme_module;

  KNO_LINK_PRIM("make-exception",make_exception,10,scheme_module);
  KNO_LINK_PRIM("stack-status",stack_entry_status,1,scheme_module);
  KNO_LINK_PRIM("stack-env",stack_entry_env,1,scheme_module);
  KNO_LINK_PRIM("stack-source",stack_entry_source,1,scheme_module);
  KNO_LINK_PRIM("stack-args",stack_entry_args,1,scheme_module);
  KNO_LINK_PRIM("stack-crumb",stack_entry_crumb,1,scheme_module);
  KNO_LINK_PRIM("stack-filename",stack_entry_filename,1,scheme_module);
  KNO_LINK_PRIM("stack-op",stack_entry_op,1,scheme_module);
  KNO_LINK_PRIM("stack-label",stack_entry_label,1,scheme_module);
  KNO_LINK_PRIM("stack-type",stack_entry_type,1,scheme_module);
  KNO_LINK_PRIM("stack-depth",stack_entry_depth,1,scheme_module);
  KNO_LINK_PRIM("reraise",reraise_prim,1,scheme_module);
  KNO_LINK_PRIM("test-u8raise",test_u8raise_prim,1,scheme_module);
  KNO_LINK_PRIM("exception-summary",exception_summary,2,scheme_module);
  KNO_LINK_PRIM("exception-stack",exception_stack,3,scheme_module);
  KNO_LINK_PRIM("exception->slotmap",exception2slotmap,2,scheme_module);
  KNO_LINK_PRIM("exception/context!",exception_add_context,3,scheme_module);
  KNO_LINK_PRIM("exception-sessionid",exception_sessionid,1,scheme_module);
  KNO_LINK_PRIM("exception-timebase",exception_timebase,1,scheme_module);
  KNO_LINK_PRIM("exception-threadno",exception_threadno,1,scheme_module);
  KNO_LINK_PRIM("exception-moment",exception_moment,1,scheme_module);
  KNO_LINK_PRIM("exception-irritant?",exception_has_irritant,1,scheme_module);
  KNO_LINK_PRIM("exception-context",exception_context,1,scheme_module);
  KNO_LINK_PRIM("exception-irritant",exception_irritant,1,scheme_module);
  KNO_LINK_PRIM("exception-details",exception_details,1,scheme_module);
  KNO_LINK_PRIM("exception-caller",exception_caller,1,scheme_module);
  KNO_LINK_PRIM("exception-condition",exception_condition,1,scheme_module);
  KNO_LINK_PRIM("clear-errors!",clear_errors,0,scheme_module);
  KNO_LINK_PRIM("%err",error_prim,4,scheme_module);

  KNO_LINK_ALIAS("ex/cond",exception_condition,scheme_module);
  KNO_LINK_ALIAS("error-condition",exception_condition,scheme_module);
  KNO_LINK_ALIAS("ex/caller",exception_caller,scheme_module);
  KNO_LINK_ALIAS("error-caller",exception_caller,scheme_module);
  KNO_LINK_ALIAS("error-context",exception_caller,scheme_module);
  KNO_LINK_ALIAS("ex/details",exception_details,scheme_module);
  KNO_LINK_ALIAS("error-details",exception_details,scheme_module);
  KNO_LINK_ALIAS("error-irritant",exception_irritant,scheme_module);
  KNO_LINK_ALIAS("get-irritant",exception_irritant,scheme_module);
  KNO_LINK_ALIAS("error-xdata",exception_irritant,scheme_module);
  KNO_LINK_ALIAS("error-irritant?",exception_has_irritant,scheme_module);
  KNO_LINK_ALIAS("ex/stack",exception_stack,scheme_module);
  KNO_LINK_ALIAS("error-summary",exception_summary,scheme_module);
}
