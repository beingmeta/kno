/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/compounds.h"
#include "framerd/support.h"
#include "framerd/errobjs.h"
#include "framerd/eval.h"
#include "framerd/ports.h"
#include "framerd/sequences.h"

static u8_condition SchemeError=_("Undistinguished Scheme Error");

static lispval stack_entry_symbol;

/* Returning error objects when troubled */

static lispval catcherr_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval toeval = fd_get_arg(expr,1);
  lispval value = fd_stack_eval(toeval,env,_stack,0);
  if (FD_THROWP(value))
    return value;
  else if (FD_ABORTP(value)) {
    u8_exception ex = u8_erreify();
    lispval exception_object = fd_wrap_exception(ex);
    u8_free_exception(ex,1);
    return exception_object;}
  else return value;
}

/* Returning errors */

static lispval error_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  u8_condition ex = SchemeError, cxt = NULL;
  lispval head = fd_get_arg(expr,0);
  lispval arg1 = fd_get_arg(expr,1);
  lispval arg2 = fd_get_arg(expr,2);
  lispval printout_body;
  if ((SYMBOLP(arg1)) && (SYMBOLP(arg2))) {
    ex = (u8_condition)(SYM_NAME(arg1));
    cxt = (u8_context)(SYM_NAME(arg2));
    printout_body = fd_get_body(expr,3);}
  else if (SYMBOLP(arg1)) {
    ex = (u8_condition)(SYM_NAME(arg1));
    printout_body = fd_get_body(expr,2);}
  else if (SYMBOLP(head)) {
    ex = (u8_condition)(SYM_NAME(head));
    printout_body = fd_get_body(expr,1);}
  else printout_body = fd_get_body(expr,1);

  {
    U8_OUTPUT out; U8_INIT_OUTPUT(&out,256);
    fd_printout_to(&out,printout_body,env);
    fd_seterr(ex,cxt,out.u8_outbuf,VOID);
    u8_close_output(&out);
    return FD_ERROR;}
}

static lispval error_prim(lispval condition,lispval caller,
                          lispval details_arg,
                          lispval irritant)
{
  u8_string details = NULL; int free_details=0;
  if ((FD_VOIDP(details_arg)) || (FD_DEFAULTP(details_arg)))
    details=NULL;
  else if (FD_STRINGP(details_arg)) details=FD_CSTRING(details_arg);
  else if (FD_SYMBOLP(details_arg)) details=FD_CSTRING(details_arg);
  else {
    details=fd_lisp2string(details_arg);
    free_details=1;}
  fd_seterr(FD_SYMBOL_NAME(condition),
            ((FD_VOIDP(caller)) ? (NULL) : (FD_SYMBOL_NAME(caller))),
            details,irritant);
  if (free_details) u8_free(details);
  return FD_ERROR;
}

static lispval irritant_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  u8_condition ex = SchemeError, cxt = NULL;
  lispval head = fd_get_arg(expr,0);
  lispval irritant_expr = fd_get_arg(expr,1), irritant=VOID;
  lispval arg1 = fd_get_arg(expr,2);
  lispval arg2 = fd_get_arg(expr,3);
  lispval printout_body;

  if ((SYMBOLP(arg1)) && (SYMBOLP(arg2))) {
    ex = (u8_condition)(SYM_NAME(arg1));
    cxt = (u8_context)(SYM_NAME(arg2));
    printout_body = fd_get_body(expr,4);}
  else if (SYMBOLP(arg1)) {
    ex = (u8_condition)(SYM_NAME(arg1));
    printout_body = fd_get_body(expr,3);}
  else if (STRINGP(arg1)) {
    u8_string pname = CSTRING(arg1);
    lispval sym = fd_intern(pname);
    ex = (u8_condition)(SYM_NAME(sym));
    printout_body = fd_get_body(expr,3);}
  else if (SYMBOLP(head)) {
    ex = (u8_condition)"Bad irritant arg";
    printout_body = fd_get_body(expr,2);}
  else printout_body = fd_get_body(expr,2);

  if (VOIDP(irritant))
    irritant = fd_eval(irritant_expr,env);

  if (FD_ABORTP(irritant)) {
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
    irritant=FD_VOID;}

  if (cxt == NULL) {
    fd_stack cur = fd_stackptr;
    while ( (cur) && (cxt==NULL) ) {
      lispval op = cur->stack_op;
      if (SYMBOLP(op))
        cxt=FD_SYMBOL_NAME(op);
      else if (FD_FUNCTIONP(op)) {
        struct FD_FUNCTION *fcn=FD_XFUNCTION(op);
        if (fcn->fcn_name)
          cxt=fcn->fcn_name;
        else cxt="FUNCTIONCALL";}
      else {}
      cur=cur->stack_caller;}}

  u8_string details=NULL;
  U8_OUTPUT out; U8_INIT_OUTPUT(&out,256);
  fd_printout_to(&out,printout_body,env);
  if (out.u8_outbuf == out.u8_write) {
    u8_close_output(&out);
    details=NULL;}
  else details=out.u8_outbuf;

  lispval err_result=fd_err(ex,cxt,details,irritant);
  fd_decref(irritant);
  if (details) u8_close_output(&out);
  return err_result;
}

static lispval onerror_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval toeval = fd_get_arg(expr,1);
  lispval error_handler = fd_get_arg(expr,2);
  lispval default_handler = fd_get_arg(expr,3);
  lispval value = fd_stack_eval(toeval,env,_stack,0);
  if (FD_THROWP(value))
    return value;
  else if (FD_BREAKP(value))
    return value;
  else if (FD_ABORTP(value)) {
    u8_exception ex = u8_erreify();
    lispval handler = fd_stack_eval(error_handler,env,_stack,0);
    if (FD_ABORTP(handler)) {
      u8_restore_exception(ex);
      return handler;}
    else if (FD_APPLICABLEP(handler)) {
      lispval err_value = fd_wrap_exception(ex);
      lispval handler_result = fd_apply(handler,1,&err_value);
      if (FD_ABORTP(handler_result)) {
        u8_exception handler_ex = u8_erreify();
        /* Clear this field so we can decref err_value while leaving
           the exception object current. */
        if (handler_ex)
          u8_log(LOG_WARN,"Recursive error",
                 "Error %m handling error during %q",
                 handler_ex->u8x_cond,toeval);
        else u8_log(LOG_WARN,"Obscure Recursive error",
                    "Obscure error caught in %q",toeval);
        fd_log_backtrace(handler_ex,LOG_WARN,"RecursiveError",128);
        u8_restore_exception(ex);
        u8_restore_exception(handler_ex);
        fd_decref(value);
        fd_decref(handler);
        fd_decref(err_value);
        return handler_result;}
      else {
        fd_decref(value);
        fd_decref(handler);
        fd_decref(err_value);
        fd_clear_errors(1);
        u8_free_exception(ex,1);
        return handler_result;}}
    else {
      u8_free_exception(ex,1);
      fd_decref(value);
      return handler;}}
  else if (VOIDP(default_handler))
    return value;
  else {
    lispval handler = fd_stack_eval(default_handler,env,_stack,0);
    if (FD_ABORTP(handler))
      return handler;
    else if (FD_APPLICABLEP(handler)) {
      lispval result;
      if (VOIDP(value))
        result = fd_finish_call(fd_dapply(handler,0,&value));
      else result = fd_finish_call(fd_dapply(handler,1,&value));
      fd_decref(value);
      fd_decref(handler);
      return result;}
    else {
      fd_decref(value);
      return handler;}}
}

/* Report/clear errors */

static lispval report_errors_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval toeval = fd_get_arg(expr,1);
  lispval value = fd_stack_eval(toeval,env,_stack,0);
  if (FD_THROWP(value))
    return value;
  else if (FD_ABORTP(value)) {
    u8_exception ex = u8_current_exception;
    fd_log_backtrace(ex,LOG_NOTICE,"CaughtError",128);
    fd_clear_errors(0);
    return FD_FALSE;}
  else return value;
}

static lispval ignore_errors_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval toeval = fd_get_arg(expr,1);
  lispval dflt = fd_get_arg(expr,2);
  lispval value = fd_stack_eval(toeval,env,_stack,0);
  if (FD_THROWP(value))
    return value;
  else if (FD_ABORTP(value)) {
    fd_clear_errors(0);
    if (FD_VOIDP(dflt))
      return FD_FALSE;
    else {
      lispval to_return = fd_stack_eval(dflt,env,_stack,0);
      if (FD_ABORTP(to_return)) {
        fd_clear_errors(0);
        return FD_FALSE;}
      else return to_return;}
    return FD_FALSE;}
  else return value;
}

static lispval clear_errors()
{
  int n_errs = fd_clear_errors(1);
  if (n_errs) return FD_INT(n_errs);
  else return FD_FALSE;
}

/* Primitives on exception objects */

static lispval exception_condition(lispval x)
{
  struct FD_EXCEPTION *xo=
    fd_consptr(struct FD_EXCEPTION *,x,fd_exception_type);
  if (xo->ex_condition)
    return fd_intern(xo->ex_condition);
  else return FD_FALSE;
}

static lispval exception_caller(lispval x)
{
  struct FD_EXCEPTION *xo=
    fd_consptr(struct FD_EXCEPTION *,x,fd_exception_type);
  if (xo->ex_caller)
    return fd_intern(xo->ex_caller);
  else return FD_FALSE;
}

static lispval exception_details(lispval x)
{
  struct FD_EXCEPTION *xo=
    fd_consptr(struct FD_EXCEPTION *,x,fd_exception_type);
  if (xo->ex_details)
    return lispval_string(xo->ex_details);
  else return FD_FALSE;
}

static lispval exception_irritant(lispval x)
{
  struct FD_EXCEPTION *xo=
    fd_consptr(struct FD_EXCEPTION *,x,fd_exception_type);
  if (FD_VOIDP(xo->ex_irritant))
    return FD_FALSE;
  else return fd_incref(xo->ex_irritant);
}

static lispval exception_context(lispval x)
{
  struct FD_EXCEPTION *xo=
    fd_consptr(struct FD_EXCEPTION *,x,fd_exception_type);
  if (FD_VOIDP(xo->ex_context))
    return FD_FALSE;
  else return fd_incref(xo->ex_context);
}

static lispval exception_has_irritant(lispval x)
{
  struct FD_EXCEPTION *xo=
    fd_consptr(struct FD_EXCEPTION *,x,fd_exception_type);
  if (FD_VOIDP(xo->ex_irritant))
    return FD_FALSE;
  else return FD_TRUE;
}

static lispval exception_moment(lispval x)
{
  struct FD_EXCEPTION *xo=
    fd_consptr(struct FD_EXCEPTION *,x,fd_exception_type);
  if (xo->ex_moment >= 0)
    return fd_make_flonum(xo->ex_moment);
  else return FD_FALSE;
}

static lispval exception_threadno(lispval x)
{
  struct FD_EXCEPTION *xo=
    fd_consptr(struct FD_EXCEPTION *,x,fd_exception_type);
  if (xo->ex_thread >= 0)
    return FD_INT(xo->ex_thread);
  else return FD_FALSE;
}

static lispval exception_timebase(lispval x)
{
  struct FD_EXCEPTION *xo=
    fd_consptr(struct FD_EXCEPTION *,x,fd_exception_type);
  if (xo->ex_timebase >= 0)
    return fd_time2timestamp(xo->ex_timebase);
  else return FD_FALSE;
}

static lispval exception_sessionid(lispval x)
{
  struct FD_EXCEPTION *xo=
    fd_consptr(struct FD_EXCEPTION *,x,fd_exception_type);
  if (xo->ex_session >= 0)
    return lispval_string(xo->ex_session);
  else return FD_FALSE;
}

static lispval exception_add_context(lispval x,lispval slotid,lispval value)
{
  struct FD_EXCEPTION *xo=
    fd_consptr(struct FD_EXCEPTION *,x,fd_exception_type);
  lispval context = xo->ex_context;
  lispval slotmap = FD_VOID;
  if ( (FD_NULLP(context)) || (FD_VOIDP(context)) || (FD_FALSEP(context)) )
    context = FD_EMPTY;
  if (FD_VOIDP(value)) {}
  else if (FD_SLOTMAPP(context))
    slotmap = context;
  else if (FD_CHOICEP(context)) {
    FD_DO_CHOICES(cxt,context) {
      if (FD_SLOTMAPP(context))  {
        slotmap = cxt;
        FD_STOP_DO_CHOICES;}}}
  else NO_ELSE;
  if (FD_VOIDP(value)) {
    fd_incref(slotid);
    FD_ADD_TO_CHOICE(context,slotid);}
  else if (!(FD_SLOTMAPP(slotmap))) {
    slotmap = fd_make_slotmap(4,0,NULL);
    FD_ADD_TO_CHOICE(context,slotmap);}
  else NO_ELSE;
  if (FD_SLOTMAPP(slotmap))
    fd_add(slotmap,slotid,value);
  xo->ex_context = context;
  return FD_VOID;
}

static lispval condition_symbol, caller_symbol, timebase_symbol, moment_symbol,
  thread_symbol, details_symbol, session_symbol, context_symbol,
  irritant_symbol, stack_symbol, env_tag, args_tag;


static lispval exception2slotmap(lispval x,lispval with_stack_arg)
{
  struct FD_EXCEPTION *xo=
    fd_consptr(struct FD_EXCEPTION *,x,fd_exception_type);
  lispval result = fd_make_slotmap(7,0,NULL);
  if (xo->ex_condition)
    fd_store(result,condition_symbol,fd_intern(xo->ex_condition));
  if (xo->ex_caller)
    fd_store(result,caller_symbol,fd_intern(xo->ex_caller));
  if (xo->ex_timebase > 0) {
    lispval timebase = fd_time2timestamp(xo->ex_timebase);
    fd_store(result,timebase_symbol,timebase);
    fd_decref(timebase);}
  if (xo->ex_moment > 0) {
    lispval moment = fd_make_flonum(xo->ex_moment);
    fd_store(result,moment_symbol,moment);
    fd_decref(moment);}
  if (xo->ex_thread > 0) {
    lispval threadnum = FD_INT(xo->ex_thread);
    fd_store(result,thread_symbol,threadnum);
    fd_decref(threadnum);}
  if (xo->ex_details) {
    lispval details_string = lispval_string(xo->ex_details);
    fd_store(result,details_symbol,details_string);
    fd_decref(details_string);}
  if (xo->ex_session) {
    lispval details_string = lispval_string(xo->ex_session);
    fd_store(result,session_symbol,details_string);
    fd_decref(details_string);}
  if (!(FD_VOIDP(xo->ex_context)))
    fd_store(result,context_symbol,xo->ex_context);
  if (!(FD_VOIDP(xo->ex_irritant)))
    fd_store(result,irritant_symbol,xo->ex_irritant);
  if (FD_VECTORP(xo->ex_stack)) {
    lispval vec = xo->ex_stack;
    int len = FD_VECTOR_LENGTH(vec);
    lispval new_stack = fd_init_vector(NULL,len,0);
    int i = 0; while (i < len) {
      lispval entry = FD_VECTOR_REF(vec,i);
      if (FD_EXCEPTIONP(entry)) {
        FD_VECTOR_SET(new_stack,i,exception2slotmap(entry,FD_FALSE));}
      if ( (FD_COMPOUND_TYPEP(entry,stack_entry_symbol)) &&
           ( (FD_COMPOUND_LENGTH(entry)) >= 5) ) {
        lispval copied = fd_copier(entry,0);
        int len = FD_COMPOUND_LENGTH(entry);
        if (len >= 5) {
          lispval argvec = FD_COMPOUND_REF(entry,5);
          if (FD_VECTORP(argvec)) {
            lispval wrapped = fd_init_compound_from_elts
              (NULL,args_tag,FD_COMPOUND_INCREF,
               FD_VECTOR_LENGTH(argvec),
               FD_VECTOR_ELTS(argvec));
            FD_COMPOUND_REF(entry,5) = wrapped;
            fd_decref(argvec);}}
        if (len >= 7) {
          lispval env = FD_COMPOUND_REF(entry,7);
          if (FD_TABLEP(env)) {
            lispval wrapped = fd_init_compound
              (NULL,env_tag,FD_COMPOUND_INCREF,1,env);
            FD_COMPOUND_REF(entry,7) = wrapped;
            fd_decref(env);}}
        FD_VECTOR_SET(new_stack,i,copied);}
      else {
        fd_incref(entry);
        FD_VECTOR_SET(new_stack,i,entry);}
      i++;}
    fd_store(result,stack_symbol,new_stack);
    fd_decref(new_stack);}
  return result;
}

static lispval exception_stack(lispval x,lispval arg1,lispval arg2)
{
  struct FD_EXCEPTION *xo=
    fd_consptr(struct FD_EXCEPTION *,x,fd_exception_type);
  if (!(FD_VECTORP(xo->ex_stack)))
    return fd_empty_vector(0);
  else {
    struct FD_VECTOR *stack = (fd_vector) (xo->ex_stack);
    size_t stack_len   = stack->vec_length;
    if (FD_VOIDP(arg1))
      return fd_incref(xo->ex_stack);
    else if ( (FD_VOIDP(arg2)) && ((FD_FIX2INT(arg1)) > 0) ) {
      long long off = FD_FIX2INT(arg1);
      if (off < 0) off = stack_len+off;
      if (off < 0) off = 0;
      else if (off >= stack_len) off = stack_len-1;
      lispval entry = FD_VECTOR_REF(xo->ex_stack,off);
      return fd_incref(entry);}
    else {
      long long start, end;
      if (FD_VOIDP(arg2)) {
        end = -(FD_FIX2INT(arg1));
        start = 0;}
      else {
        start = FD_FIX2INT(arg1);
        if (FD_FIXNUMP(arg1))
          end = FD_FIX2INT(arg2);
        else end = stack_len;}
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
        lispval entry = FD_VECTOR_REF(xo->ex_stack,start);
        return fd_incref(entry);}
      else if (end > start)
        return fd_slice(xo->ex_stack,start,end);
      else {
        lispval slice = fd_slice(xo->ex_stack,end,start);
        lispval reversed = fd_reverse(slice);
        fd_decref(slice);
        return reversed;}}}
}

static lispval exception_summary(lispval x,lispval with_irritant)
{
  struct FD_EXCEPTION *xo=
    fd_consptr(struct FD_EXCEPTION *,x,fd_exception_type);
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
  return_value = lispval_string(summary);
  u8_free(summary);
  return return_value;
}

static int thunkp(lispval x)
{
  if (!(FD_APPLICABLEP(x))) return 0;
  else {
    struct FD_FUNCTION *f = FD_XFUNCTION(x);
    if (f->fcn_arity==0) return 1;
    else if ((f->fcn_arity<0) && (f->fcn_min_arity==0)) return 1;
    else return 0;}
}

static lispval dynamic_wind_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval wind = fd_get_arg(expr,1);
  lispval doit = fd_get_arg(expr,2);
  lispval unwind = fd_get_arg(expr,3);
  if ((VOIDP(wind)) || (VOIDP(doit)) || (VOIDP(unwind)))
    return fd_err(fd_SyntaxError,"dynamic_wind_evalfn",NULL,expr);
  else {
    wind = fd_stack_eval(wind,env,_stack,0);
    if (FD_ABORTP(wind)) return wind;
    else if (!(thunkp(wind)))
      return fd_type_error("thunk","dynamic_wind_evalfn",wind);
    else doit = fd_stack_eval(doit,env,_stack,0);
    if (FD_ABORTP(doit)) {
      fd_decref(wind);
      return doit;}
    else if (!(thunkp(doit))) {
      fd_decref(wind);
      return fd_type_error("thunk","dynamic_wind_evalfn",doit);}
    else unwind = fd_stack_eval(unwind,env,_stack,0);
    if (FD_ABORTP(unwind)) {
      fd_decref(wind); fd_decref(doit);
      return unwind;}
    else if (!(thunkp(unwind))) {
      fd_decref(wind); fd_decref(doit);
      return fd_type_error("thunk","dynamic_wind_evalfn",unwind);}
    else {
      lispval retval = fd_apply(wind,0,NULL);
      if (FD_ABORTP(retval)) {}
      else {
        fd_decref(retval);
        retval = fd_apply(doit,0,NULL);
        if (FD_ABORTP(retval)) {
          u8_exception saved = u8_erreify();
          lispval unwindval = fd_apply(unwind,0,NULL);
          u8_restore_exception(saved);
          if (FD_ABORTP(unwindval)) {
            fd_decref(retval);
            retval = unwindval;}
          else fd_decref(unwindval);}
        else {
          lispval unwindval = fd_apply(unwind,0,NULL);
          if (FD_ABORTP(unwindval)) {
            fd_decref(retval);
            retval = unwindval;}
          else fd_decref(unwindval);}}
      fd_decref(wind); fd_decref(doit); fd_decref(unwind);
      return retval;}}
}

static lispval unwind_protect_evalfn(lispval uwp,fd_lexenv env,fd_stack _stack)
{
  lispval heart = fd_get_arg(uwp,1);
  lispval result;
  {U8_WITH_CONTOUR("UNWIND-PROTECT(body)",0)
      result = fd_stack_eval(heart,env,_stack,0);
    U8_ON_EXCEPTION {
      U8_CLEAR_CONTOUR();
      result = FD_ERROR;}
    U8_END_EXCEPTION;}
  {U8_WITH_CONTOUR("UNWIND-PROTECT(unwind)",0)
      {lispval unwinds = fd_get_body(uwp,2);
        FD_DOLIST(expr,unwinds) {
          lispval uw_result = fd_stack_eval(expr,env,_stack,0);
          if (FD_ABORTP(uw_result))
            if (FD_ABORTP(result)) {
              fd_interr(result); fd_interr(uw_result);
              return u8_return(FD_ERROR);}
            else {
              fd_decref(result);
              result = uw_result;
              break;}
          else fd_decref(uw_result);}}
    U8_ON_EXCEPTION {
      U8_CLEAR_CONTOUR();
      result = FD_ERROR;}
    U8_END_EXCEPTION;}
  return result;
}

/* Reraising exceptions */

static lispval reraise_prim(lispval exo)
{
  struct FD_EXCEPTION *xo=
    fd_consptr(struct FD_EXCEPTION *,exo,fd_exception_type);
  fd_restore_exception(xo);
  return FD_ERROR_VALUE;
}

/* Operations on stack objects */

static lispval stack_entry_depth(lispval stackobj)
{
  return fd_compound_ref(stackobj,stack_entry_symbol,0,FD_FALSE);
}

static lispval stack_entry_type(lispval stackobj)
{
  return fd_compound_ref(stackobj,stack_entry_symbol,1,FD_FALSE);
}

static lispval stack_entry_label(lispval stackobj)
{
  return fd_compound_ref(stackobj,stack_entry_symbol,2,FD_FALSE);
}

static lispval stack_entry_op(lispval stackobj)
{
  return fd_compound_ref(stackobj,stack_entry_symbol,3,FD_FALSE);
}

static lispval stack_entry_filename(lispval stackobj)
{
  return fd_compound_ref(stackobj,stack_entry_symbol,4,FD_FALSE);
}

static lispval stack_entry_crumb(lispval stackobj)
{
  return fd_compound_ref(stackobj,stack_entry_symbol,5,FD_FALSE);
}

static lispval stack_entry_args(lispval stackobj)
{
  return fd_compound_ref(stackobj,stack_entry_symbol,6,FD_FALSE);
}

static lispval stack_entry_source(lispval stackobj)
{
  return fd_compound_ref(stackobj,stack_entry_symbol,7,FD_FALSE);
}

static lispval stack_entry_env(lispval stackobj)
{
  return fd_compound_ref(stackobj,stack_entry_symbol,8,FD_FALSE);
}

static lispval stack_entry_status(lispval stackobj)
{
  return fd_compound_ref(stackobj,stack_entry_symbol,9,FD_FALSE);
}

static u8_string static_string(lispval x,int err)
{
  if (FD_SYMBOLP(x))
    return FD_SYMBOL_NAME(x);
  else if (FD_STRINGP(x)) {
    u8_string s = FD_CSTRING(x);
    lispval sym = fd_intern(s);
    return FD_SYMBOL_NAME(sym);}
  else if (err)
    return "BadExceptionArgument";
  else return NULL;
}

static lispval make_exception(lispval cond,lispval caller,lispval details,
                              lispval threadid,lispval sessionid,
                              lispval moment,lispval timebase,
                              lispval stack,lispval context,
                              lispval irritant)
{
  double secs = (FD_FLONUMP(moment)) ? (FD_FLONUM(moment)) : (-1);
  struct FD_TIMESTAMP *tstamp = (FD_TYPEP(timebase,fd_timestamp_type)) ?
    ((fd_timestamp) timebase) : (NULL);
  time_t tick = (FD_FIXNUMP(timebase)) ? (FD_FIX2INT(timebase)) :
    (tstamp) ? (tstamp->u8xtimeval.u8_tick) : (-1);
  return fd_init_exception
    (NULL,(u8_condition) static_string(cond,1),
     (u8_condition) static_string(caller,0),
     (FD_STRINGP(details)) ? (u8_strdup(FD_CSTRING(details))) : (NULL),
     (FD_DEFAULTP(irritant)) ? (FD_VOID) : (fd_incref(irritant)),
     fd_incref(stack),fd_incref(context),
     (FD_STRINGP(sessionid)) ? (u8_strdup(FD_CSTRING(sessionid))) : (NULL),
     secs,tick,
     (FD_FIXNUMP(threadid)) ? (FD_FIX2INT(threadid)) : (-1));
}

/* Clear errors */

FD_EXPORT void fd_init_errfns_c()
{
  u8_register_source_file(_FILEINFO);

  fd_def_evalfn(fd_scheme_module,"ERROR",
                "(error *condition* *caller* [*irritant*] details...) "
                "signals an error\n"
                "*condition* and *caller* should be symbols and are not "
                "evaluated. *irritant* is a lisp object (evaluated) and "
                "a details message is generated by PRINTOUT ..details",
                error_evalfn);
  fd_def_evalfn(fd_scheme_module,"IRRITANT",
                "(irritant *irritant* *condition* *caller* details...) "
                "signals an error with the designated *irritant* \n"
                "*condition* and *caller* should be symbols and are not "
                "evaluated. A details message is generated by "
                "PRINTOUT ..details",
                irritant_evalfn);

  fd_idefn4(fd_scheme_module,"%ERR",error_prim,FD_NEEDS_1_ARG|FD_NDCALL,
            "(%err *cond* [*caller*] [*details*] [*irritant*]) returns "
            "an error object with condition *cond* (a symbol), "
            "a *caller* (also a symbol), *details* (usually a string), "
            "and *irritant* (a lisp object).",
            fd_symbol_type,FD_VOID,fd_symbol_type,FD_VOID,
            -1,FD_VOID,-1,FD_VOID);

  fd_def_evalfn(fd_scheme_module,"ONERROR",
                "(ONERROR *expr* *onerr* [*normally*])\n"
                "Evaluates *expr*, catching errors during the evaluation. "
                "If there are errors, either apply *onerr* to an exception "
                "object or simply return *onerr* if it's not applicable. "
                "If there are no errors and *normal* is not specified, "
                "return the result. If *normal* is specified, apply it to "
                "the result and return it's result.",
                onerror_evalfn);
  fd_def_evalfn(fd_scheme_module,"CATCHERR",
                "(CATCHERR *expr*) returns the result of *expr* "
                "or an exception object describing any signalled errors",
                catcherr_evalfn);
  fd_def_evalfn(fd_scheme_module,"ERREIFY",
                "(ERREIFY *expr*) returns the result of *expr* "
                "or an exception object describing any signalled errors",
                catcherr_evalfn);

  fd_def_evalfn(fd_scheme_module,"REPORT-ERRORS",
                "(REPORT-ERRORS *expr* [*errval*]) evaluates *expr*, "
                "reporting and clearing any errors.\n"
                "Returns *errval* or #f if any errors occur.",
                report_errors_evalfn);
  fd_def_evalfn(fd_scheme_module,"IGNORE-ERRORS",
                "(REPORT-ERRORS *expr* [*errval*]) evaluates *expr*, "
                "reporting and clearing any errors.\n"
                "Returns *errval* or #f if any errors occur.",
                ignore_errors_evalfn);
  fd_idefn(fd_scheme_module,
           fd_make_cprim0("CLEAR-ERRORS!",clear_errors));

  /* Deprecated, from old error system */
  fd_def_evalfn(fd_scheme_module,"NEWERR","",irritant_evalfn);


  fd_idefn1(fd_scheme_module,"RERAISE",reraise_prim,1,
            "Reraises the exception represented by an object",
            fd_exception_type,FD_VOID);

  fd_idefn1(fd_scheme_module,"EXCEPTION-CONDITION",exception_condition,1,
            "Returns the condition name (a symbol) for an exception",
            fd_exception_type,VOID);
  fd_idefn1(fd_scheme_module,"EXCEPTION-CALLER",exception_caller,1,
            "Returns the immediate context (caller) for an exception",
            fd_exception_type,VOID);
  fd_idefn1(fd_scheme_module,"EXCEPTION-DETAILS",exception_details,1,
            "Returns any descriptive details (a string) for the exception",
            fd_exception_type,VOID);
  fd_idefn1(fd_scheme_module,"EXCEPTION-MOMENT",exception_moment,1,
            "Returns the time an exception was initially recorded",
            fd_exception_type,VOID);
  fd_idefn1(fd_scheme_module,"EXCEPTION-TIMEBASE",exception_timebase,1,
            "Returns the time from which exception moments are measured",
            fd_exception_type,VOID);
  fd_idefn1(fd_scheme_module,"EXCEPTION-THREADNO",exception_threadno,1,
            "Returns an integer ID for the thread where the error occurred",
            fd_exception_type,VOID);
  fd_idefn1(fd_scheme_module,"EXCEPTION-SESSIONID",exception_sessionid,1,
            "Returns the sessionid when the error occurred",
            fd_exception_type,VOID);
  fd_idefn1(fd_scheme_module,"EXCEPTION-IRRITANT",exception_irritant,1,
            "Returns the LISP object (if any) which 'caused' the exception",
            fd_exception_type,VOID);
  fd_idefn1(fd_scheme_module,"EXCEPTION-CONTEXT",exception_context,1,
            "Returns aspects of where the error occured",
            fd_exception_type,VOID);
  fd_idefn1(fd_scheme_module,"EXCEPTION-IRRITANT?",exception_has_irritant,1,
            "(EXCEPTION-IRRITANT? *ex*) Returns true if *ex* is an exception "
            "and has an 'irritant'",
            fd_exception_type,VOID);
  fd_idefn2(fd_scheme_module,"EXCEPTION->SLOTMAP",exception2slotmap,1,
            "(EXCEPTION->SLOTMAP *ex*) Breaks out the elements of "
            "the exception *ex* into a slotmap",
            fd_exception_type,VOID,-1,FD_FALSE);

  fd_idefn3(fd_scheme_module,"EXCEPTION-STACK",exception_stack,1,
            "(EXCEPTION-STACK *ex* [*len*] [*start*]) returns the call stack "
            "(or a slice of it) where exception object *ex* occurred",
            fd_exception_type,VOID,fd_fixnum_type,FD_VOID,-1,FD_VOID);
  fd_defalias(fd_scheme_module,"EX/STACK","EXCEPTION-STACK");
  fd_idefn2(fd_scheme_module,"EXCEPTION-SUMMARY",exception_summary,1,
            "Returns a shortened backtrace for an exception",
            fd_exception_type,VOID,-1,FD_FALSE);
  fd_defalias(fd_scheme_module,"EX/CALLER","EXCEPTION-CALLER");
  fd_defalias(fd_scheme_module,"EX/COND","EXCEPTION-CONDITION");
  fd_defalias(fd_scheme_module,"EX/DETAILS","EXCEPTION-DETAILS");

  fd_idefn3(fd_scheme_module,"EXCEPTION/CONTEXT!",
            exception_add_context,2|FD_NDCALL,
            "Creates an exception object",
            fd_exception_type,VOID,-1,FD_VOID,-1,FD_VOID);

  fd_idefn10(fd_scheme_module,"MAKE-EXCEPTION",make_exception,1|FD_NDCALL,
             "Creates an exception object",
             -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,
             -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,
             -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,
             -1,FD_VOID);

  fd_defalias(fd_scheme_module,"ERROR-CONDITION","EXCEPTION-CONDITION");
  fd_defalias(fd_scheme_module,"ERROR-CALLER","EXCEPTION-CALLER");
  fd_defalias(fd_scheme_module,"ERROR-DETAILS","EXCEPTION-DETAILS");
  fd_defalias(fd_scheme_module,"ERROR-IRRITANT","EXCEPTION-IRRITANT");
  fd_defalias(fd_scheme_module,"ERROR-BACKTRACE","EXCEPTION-BACKTRACE");
  fd_defalias(fd_scheme_module,"ERROR-SUMMARY","EXCEPTION-SUMMARY");

  fd_defalias(fd_scheme_module,"ERROR-CONTEXT","EXCEPTION-CALLER");
  fd_defalias(fd_scheme_module,"GET-IRRITANT","EXCEPTION-IRRITANT");
  fd_defalias(fd_scheme_module,"ERROR-IRRITANT","EXCEPTION-IRRITANT");
  fd_defalias(fd_scheme_module,"ERROR-XDATA","ERROR-IRRITANT");
  fd_defalias(fd_scheme_module,"ERROR-IRRITANT?","EXCEPTION-IRRITANT?");

  fd_def_evalfn(fd_scheme_module,"DYNAMIC-WIND","",dynamic_wind_evalfn);
  fd_def_evalfn(fd_scheme_module,"UNWIND-PROTECT","",unwind_protect_evalfn);

  fd_idefn1(fd_scheme_module,"STACK-DEPTH",stack_entry_depth,1,
            "Returns the depth of a stack entry",
            fd_compound_type,FD_VOID);
  fd_idefn1(fd_scheme_module,"STACK-TYPE",stack_entry_type,1,
            "Returns the type of a stack entry",
            fd_compound_type,FD_VOID);
  fd_idefn1(fd_scheme_module,"STACK-OP",stack_entry_op,1,
            "Returns the op of a stack entry",
            fd_compound_type,FD_VOID);
  fd_idefn1(fd_scheme_module,"STACK-LABEL",stack_entry_label,1,
            "Returns the label of a stack entry",
            fd_compound_type,FD_VOID);
  fd_idefn1(fd_scheme_module,"STACK-FILENAME",stack_entry_filename,1,
            "Returns the label of a stack entry",
            fd_compound_type,FD_VOID);
  fd_idefn1(fd_scheme_module,"STACK-CRUMB",stack_entry_crumb,1,
            "Returns a probably unique integer identifier for the stack frame",
            fd_compound_type,FD_VOID);
  fd_idefn1(fd_scheme_module,"STACK-ARGS",stack_entry_args,1,
            "Returns the args of a stack entry",
            fd_compound_type,FD_VOID);
  fd_idefn1(fd_scheme_module,"STACK-SOURCE",stack_entry_source,1,
            "Returns the source of a stack entry",
            fd_compound_type,FD_VOID);
  fd_idefn1(fd_scheme_module,"STACK-ENV",stack_entry_env,1,
            "Returns the env of a stack entry",
            fd_compound_type,FD_VOID);
  fd_idefn1(fd_scheme_module,"STACK-STATUS",stack_entry_status,1,
            "Returns the status of a stack entry",
            fd_compound_type,FD_VOID);

  stack_entry_symbol = fd_intern("_STACK");
  condition_symbol = fd_intern("CONDITION");
  caller_symbol = fd_intern("CALLER");
  timebase_symbol = fd_intern("TIMEBASE");
  moment_symbol = fd_intern("MOMENT");
  thread_symbol = fd_intern("THREAD");
  details_symbol = fd_intern("DETAILS");
  session_symbol = fd_intern("SESSION");
  context_symbol = fd_intern("CONTEXT");
  irritant_symbol = fd_intern("IRRITANT");
  stack_symbol = fd_intern("STACK");
  env_tag = fd_intern("%ENV");
  args_tag = fd_intern("%ARGS");

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
