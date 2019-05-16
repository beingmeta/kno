/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/dtype.h"
#include "kno/compounds.h"
#include "kno/support.h"
#include "kno/errobjs.h"
#include "kno/eval.h"
#include "kno/ports.h"
#include "kno/sequences.h"

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

  {
    U8_OUTPUT out; U8_INIT_OUTPUT(&out,256);
    kno_printout_to(&out,printout_body,env);
    kno_seterr(ex,cxt,out.u8_outbuf,VOID);
    u8_close_output(&out);
    return KNO_ERROR;}
}

static lispval error_prim(lispval condition,lispval caller,
                          lispval details_arg,
                          lispval irritant)
{
  u8_string details = NULL; int free_details=0;
  if ((KNO_VOIDP(details_arg)) || (KNO_DEFAULTP(details_arg)))
    details=NULL;
  else if (KNO_STRINGP(details_arg)) details=KNO_CSTRING(details_arg);
  else if (KNO_SYMBOLP(details_arg)) details=KNO_CSTRING(details_arg);
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
        struct KNO_FUNCTION *fcn=KNO_XFUNCTION(op);
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
        result = kno_finish_call(kno_dapply(handler,0,&value));
      else result = kno_finish_call(kno_dapply(handler,1,&value));
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

static lispval clear_errors()
{
  int n_errs = kno_clear_errors(1);
  if (n_errs) return KNO_INT(n_errs);
  else return KNO_FALSE;
}

/* Primitives on exception objects */

static lispval exception_condition(lispval x)
{
  struct KNO_EXCEPTION *xo=
    kno_consptr(struct KNO_EXCEPTION *,x,kno_exception_type);
  if (xo->ex_condition)
    return kno_intern(xo->ex_condition);
  else return KNO_FALSE;
}

static lispval exception_caller(lispval x)
{
  struct KNO_EXCEPTION *xo=
    kno_consptr(struct KNO_EXCEPTION *,x,kno_exception_type);
  if (xo->ex_caller)
    return kno_intern(xo->ex_caller);
  else return KNO_FALSE;
}

static lispval exception_details(lispval x)
{
  struct KNO_EXCEPTION *xo=
    kno_consptr(struct KNO_EXCEPTION *,x,kno_exception_type);
  if (xo->ex_details)
    return lispval_string(xo->ex_details);
  else return KNO_FALSE;
}

static lispval exception_irritant(lispval x)
{
  struct KNO_EXCEPTION *xo=
    kno_consptr(struct KNO_EXCEPTION *,x,kno_exception_type);
  if (KNO_VOIDP(xo->ex_irritant))
    return KNO_FALSE;
  else return kno_incref(xo->ex_irritant);
}

static lispval exception_context(lispval x)
{
  struct KNO_EXCEPTION *xo=
    kno_consptr(struct KNO_EXCEPTION *,x,kno_exception_type);
  if (KNO_VOIDP(xo->ex_context))
    return KNO_FALSE;
  else return kno_incref(xo->ex_context);
}

static lispval exception_has_irritant(lispval x)
{
  struct KNO_EXCEPTION *xo=
    kno_consptr(struct KNO_EXCEPTION *,x,kno_exception_type);
  if (KNO_VOIDP(xo->ex_irritant))
    return KNO_FALSE;
  else return KNO_TRUE;
}

static lispval exception_moment(lispval x)
{
  struct KNO_EXCEPTION *xo=
    kno_consptr(struct KNO_EXCEPTION *,x,kno_exception_type);
  if (xo->ex_moment >= 0)
    return kno_make_flonum(xo->ex_moment);
  else return KNO_FALSE;
}

static lispval exception_threadno(lispval x)
{
  struct KNO_EXCEPTION *xo=
    kno_consptr(struct KNO_EXCEPTION *,x,kno_exception_type);
  if (xo->ex_thread >= 0)
    return KNO_INT(xo->ex_thread);
  else return KNO_FALSE;
}

static lispval exception_timebase(lispval x)
{
  struct KNO_EXCEPTION *xo=
    kno_consptr(struct KNO_EXCEPTION *,x,kno_exception_type);
  if (xo->ex_timebase >= 0)
    return kno_time2timestamp(xo->ex_timebase);
  else return KNO_FALSE;
}

static lispval exception_sessionid(lispval x)
{
  struct KNO_EXCEPTION *xo=
    kno_consptr(struct KNO_EXCEPTION *,x,kno_exception_type);
  if (xo->ex_session >= 0)
    return lispval_string(xo->ex_session);
  else return KNO_FALSE;
}

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
    lispval details_string = lispval_string(xo->ex_details);
    kno_store(result,details_symbol,details_string);
    kno_decref(details_string);}
  if (xo->ex_session) {
    lispval details_string = lispval_string(xo->ex_session);
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
        if (len >= 5) {
          lispval argvec = KNO_COMPOUND_REF(entry,5);
          if (KNO_VECTORP(argvec)) {
            lispval wrapped = kno_init_compound_from_elts
              (NULL,args_tag,KNO_COMPOUND_INCREF,
               KNO_VECTOR_LENGTH(argvec),
               KNO_VECTOR_ELTS(argvec));
            KNO_COMPOUND_REF(entry,5) = wrapped;
            kno_decref(argvec);}}
        if (len >= 7) {
          lispval env = KNO_COMPOUND_REF(entry,7);
          if (KNO_TABLEP(env)) {
            lispval wrapped = kno_init_compound
              (NULL,env_tag,KNO_COMPOUND_INCREF,1,env);
            KNO_COMPOUND_REF(entry,7) = wrapped;
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
        if (KNO_FIXNUMP(arg1))
          end = KNO_FIX2INT(arg2);
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
  return_value = lispval_string(summary);
  u8_free(summary);
  return return_value;
}

static int thunkp(lispval x)
{
  if (!(KNO_APPLICABLEP(x))) return 0;
  else {
    struct KNO_FUNCTION *f = KNO_XFUNCTION(x);
    if (f->fcn_arity==0) return 1;
    else if ((f->fcn_arity<0) && (f->fcn_min_arity==0)) return 1;
    else return 0;}
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
    if (KNO_ABORTP(wind)) return wind;
    else if (!(thunkp(wind)))
      return kno_type_error("thunk","dynamic_wind_evalfn",wind);
    else doit = kno_stack_eval(doit,env,_stack,0);
    if (KNO_ABORTP(doit)) {
      kno_decref(wind);
      return doit;}
    else if (!(thunkp(doit))) {
      kno_decref(wind);
      return kno_type_error("thunk","dynamic_wind_evalfn",doit);}
    else unwind = kno_stack_eval(unwind,env,_stack,0);
    if (KNO_ABORTP(unwind)) {
      kno_decref(wind); kno_decref(doit);
      return unwind;}
    else if (!(thunkp(unwind))) {
      kno_decref(wind); kno_decref(doit);
      return kno_type_error("thunk","dynamic_wind_evalfn",unwind);}
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

/* Reraising exceptions */

static lispval reraise_prim(lispval exo)
{
  struct KNO_EXCEPTION *xo=
    kno_consptr(struct KNO_EXCEPTION *,exo,kno_exception_type);
  kno_restore_exception(xo);
  return KNO_ERROR_VALUE;
}

/* Operations on stack objects */

static lispval stack_entry_depth(lispval stackobj)
{
  return kno_compound_ref(stackobj,stack_entry_symbol,0,KNO_FALSE);
}

static lispval stack_entry_type(lispval stackobj)
{
  return kno_compound_ref(stackobj,stack_entry_symbol,1,KNO_FALSE);
}

static lispval stack_entry_label(lispval stackobj)
{
  return kno_compound_ref(stackobj,stack_entry_symbol,2,KNO_FALSE);
}

static lispval stack_entry_op(lispval stackobj)
{
  return kno_compound_ref(stackobj,stack_entry_symbol,3,KNO_FALSE);
}

static lispval stack_entry_filename(lispval stackobj)
{
  return kno_compound_ref(stackobj,stack_entry_symbol,4,KNO_FALSE);
}

static lispval stack_entry_crumb(lispval stackobj)
{
  return kno_compound_ref(stackobj,stack_entry_symbol,5,KNO_FALSE);
}

static lispval stack_entry_args(lispval stackobj)
{
  return kno_compound_ref(stackobj,stack_entry_symbol,6,KNO_FALSE);
}

static lispval stack_entry_source(lispval stackobj)
{
  return kno_compound_ref(stackobj,stack_entry_symbol,7,KNO_FALSE);
}

static lispval stack_entry_env(lispval stackobj)
{
  return kno_compound_ref(stackobj,stack_entry_symbol,8,KNO_FALSE);
}

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

  kno_def_evalfn(kno_scheme_module,"ERROR",
                "(error *condition* *caller* [*irritant*] details...) "
                "signals an error\n"
                "*condition* and *caller* should be symbols and are not "
                "evaluated. *irritant* is a lisp object (evaluated) and "
                "a details message is generated by PRINTOUT ..details",
                error_evalfn);
  kno_def_evalfn(kno_scheme_module,"IRRITANT",
                "(irritant *irritant* *condition* *caller* details...) "
                "signals an error with the designated *irritant* \n"
                "*condition* and *caller* should be symbols and are not "
                "evaluated. A details message is generated by "
                "PRINTOUT ..details",
                irritant_evalfn);

  kno_idefn4(kno_scheme_module,"%ERR",error_prim,KNO_NEEDS_1_ARG|KNO_NDCALL,
            "(%err *cond* [*caller*] [*details*] [*irritant*]) returns "
            "an error object with condition *cond* (a symbol), "
            "a *caller* (also a symbol), *details* (usually a string), "
            "and *irritant* (a lisp object).",
            kno_symbol_type,KNO_VOID,kno_symbol_type,KNO_VOID,
            -1,KNO_VOID,-1,KNO_VOID);

  kno_def_evalfn(kno_scheme_module,"ONERROR",
                "(ONERROR *expr* *onerr* [*normally*])\n"
                "Evaluates *expr*, catching errors during the evaluation. "
                "If there are errors, either apply *onerr* to an exception "
                "object or simply return *onerr* if it's not applicable. "
                "If there are no errors and *normal* is not specified, "
                "return the result. If *normal* is specified, apply it to "
                "the result and return it's result.",
                onerror_evalfn);
  kno_def_evalfn(kno_scheme_module,"CATCHERR",
                "(CATCHERR *expr*) returns the result of *expr* "
                "or an exception object describing any signalled errors",
                catcherr_evalfn);
  kno_def_evalfn(kno_scheme_module,"ERREIFY",
                "(ERREIFY *expr*) returns the result of *expr* "
                "or an exception object describing any signalled errors",
                catcherr_evalfn);

  kno_def_evalfn(kno_scheme_module,"REPORT-ERRORS",
                "(REPORT-ERRORS *expr* [*errval*]) evaluates *expr*, "
                "reporting and clearing any errors.\n"
                "Returns *errval* or #f if any errors occur.",
                report_errors_evalfn);
  kno_def_evalfn(kno_scheme_module,"IGNORE-ERRORS",
                "(REPORT-ERRORS *expr* [*errval*]) evaluates *expr*, "
                "reporting and clearing any errors.\n"
                "Returns *errval* or #f if any errors occur.",
                ignore_errors_evalfn);
  kno_idefn(kno_scheme_module,
           kno_make_cprim0("CLEAR-ERRORS!",clear_errors));

  /* Deprecated, from old error system */
  kno_def_evalfn(kno_scheme_module,"NEWERR","",irritant_evalfn);


  kno_idefn1(kno_scheme_module,"RERAISE",reraise_prim,1,
            "Reraises the exception represented by an object",
            kno_exception_type,KNO_VOID);

  kno_idefn1(kno_scheme_module,"EXCEPTION-CONDITION",exception_condition,1,
            "Returns the condition name (a symbol) for an exception",
            kno_exception_type,VOID);
  kno_idefn1(kno_scheme_module,"EXCEPTION-CALLER",exception_caller,1,
            "Returns the immediate context (caller) for an exception",
            kno_exception_type,VOID);
  kno_idefn1(kno_scheme_module,"EXCEPTION-DETAILS",exception_details,1,
            "Returns any descriptive details (a string) for the exception",
            kno_exception_type,VOID);
  kno_idefn1(kno_scheme_module,"EXCEPTION-MOMENT",exception_moment,1,
            "Returns the time an exception was initially recorded",
            kno_exception_type,VOID);
  kno_idefn1(kno_scheme_module,"EXCEPTION-TIMEBASE",exception_timebase,1,
            "Returns the time from which exception moments are measured",
            kno_exception_type,VOID);
  kno_idefn1(kno_scheme_module,"EXCEPTION-THREADNO",exception_threadno,1,
            "Returns an integer ID for the thread where the error occurred",
            kno_exception_type,VOID);
  kno_idefn1(kno_scheme_module,"EXCEPTION-SESSIONID",exception_sessionid,1,
            "Returns the sessionid when the error occurred",
            kno_exception_type,VOID);
  kno_idefn1(kno_scheme_module,"EXCEPTION-IRRITANT",exception_irritant,1,
            "Returns the LISP object (if any) which 'caused' the exception",
            kno_exception_type,VOID);
  kno_idefn1(kno_scheme_module,"EXCEPTION-CONTEXT",exception_context,1,
            "Returns aspects of where the error occured",
            kno_exception_type,VOID);
  kno_idefn1(kno_scheme_module,"EXCEPTION-IRRITANT?",exception_has_irritant,1,
            "(EXCEPTION-IRRITANT? *ex*) Returns true if *ex* is an exception "
            "and has an 'irritant'",
            kno_exception_type,VOID);
  kno_idefn2(kno_scheme_module,"EXCEPTION->SLOTMAP",exception2slotmap,1,
            "(EXCEPTION->SLOTMAP *ex*) Breaks out the elements of "
            "the exception *ex* into a slotmap",
            kno_exception_type,VOID,-1,KNO_FALSE);

  kno_idefn3(kno_scheme_module,"EXCEPTION-STACK",exception_stack,1,
            "(EXCEPTION-STACK *ex* [*len*] [*start*]) returns the call stack "
            "(or a slice of it) where exception object *ex* occurred",
            kno_exception_type,VOID,kno_fixnum_type,KNO_VOID,-1,KNO_VOID);
  kno_defalias(kno_scheme_module,"EX/STACK","EXCEPTION-STACK");
  kno_idefn2(kno_scheme_module,"EXCEPTION-SUMMARY",exception_summary,1,
            "Returns a shortened backtrace for an exception",
            kno_exception_type,VOID,-1,KNO_FALSE);
  kno_defalias(kno_scheme_module,"EX/CALLER","EXCEPTION-CALLER");
  kno_defalias(kno_scheme_module,"EX/COND","EXCEPTION-CONDITION");
  kno_defalias(kno_scheme_module,"EX/DETAILS","EXCEPTION-DETAILS");

  kno_idefn3(kno_scheme_module,"EXCEPTION/CONTEXT!",
            exception_add_context,2|KNO_NDCALL,
            "Creates an exception object",
            kno_exception_type,VOID,-1,KNO_VOID,-1,KNO_VOID);

  kno_idefn10(kno_scheme_module,"MAKE-EXCEPTION",make_exception,1|KNO_NDCALL,
             "Creates an exception object",
             -1,KNO_VOID,-1,KNO_VOID,-1,KNO_VOID,
             -1,KNO_VOID,-1,KNO_VOID,-1,KNO_VOID,
             -1,KNO_VOID,-1,KNO_VOID,-1,KNO_VOID,
             -1,KNO_VOID);

  kno_defalias(kno_scheme_module,"ERROR-CONDITION","EXCEPTION-CONDITION");
  kno_defalias(kno_scheme_module,"ERROR-CALLER","EXCEPTION-CALLER");
  kno_defalias(kno_scheme_module,"ERROR-DETAILS","EXCEPTION-DETAILS");
  kno_defalias(kno_scheme_module,"ERROR-IRRITANT","EXCEPTION-IRRITANT");
  kno_defalias(kno_scheme_module,"ERROR-BACKTRACE","EXCEPTION-BACKTRACE");
  kno_defalias(kno_scheme_module,"ERROR-SUMMARY","EXCEPTION-SUMMARY");

  kno_defalias(kno_scheme_module,"ERROR-CONTEXT","EXCEPTION-CALLER");
  kno_defalias(kno_scheme_module,"GET-IRRITANT","EXCEPTION-IRRITANT");
  kno_defalias(kno_scheme_module,"ERROR-IRRITANT","EXCEPTION-IRRITANT");
  kno_defalias(kno_scheme_module,"ERROR-XDATA","ERROR-IRRITANT");
  kno_defalias(kno_scheme_module,"ERROR-IRRITANT?","EXCEPTION-IRRITANT?");

  kno_def_evalfn(kno_scheme_module,"DYNAMIC-WIND","",dynamic_wind_evalfn);
  kno_def_evalfn(kno_scheme_module,"UNWIND-PROTECT","",unwind_protect_evalfn);

  kno_idefn1(kno_scheme_module,"STACK-DEPTH",stack_entry_depth,1,
            "Returns the depth of a stack entry",
            kno_compound_type,KNO_VOID);
  kno_idefn1(kno_scheme_module,"STACK-TYPE",stack_entry_type,1,
            "Returns the type of a stack entry",
            kno_compound_type,KNO_VOID);
  kno_idefn1(kno_scheme_module,"STACK-OP",stack_entry_op,1,
            "Returns the op of a stack entry",
            kno_compound_type,KNO_VOID);
  kno_idefn1(kno_scheme_module,"STACK-LABEL",stack_entry_label,1,
            "Returns the label of a stack entry",
            kno_compound_type,KNO_VOID);
  kno_idefn1(kno_scheme_module,"STACK-FILENAME",stack_entry_filename,1,
            "Returns the label of a stack entry",
            kno_compound_type,KNO_VOID);
  kno_idefn1(kno_scheme_module,"STACK-CRUMB",stack_entry_crumb,1,
            "Returns a probably unique integer identifier for the stack frame",
            kno_compound_type,KNO_VOID);
  kno_idefn1(kno_scheme_module,"STACK-ARGS",stack_entry_args,1,
            "Returns the args of a stack entry",
            kno_compound_type,KNO_VOID);
  kno_idefn1(kno_scheme_module,"STACK-SOURCE",stack_entry_source,1,
            "Returns the source of a stack entry",
            kno_compound_type,KNO_VOID);
  kno_idefn1(kno_scheme_module,"STACK-ENV",stack_entry_env,1,
            "Returns the env of a stack entry",
            kno_compound_type,KNO_VOID);
  kno_idefn1(kno_scheme_module,"STACK-STATUS",stack_entry_status,1,
            "Returns the status of a stack entry",
            kno_compound_type,KNO_VOID);

  stack_entry_symbol = kno_intern("_STACK");
  condition_symbol = kno_intern("CONDITION");
  caller_symbol = kno_intern("CALLER");
  timebase_symbol = kno_intern("TIMEBASE");
  moment_symbol = kno_intern("MOMENT");
  thread_symbol = kno_intern("THREAD");
  details_symbol = kno_intern("DETAILS");
  session_symbol = kno_intern("SESSION");
  context_symbol = kno_intern("CONTEXT");
  irritant_symbol = kno_intern("IRRITANT");
  stack_symbol = kno_intern("STACK");
  env_tag = kno_intern("%ENV");
  args_tag = kno_intern("%ARGS");

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
