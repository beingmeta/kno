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
#include "framerd/support.h"
#include "framerd/exceptions.h"
#include "framerd/eval.h"
#include "framerd/ports.h"

static u8_condition SchemeError=_("Undistinguished Scheme Error");

/* Returning errors */

static lispval return_error_helper(lispval expr,fd_lexenv env,int wrapped)
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
    if (wrapped) {
      u8_exception sub_ex = u8_new_exception
	((u8_condition)ex,(u8_context)cxt,out.u8_outbuf,(void *)VOID,NULL);
      u8_close_output(&out);
      return fd_wrap_exception(sub_ex);}
    else  {
      fd_seterr(ex,cxt,out.u8_outbuf,VOID);
      u8_close_output(&out);
      return FD_ERROR;}}
}
static lispval return_error_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  return return_error_helper(expr,env,0);
}
static lispval extend_error_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  return return_error_helper(expr,env,1);
}

static lispval return_irritant_helper(lispval expr,fd_lexenv env,
                                      int wrapped,int eval_args)
{
  u8_condition ex = SchemeError, cxt = NULL;
  lispval head = fd_get_arg(expr,0);
  lispval irritant_expr = fd_get_arg(expr,1), irritant=VOID;
  lispval arg1 = fd_get_arg(expr,2);
  lispval arg2 = fd_get_arg(expr,3);
  lispval printout_body;

  if (eval_args) {
    lispval exval = fd_eval(arg1,env), cxtval;
    if (FD_ABORTP(exval))
      ex = (u8_condition)"Recursive error on exception name";
    else if (SYMBOLP(exval))
      ex = (u8_condition)(SYM_NAME(exval));
    else if (STRINGP(exval)) {
      lispval sym = fd_intern(CSTRING(exval));
      ex = (u8_condition)(SYM_NAME(sym));}
    else ex = (u8_condition)"Bad exception condition";
    cxtval = fd_eval(arg2,env);
    if ((FALSEP(cxtval))||(EMPTYP(cxtval)))
      cxt = (u8_condition)NULL;
    else if (FD_ABORTP(cxtval))
      cxt = (u8_context)"Recursive error on exception context";
    else if (SYMBOLP(cxtval))
      cxt = (u8_context)(SYM_NAME(cxtval));
    else if (STRINGP(exval)) {
      lispval sym = fd_intern(CSTRING(cxtval));
      cxt = (u8_context)(SYM_NAME(sym));}
    else cxt = (u8_context)"Bad exception condition";
    printout_body = fd_get_body(expr,4);}
  else if ((SYMBOLP(arg1)) && (SYMBOLP(arg2))) {
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
      u8_log(LOGCRIT,"RecursiveError",
             "Error %s (%s:%s) evaluating irritant arg %q",
             ex->u8x_cond,ex->u8x_context,ex->u8x_details,
             irritant_expr);
    else u8_log(LOGCRIT,"RecursiveError",
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
        struct FD_FUNCTION *fcn=(fd_function)op;
        if (fcn->fcn_name)
          cxt=fcn->fcn_name;
        else if (fcn->fcn_documentation)
          cxt=fcn->fcn_documentation;
        else cxt="FUNCTIONCALL";}
      else {}
      cur=cur->stack_caller;}}

  {
    u8_string details=NULL;
    U8_OUTPUT out; U8_INIT_OUTPUT(&out,256);
    fd_printout_to(&out,printout_body,env);
    if (out.u8_outbuf == out.u8_write) {
      u8_close_output(&out);
      details=NULL;}
    else details=out.u8_outbuf;
    if (wrapped) {
      u8_exception u8ex=
        u8_make_exception((u8_condition)ex,(u8_context)cxt,details,
                          (void *)irritant,fd_decref_u8x_xdata);
      return fd_wrap_exception(u8ex);}
    else {
      lispval err_result=fd_err(ex,cxt,details,irritant);
      fd_decref(irritant);
      if (details) u8_close_output(&out);
      return err_result;}}
}
static lispval return_irritant_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  return return_irritant_helper(expr,env,0,0);
}
static lispval extend_irritant_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  return return_irritant_helper(expr,env,1,0);
}

static lispval onerror_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval toeval = fd_get_arg(expr,1);
  lispval error_handler = fd_get_arg(expr,2);
  lispval default_handler = fd_get_arg(expr,3);
  lispval value = fd_eval(toeval,env);
  if (FD_THROWP(value))
    return value;
  else if (FD_ABORTP(value)) {
    u8_exception ex = u8_erreify();
    lispval handler = fd_eval(error_handler,env);
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
        u8_log(LOG_WARN,"Recursive error",
               "Error %m handling error during %q",
               handler_ex->u8x_cond,toeval);
        fd_log_backtrace(handler_ex,LOGWARN,"RecursiveError",128);
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
        return handler_result;}}
    else {
      u8_free_exception(ex,1);
      fd_decref(value);
      return handler;}}
  else if (VOIDP(default_handler))
    return value;
  else {
    lispval handler = fd_eval(default_handler,env);
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

static lispval report_errors_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval toeval = fd_get_arg(expr,1);
  lispval value = fd_eval(toeval,env);
  if (FD_THROWP(value))
    return value;
  else if (FD_ABORTP(value)) {
    u8_exception ex = u8_current_exception;
    fd_log_backtrace(ex,LOG_NOTICE,"CaughtError",128);
    fd_clear_errors(0);
    return FD_FALSE;}
  else return value;
}

static lispval erreify_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval toeval = fd_get_arg(expr,1);
  lispval value = fd_eval(toeval,env);
  if (FD_THROWP(value))
    return value;
  else if (FD_ABORTP(value)) {
    u8_exception ex = u8_erreify();
    return fd_wrap_exception(ex);}
  else return value;
}

static lispval error_condition(lispval x)
{
  struct FD_EXCEPTION *xo=
    fd_consptr(struct FD_EXCEPTION *,x,fd_exception_type);
  if (xo->ex_condition)
    return fd_intern(xo->ex_condition);
  else return FD_FALSE;
}

static lispval error_caller(lispval x)
{
  struct FD_EXCEPTION *xo=
    fd_consptr(struct FD_EXCEPTION *,x,fd_exception_type);
  if (xo->ex_caller)
    return fd_intern(xo->ex_caller);
  else return FD_FALSE;
}

static lispval error_details(lispval x)
{
  struct FD_EXCEPTION *xo=
    fd_consptr(struct FD_EXCEPTION *,x,fd_exception_type);
  if (xo->ex_details)
    return lispval_string(xo->ex_details);
  else return FD_FALSE;
}

static lispval error_irritant(lispval x)
{
  struct FD_EXCEPTION *xo=
    fd_consptr(struct FD_EXCEPTION *,x,fd_exception_type);
  return fd_incref(xo->ex_irritant);
}

static lispval error_has_irritant(lispval x)
{
  struct FD_EXCEPTION *xo=
    fd_consptr(struct FD_EXCEPTION *,x,fd_exception_type);
  if (FD_VOIDP(xo->ex_irritant))
    return FD_FALSE;
  else return FD_TRUE;
}

static lispval error_backtrace(lispval x)
{
  struct FD_EXCEPTION *xo=
    fd_consptr(struct FD_EXCEPTION *,x,fd_exception_type);
  if (FD_VOIDP(xo->ex_stack))
    return FD_FALSE;
  else return fd_incref(xo->ex_stack);
}

static lispval error_summary(lispval x,lispval with_irritant)
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
    struct FD_FUNCTION *f = (fd_function)x;
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
    wind = fd_eval(wind,env);
    if (FD_ABORTP(wind)) return wind;
    else if (!(thunkp(wind)))
      return fd_type_error("thunk","dynamic_wind_evalfn",wind);
    else doit = fd_eval(doit,env);
    if (FD_ABORTP(doit)) {
      fd_decref(wind);
      return doit;}
    else if (!(thunkp(doit))) {
      fd_decref(wind);
      return fd_type_error("thunk","dynamic_wind_evalfn",doit);}
    else unwind = fd_eval(unwind,env);
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
      result = fd_eval(heart,env);
    U8_ON_EXCEPTION {
      U8_CLEAR_CONTOUR();
      result = FD_ERROR;}
    U8_END_EXCEPTION;}
  {U8_WITH_CONTOUR("UNWIND-PROTECT(unwind)",0)
      {lispval unwinds = fd_get_body(uwp,2);
        FD_DOLIST(expr,unwinds) {
          lispval uw_result = fd_eval(expr,env);
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

/* Getting an irritant without a stacktrace */

static lispval get_irritant_prim(lispval exo)
{
  struct FD_EXCEPTION *xo=
    fd_consptr(struct FD_EXCEPTION *,exo,fd_exception_type);
  return fd_incref(xo->ex_irritant);
}

/* Clear errors */

static lispval clear_errors()
{
  int n_errs = fd_clear_errors(1);
  if (n_errs) return FD_INT(n_errs);
  else return FD_FALSE;
}

FD_EXPORT void fd_init_errors_c()
{
  u8_register_source_file(_FILEINFO);

  fd_def_evalfn(fd_scheme_module,"ERROR","",return_error_evalfn);
  fd_def_evalfn(fd_scheme_module,"IRRITANT","",return_irritant_evalfn);
  fd_def_evalfn(fd_scheme_module,"NEWERR","",return_irritant_evalfn);
  fd_def_evalfn(fd_scheme_module,"ONERROR","",onerror_evalfn);
  fd_def_evalfn(fd_scheme_module,"REPORT-ERRORS","",report_errors_evalfn);
  fd_def_evalfn(fd_scheme_module,"ERREIFY","",erreify_evalfn);

  fd_idefn1(fd_scheme_module,"RERAISE",reraise_prim,1,
            "Reraises the exception represented by an object",
            fd_exception_type,FD_VOID);
  fd_idefn1(fd_scheme_module,"ERROR-CONDITION",error_condition,1,
            "Returns the condition name (a symbol) for an error",
            fd_exception_type,VOID);
  fd_idefn1(fd_scheme_module,"ERROR-CALLER",error_caller,1,
            "Returns the immediate context (caller, a symbol) for an error",
            fd_exception_type,VOID);
  fd_idefn1(fd_scheme_module,"ERROR-DETAILS",error_details,1,
            "Returns any descriptive details (a string) for the error",
            fd_exception_type,VOID);
  fd_idefn1(fd_scheme_module,"ERROR-IRRITANT",error_irritant,1,
            "Returns the LISP object (if any) caused the error",
            fd_exception_type,VOID);

  fd_defalias(fd_scheme_module,"ERROR-CONTEXT","ERROR-CALLER");
  fd_defalias(fd_scheme_module,"GET-IRRITANT","ERROR-IRRITANT");
  fd_defalias(fd_scheme_module,"ERROR-XDATA","ERROR-IRRITANT");

  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("ERROR-IRRITANT?",error_has_irritant,1,
                           fd_exception_type,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("ERROR-SUMMARY",error_summary,1,
                           fd_exception_type,VOID,-1,FD_FALSE));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("ERROR-BACKTRACE",error_backtrace,1,
                           fd_exception_type,VOID));

  fd_def_evalfn(fd_scheme_module,"DYNAMIC-WIND","",dynamic_wind_evalfn);
  fd_def_evalfn(fd_scheme_module,"UNWIND-PROTECT","",unwind_protect_evalfn);

  fd_idefn(fd_scheme_module,
           fd_make_cprim0("CLEAR-ERRORS!",clear_errors));

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
