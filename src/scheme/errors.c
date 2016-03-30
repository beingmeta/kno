/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2016 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/support.h"
#include "framerd/eval.h"
#include "framerd/ports.h"

static fd_exception SchemeError=_("Undistinguished Scheme Error");

/* Returning errors */

static fdtype return_error_helper(fdtype expr,fd_lispenv env,int wrapped)
{
  fd_exception ex=SchemeError, cxt=NULL;
  fdtype head=fd_get_arg(expr,0);
  fdtype arg1=fd_get_arg(expr,1);
  fdtype arg2=fd_get_arg(expr,2);
  fdtype printout_body;

  if ((FD_SYMBOLP(arg1)) && (FD_SYMBOLP(arg2))) {
    ex=(fd_exception)(FD_SYMBOL_NAME(arg1));
    cxt=(u8_context)(FD_SYMBOL_NAME(arg2));
    printout_body=fd_get_body(expr,3);}
  else if (FD_SYMBOLP(arg1)) {
    ex=(fd_exception)(FD_SYMBOL_NAME(arg1));
    printout_body=fd_get_body(expr,2);}
  else if (FD_SYMBOLP(head)) {
    ex=(fd_exception)(FD_SYMBOL_NAME(head));
    printout_body=fd_get_body(expr,1);}
  else printout_body=fd_get_body(expr,1);

  {
    U8_OUTPUT out; U8_INIT_OUTPUT(&out,256);
    fd_printout_to(&out,printout_body,env);
    if (wrapped) {
      u8_exception ex=u8_new_exception
	((u8_condition)ex,(u8_context)cxt,out.u8_outbuf,(void *)FD_VOID,NULL);
      return fd_init_exception(NULL,ex);}
    else  {
      fd_seterr(ex,cxt,out.u8_outbuf,FD_VOID);
      return FD_ERROR_VALUE;}}
}
static fdtype return_error(fdtype expr,fd_lispenv env)
{
  return return_error_helper(expr,env,0);
}
static fdtype extend_error(fdtype expr,fd_lispenv env)
{
  return return_error_helper(expr,env,1);
}

static fdtype return_irritant_helper(fdtype expr,fd_lispenv env,int wrapped)
{
  fd_exception ex=SchemeError, cxt=NULL;
  fdtype head=fd_get_arg(expr,0);
  fdtype irritant=fd_get_arg(expr,1);
  fdtype arg1=fd_get_arg(expr,2);
  fdtype arg2=fd_get_arg(expr,3);
  fdtype printout_body;

  if (FD_VOIDP(irritant))
    return fd_err(fd_SyntaxError,"return_irritant","no irritant",expr);
  else if ((FD_SYMBOLP(arg1)) && (FD_SYMBOLP(arg2))) {
    ex=(fd_exception)(FD_SYMBOL_NAME(arg1));
    cxt=(u8_context)(FD_SYMBOL_NAME(arg2));
    printout_body=fd_get_body(expr,4);}
  else if (FD_SYMBOLP(arg1)) {
    ex=(fd_exception)(FD_SYMBOL_NAME(arg1));
    printout_body=fd_get_body(expr,3);}
  else if (FD_SYMBOLP(head)) {
    ex=(fd_exception)(FD_SYMBOL_NAME(head));
    printout_body=fd_get_body(expr,2);}
  else printout_body=fd_get_body(expr,2);

  irritant=fd_eval(irritant,env);

  {
    U8_OUTPUT out; U8_INIT_OUTPUT(&out,256);
    fd_printout_to(&out,printout_body,env);
    if (wrapped) {
      u8_exception u8ex=
	u8_make_exception((u8_condition)ex,(u8_context)cxt,out.u8_outbuf,
			  (void *)irritant,fd_free_exception_xdata);
      return fd_init_exception(NULL,u8ex);}
    else return fd_err(ex,cxt,out.u8_outbuf,irritant);}
}
static fdtype return_irritant(fdtype expr,fd_lispenv env)
{
  return return_irritant_helper(expr,env,0);
}
static fdtype extend_irritant(fdtype expr,fd_lispenv env)
{
  return return_irritant_helper(expr,env,1);
}

static fdtype onerror_handler(fdtype expr,fd_lispenv env)
{
  fdtype toeval=fd_get_arg(expr,1);
  fdtype error_handler=fd_get_arg(expr,2);
  fdtype default_handler=fd_get_arg(expr,3);
  fdtype value=fd_eval(toeval,env);
  if (FD_THROWP(value))
    return value;
  else if (FD_ABORTP(value)) {
    u8_exception ex=u8_erreify();
    fdtype handler=fd_eval(error_handler,env);
    if (FD_ABORTP(handler)) {
      u8_restore_exception(ex);
      return handler;}
    else if (FD_APPLICABLEP(handler)) {
      fdtype err_value=fd_init_exception(NULL,ex);
      fdtype handler_result=fd_apply(handler,1,&err_value);
      fd_exception_object exo=
	FD_GET_CONS(err_value,fd_error_type,fd_exception_object);
      if (handler_result==err_value) {
	u8_restore_exception(ex);
        fd_decref(handler); fd_decref(value); fd_decref(err_value);
        return FD_ERROR_VALUE;}
      else if (FD_PRIM_TYPEP(handler_result,fd_error_type)) {
	fd_exception_object newexo=
	  FD_GET_CONS(handler_result,fd_error_type,fd_exception_object);
	u8_exception newex=newexo->ex;
	u8_push_exception(newex->u8x_cond,newex->u8x_context,
                          newex->u8x_details,newex->u8x_xdata,
                          newex->u8x_free_xdata);
	newexo->ex=NULL;
	exo->ex=NULL;
	u8_restore_exception(ex);
	fd_decref(handler); fd_decref(value); 
	fd_decref(err_value); fd_decref(handler_result);
	return FD_ERROR_VALUE;}
      else if (FD_ABORTP(handler_result)) {
        u8_exception cur_ex=u8_current_exception;
        /* Clear this field so we can decref err_value while leaving
           the exception object current. */
        u8_log(LOG_WARN,"Recursive error",
               "Error handling error during %q",toeval);
        fd_log_backtrace(cur_ex);
        exo->ex=NULL;
        u8_restore_exception(ex);
        fd_decref(handler); fd_decref(value); fd_decref(err_value);
        return handler_result;}
      else {
        fd_decref(value); fd_decref(handler); fd_decref(err_value);
        return handler_result;}}
    else {
      u8_free_exception(ex,1);
      fd_decref(value);
      return handler;}}
  else if (FD_VOIDP(default_handler))
    return value;
  else {
    fdtype handler=fd_eval(default_handler,env);
    if (FD_ABORTP(handler))
      return handler;
    else if (FD_APPLICABLEP(handler)) {
      if (FD_VOIDP(value)) {
        fdtype result=fd_finish_call(fd_dapply(handler,0,&value));
        fd_decref(handler);
        return result;}
      else {
        fdtype result=fd_finish_call(fd_dapply(handler,1,&value));
        fd_decref(handler);
        fd_decref(value);
        return result;}}
    else {
      fd_decref(value);
      return handler;}}
}

static fdtype report_errors_handler(fdtype expr,fd_lispenv env)
{
  fdtype toeval=fd_get_arg(expr,1);
  fdtype value=fd_eval(toeval,env);
  if (FD_THROWP(value))
    return value;
  else if (FD_ABORTP(value)) {
    u8_exception ex=u8_current_exception;
    fd_log_backtrace(ex);
    fd_clear_errors(0);
    return FD_FALSE;}
  else return value;
}

static fdtype erreify_handler(fdtype expr,fd_lispenv env)
{
  fdtype toeval=fd_get_arg(expr,1);
  fdtype value=fd_eval(toeval,env);
  if (FD_THROWP(value))
    return value;
  else if (FD_ABORTP(value)) {
    u8_exception ex=u8_erreify();
    return fd_init_exception(NULL,ex);}
  else return value;
}

static fdtype error_condition(fdtype x,fdtype top_arg)
{
  int top=(!(FD_FALSEP(top_arg)));
  struct FD_EXCEPTION_OBJECT *xo=
    FD_GET_CONS(x,fd_error_type,struct FD_EXCEPTION_OBJECT *);
  u8_exception ex=xo->ex;
  u8_condition found=ex->u8x_cond;
  if (top) {
    while (ex) {
      if (ex->u8x_cond) {found=ex->u8x_cond; break;}
      else ex=ex->u8x_prev;}}
  else while (ex) {
      if (ex->u8x_cond) found=ex->u8x_cond;
      ex=ex->u8x_prev;}
  if (found==NULL) return FD_FALSE;
  else return fd_intern((u8_string)found);
}

static fdtype error_context(fdtype x,fdtype top_arg)
{
  int top=(!(FD_FALSEP(top_arg)));
  struct FD_EXCEPTION_OBJECT *xo=
    FD_GET_CONS(x,fd_error_type,struct FD_EXCEPTION_OBJECT *);
  u8_exception ex=xo->ex;
  u8_context found=ex->u8x_cond;
  if (top) {
    while (ex) {
      if (ex->u8x_context) {found=ex->u8x_context; break;}
      else ex=ex->u8x_prev;}}
  else while (ex) {
      if (ex->u8x_context) found=ex->u8x_context;
      ex=ex->u8x_prev;}
  if (found==NULL) return FD_FALSE;
  else return fd_intern((u8_string)found);
}

static fdtype error_details(fdtype x,fdtype top_arg)
{
  int top=(!(FD_FALSEP(top_arg)));
  struct FD_EXCEPTION_OBJECT *xo=
    FD_GET_CONS(x,fd_error_type,struct FD_EXCEPTION_OBJECT *);
  u8_exception ex=xo->ex;
  u8_string found=ex->u8x_details;
  if (top) {
    while (ex) {
      if (ex->u8x_details) {found=ex->u8x_details; break;}
      else ex=ex->u8x_prev;}}
  else while (ex) {
      if (ex->u8x_details) found=ex->u8x_details;
      ex=ex->u8x_prev;}
  if (found==NULL) return FD_FALSE;
  else return fdtype_string(found);
}

static fdtype error_irritant(fdtype x,fdtype top_arg)
{
  int top=(!(FD_FALSEP(top_arg)));
  struct FD_EXCEPTION_OBJECT *xo=
    FD_GET_CONS(x,fd_error_type,struct FD_EXCEPTION_OBJECT *);
  u8_exception ex=xo->ex;
  fdtype found=FD_VOID;
  if (top) {
    while (ex) {
      if (ex->u8x_xdata) {
        found=fd_exception_xdata(ex); break;}
      else ex=ex->u8x_prev;}}
  else while (ex) {
      if  (ex->u8x_xdata)
        found=fd_exception_xdata(ex);
      ex=ex->u8x_prev;}
  if (FD_VOIDP(found)) return FD_VOID;
  else return fd_incref(found);
}

static fdtype error_has_irritant(fdtype x,fdtype top_arg)
{
  int top=(!(FD_FALSEP(top_arg)));
  struct FD_EXCEPTION_OBJECT *xo=
    FD_GET_CONS(x,fd_error_type,struct FD_EXCEPTION_OBJECT *);
  u8_exception ex=xo->ex;
  fdtype found=FD_VOID;
  if (top) {
    while (ex) {
      if (ex->u8x_xdata) {
        found=fd_exception_xdata(ex); break;}
      else ex=ex->u8x_prev;}}
  else while (ex) {
      if  (ex->u8x_xdata)
        found=fd_exception_xdata(ex);
      ex=ex->u8x_prev;}
  if (FD_VOIDP(found)) return FD_FALSE;
  else return FD_TRUE;
}

static fdtype error_backtrace(fdtype x)
{
  struct FD_EXCEPTION_OBJECT *xo=
    FD_GET_CONS(x,fd_error_type,struct FD_EXCEPTION_OBJECT *);
  u8_exception ex=xo->ex;
  return fd_exception_backtrace(ex);
}

static fdtype error_xdata(fdtype x)
{
  struct FD_EXCEPTION_OBJECT *xo=
    FD_GET_CONS(x,fd_error_type,struct FD_EXCEPTION_OBJECT *);
  u8_exception ex=xo->ex;
  fdtype xdata=fd_exception_xdata(ex);
  if (FD_VOIDP(xdata)) return FD_FALSE;
  else return fd_incref(xdata);
}

static int thunkp(fdtype x)
{
  if (!(FD_APPLICABLEP(x))) return 0;
  else {
    struct FD_FUNCTION *f=(fd_function)x;
    if (f->arity==0) return 1;
    else if ((f->arity<0) && (f->min_arity==0)) return 1;
    else return 0;}
}

static u8_condition UnwindError=_("Unwind error");

static fdtype dynamic_wind_handler(fdtype expr,fd_lispenv env)
{
  fdtype wind=fd_get_arg(expr,1), doit=fd_get_arg(expr,2), unwind=fd_get_arg(expr,3);
  if ((FD_VOIDP(wind)) || (FD_VOIDP(doit)) || (FD_VOIDP(unwind)))
    return fd_err(fd_SyntaxError,"dynamic_wind_handler",NULL,expr);
  else {
    wind=fd_eval(wind,env);
    if (FD_ABORTP(wind)) return wind;
    doit=fd_eval(doit,env);
    if (FD_ABORTP(doit)) {
      fd_decref(wind);
      return doit;}
    unwind=fd_eval(unwind,env);
    if (FD_ABORTP(unwind)) {
      fd_decref(wind); fd_decref(doit);
      return unwind;}
    if (!(thunkp(wind)))
      return fd_type_error("thunk","dynamic_wind_handler",wind);
    else if (!(thunkp(doit)))
      return fd_type_error("thunk","dynamic_wind_handler",doit);
    else if (!(thunkp(unwind)))
      return fd_type_error("thunk","dynamic_wind_handler",unwind);
    else {
      fdtype windval=fd_apply(wind,0,NULL);
      if (FD_ABORTP(windval)) {
        fd_decref(wind); fd_decref(doit); fd_decref(unwind);
        return windval;}
      else {
        fdtype retval=fd_apply(doit,0,NULL);
        u8_exception saved=u8_erreify();
        fdtype unwindval=fd_apply(unwind,0,NULL);
        u8_restore_exception(saved);
        fd_decref(windval);
        fd_decref(wind); fd_decref(doit); fd_decref(unwind);
        if (FD_ABORTP(unwindval))
          u8_log(LOG_WARN,UnwindError,"DYNAMIC-WIND: %q",unwindval);
        fd_decref(unwindval);
        return retval;}}}
}

static fdtype unwind_protect_handler(fdtype uwp,fd_lispenv env)
{
  fdtype heart=fd_get_arg(uwp,1);
  fdtype result=fd_eval(heart,env);
  {FD_DOBODY(expr,uwp,2) {
      fdtype uw_result=fd_eval(expr,env);
      if (FD_ABORTP(uw_result))
        if (FD_ABORTP(result)) {
          fd_interr(result); fd_interr(uw_result);
          return FD_ERROR_VALUE;}
        else {
          fd_decref(result); result=uw_result; break;}
      else fd_decref(uw_result);}}
  return result;
}

/* Clear errors */

static fdtype clear_errors()
{
  int n_errs=fd_clear_errors(1);
  if (n_errs) return FD_INT(n_errs);
  else return FD_FALSE;
}

FD_EXPORT void fd_init_errors_c()
{
  u8_register_source_file(_FILEINFO);

  fd_defspecial(fd_scheme_module,"ERROR",return_error);
  fd_defspecial(fd_scheme_module,"ERROR+",extend_error);
  fd_defspecial(fd_scheme_module,"IRRITANT",return_irritant);
  fd_defspecial(fd_scheme_module,"IRRITANT+",extend_irritant);
  fd_defspecial(fd_scheme_module,"ONERROR",onerror_handler);
  fd_defspecial(fd_scheme_module,"REPORT-ERRORS",report_errors_handler);
  fd_defspecial(fd_scheme_module,"ERREIFY",erreify_handler);

  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("ERROR-CONDITION",error_condition,1,
                           fd_error_type,FD_VOID,-1,FD_FALSE));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("ERROR-CONTEXT",error_context,1,
                           fd_error_type,FD_VOID,-1,FD_FALSE));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("ERROR-DETAILS",error_details,1,
                           fd_error_type,FD_VOID,-1,FD_FALSE));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("ERROR-IRRITANT",error_irritant,1,
                           fd_error_type,FD_VOID,-1,FD_FALSE));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("ERROR-IRRITANT?",error_has_irritant,1,
                           fd_error_type,FD_VOID,-1,FD_FALSE));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("ERROR-XDATA",error_xdata,1,
                           fd_error_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("ERROR-BACKTRACE",error_backtrace,1,
                           fd_error_type,FD_VOID));

  fd_defspecial(fd_scheme_module,"DYNAMIC-WIND",dynamic_wind_handler);
  fd_defspecial(fd_scheme_module,"UNWIND-PROTECT",unwind_protect_handler);

  fd_idefn(fd_scheme_module,
           fd_make_cprim0("CLEAR-ERRORS!",clear_errors,0));

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
