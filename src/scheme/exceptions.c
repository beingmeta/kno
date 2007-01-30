/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "fdb/dtype.h"
#include "fdb/support.h"
#include "fdb/eval.h"

/* Returning errors */

static fdtype return_error(fdtype expr,fd_lispenv env)
{
  if (!(FD_SYMBOLP(fd_get_arg(expr,1)))) 
    return fd_type_error("symbol",NULL,fd_get_arg(expr,1));
  else if (!(FD_SYMBOLP(fd_get_arg(expr,2))))
    return fd_type_error("symbol",NULL,fd_get_arg(expr,2));
  else {
    U8_OUTPUT out; U8_INIT_OUTPUT(&out,256);
    fd_printout_to(&out,fd_get_body(expr,4),env);
    return fd_err(FD_SYMBOL_NAME(fd_get_arg(expr,1)),
		  FD_SYMBOL_NAME(fd_get_arg(expr,2)),
		  out.u8_outbuf,
		  fd_incref(fd_get_arg(expr,3)));}
}

static fdtype raise_handler(fdtype expr,fd_lispenv env)
{
  fdtype to_eval=fd_get_arg(expr,1);
  if (FD_VOIDP(to_eval))
    return fd_err(fd_SyntaxError,"raise_handler",NULL,expr);
  else {
    fdtype value=fd_eval(to_eval,env);
    if (FD_ERRORP(value)) return value;
    else if (FD_EXCEPTIONP(value)) {
      FD_SET_CONS_TYPE(value,fd_error_type);
      return value;}
    else if (FD_TROUBLEP(value)) 
      return fd_err(fd_retcode_to_exception(value),NULL,NULL,to_eval);
    else return fd_type_error("raise_handler","an error",value);}
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
    fdtype handler=fd_eval(error_handler,env);
    if (FD_ABORTP(handler))
      return fd_passerr(handler,fd_passerr(value,FD_EMPTY_LIST));
    else if (FD_APPLICABLEP(handler)) {
      struct FD_FUNCTION *f=(fd_function)handler;
      fdtype err_result;
      if (FD_ERRORP(value)) {
	FD_SET_CONS_TYPE(value,fd_exception_type);}
      else {
	value=fd_err(fd_retcode_to_exception(value),NULL,NULL,FD_VOID);}
      err_result=fd_apply(handler,1,&value);
      fd_decref(value); fd_decref(handler);
      return err_result;}
    else {
      fd_decref(value);
      return handler;}}
  else if (FD_VOIDP(default_handler))
    return value;
  else {
    fdtype handler=fd_eval(default_handler,env);
    if (FD_ABORTP(handler))
      return fd_passerr(handler,fd_passerr(value,FD_EMPTY_LIST));
    else if (FD_APPLICABLEP(handler)) {
      fdtype result=fd_dapply(handler,1,&value);
      fd_decref(value);
      return result;}
    else {
      fd_decref(value);
      return handler;}}
}

static fdtype exception_condition(fdtype x)
{
  struct FD_EXCEPTION_OBJECT *xo=
    FD_GET_CONS(x,fd_exception_type,struct FD_EXCEPTION_OBJECT *);
  if (xo->data.cond==NULL) return FD_FALSE;
  else return fdtype_string((u8_string)xo->data.cond);
}

static fdtype exception_context(fdtype x)
{
  struct FD_EXCEPTION_OBJECT *xo=
    FD_GET_CONS(x,fd_exception_type,struct FD_EXCEPTION_OBJECT *);
  if (xo->data.cxt==NULL) return FD_FALSE;
  else return fdtype_string((u8_string)xo->data.cxt);
}

static fdtype exception_details(fdtype x)
{
  struct FD_EXCEPTION_OBJECT *xo=
    FD_GET_CONS(x,fd_exception_type,struct FD_EXCEPTION_OBJECT *);
  if (xo->data.cond==NULL) return FD_FALSE;
  else return fdtype_string(xo->data.details);
}

static fdtype exception_irritant(fdtype x)
{
  struct FD_EXCEPTION_OBJECT *xo=
    FD_GET_CONS(x,fd_exception_type,struct FD_EXCEPTION_OBJECT *);
  if (FD_VOIDP(xo->data.irritant)) return FD_FALSE;
  else return fd_incref(xo->data.irritant);
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
	fdtype unwindval=fd_apply(unwind,0,NULL);
	fd_decref(windval);
	fd_decref(wind); fd_decref(doit); fd_decref(unwind);
	if (FD_ABORTP(unwindval))
	  u8_warn(UnwindError,"DYNAMIC-WIND: %q",unwindval);
	fd_decref(unwindval);
	return retval;}}}
}

FD_EXPORT void fd_init_exceptions_c()
{
  fd_register_source_file(versionid);
  
  fd_defspecial(fd_scheme_module,"ERROR",return_error);
  fd_defspecial(fd_scheme_module,"ONERROR",onerror_handler);
  fd_defspecial(fd_scheme_module,"RAISE",raise_handler);
  fd_idefn(fd_scheme_module,fd_make_cprim1x("EXCEPTION-CONDITION",exception_condition,1,fd_exception_type,FD_VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim1x("EXCEPTION-CONTEXT",exception_condition,1,fd_exception_type,FD_VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim1x("EXCEPTION-DETAILS",exception_condition,1,fd_exception_type,FD_VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim1x("EXCEPTION-IRRITANT",exception_condition,1,fd_exception_type,FD_VOID));

  fd_defspecial(fd_scheme_module,"DYNAMIC-WIND",dynamic_wind_handler);
}
