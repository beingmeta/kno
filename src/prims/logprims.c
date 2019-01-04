/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_PROVIDE_FASTEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/frames.h"
#include "framerd/streams.h"
#include "framerd/dtypeio.h"
#include "framerd/ports.h"
#include "framerd/pprint.h"

#include <libu8/u8streamio.h>
#include <libu8/u8crypto.h>

#include <zlib.h>

static int printout_helper(U8_OUTPUT *out,lispval x)
{
  if (FD_ABORTP(x)) return 0;
  else if (VOIDP(x)) return 1;
  if (out == NULL) out = u8_current_output;
  if (STRINGP(x))
    u8_puts(out,CSTRING(x));
  else fd_unparse(out,x);
  return 1;
}

static u8_output get_output_port(lispval portarg)
{
  if ((VOIDP(portarg))||(FD_TRUEP(portarg)))
    return u8_current_output;
  else if (FD_PORTP(portarg)) {
    struct FD_PORT *p=
      fd_consptr(struct FD_PORT *,portarg,fd_port_type);
    return p->port_output;}
  else return NULL;
}

#define fast_eval(x,env) (fd_stack_eval(x,env,_stack,0))

static lispval message_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval body = fd_get_body(expr,1);
  U8_OUTPUT *out = u8_open_output_string(1024);
  U8_OUTPUT *stream = u8_current_output;
  u8_set_default_output(out);
  while (PAIRP(body)) {
    lispval value = fast_eval(FD_CAR(body),env);
    if (printout_helper(out,value)) fd_decref(value);
    else {
      u8_set_default_output(stream);
      u8_close_output(out);
      return value;}
    body = FD_CDR(body);}
  u8_set_default_output(stream);
  u8_logger(U8_LOG_MSG,NULL,out->u8_outbuf);
  u8_close_output(out);
  return VOID;
}

static lispval notify_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval body = fd_get_body(expr,1);
  U8_OUTPUT *out = u8_open_output_string(1024);
  U8_OUTPUT *stream = u8_current_output;
  u8_set_default_output(out);
  while (PAIRP(body)) {
    lispval value = fast_eval(FD_CAR(body),env);
    if (printout_helper(out,value)) fd_decref(value);
    else {
      u8_set_default_output(stream);
      u8_close_output(out);
      return value;}
    body = FD_CDR(body);}
  u8_set_default_output(stream);
  u8_logger(LOG_NOTICE,NULL,out->u8_outbuf);
  u8_close_output(out);
  return VOID;
}

static lispval status_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval body = fd_get_body(expr,1);
  U8_OUTPUT *out = u8_open_output_string(1024);
  U8_OUTPUT *stream = u8_current_output;
  u8_set_default_output(out);
  while (PAIRP(body)) {
    lispval value = fast_eval(FD_CAR(body),env);
    if (printout_helper(out,value)) fd_decref(value);
    else {
      u8_set_default_output(stream);
      u8_close_output(out);
      return value;}
    body = FD_CDR(body);}
  u8_set_default_output(stream);
  u8_logger(LOG_INFO,NULL,out->u8_outbuf);
  u8_close_output(out);
  return VOID;
}

static lispval warning_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval body = fd_get_body(expr,1);
  U8_OUTPUT *out = u8_open_output_string(1024);
  U8_OUTPUT *stream = u8_current_output;
  u8_set_default_output(out);
  while (PAIRP(body)) {
    lispval value = fast_eval(FD_CAR(body),env);
    if (printout_helper(out,value)) fd_decref(value);
    else {
      u8_set_default_output(stream);
      u8_close_output(out);
      return value;}
    body = FD_CDR(body);}
  u8_set_default_output(stream);
  u8_logger(LOG_WARN,NULL,out->u8_outbuf);
  u8_close_output(out);
  return VOID;
}

static int get_loglevel(lispval level_arg)
{
  if (FD_INTP(level_arg)) return FIX2INT(level_arg);
  else return -1;
}

static lispval log_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval level_arg = fd_eval(fd_get_arg(expr,1),env);
  lispval body = fd_get_body(expr,2);
  int level = get_loglevel(level_arg);
  U8_OUTPUT *out = u8_open_output_string(1024);
  U8_OUTPUT *stream = u8_current_output;
  u8_condition condition = NULL;
  if (FD_THROWP(level_arg))
    return level_arg;
  else if (FD_ABORTP(level_arg)) {
    fd_clear_errors(1);}
  else fd_decref(level_arg);
  u8_set_default_output(out);
  if (PAIRP(body)) {
    lispval cond_expr = FD_CAR(body);
    if (FD_SYMBOLP(cond_expr))
      condition = SYM_NAME(FD_CAR(body));
    else if (FD_EVALP(cond_expr)) {
      lispval condition_name = fd_stack_eval(cond_expr,env,_stack,0);
      if (FD_SYMBOLP(condition_name))
        condition = SYM_NAME(condition_name);
      else if (!(FD_VOIDP(condition_name)))
        fd_unparse(out,condition_name);
      else {}
      fd_decref(condition_name);}
    else {}
    body = FD_CDR(body);}
  while (PAIRP(body)) {
    lispval value = fast_eval(FD_CAR(body),env);
    if (printout_helper(out,value)) fd_decref(value);
    else {
      u8_set_default_output(stream);
      u8_close_output(out);
      return value;}
    body = FD_CDR(body);}
  u8_set_default_output(stream);
  u8_logger(level,condition,out->u8_outbuf);
  u8_close_output(out);
  return VOID;
}

static lispval logif_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval test_expr = fd_get_arg(expr,1), value = FD_FALSE;
  if (FD_ABORTP(test_expr)) return test_expr;
  else if (PRED_FALSE(STRINGP(test_expr)))
    return fd_reterr(fd_SyntaxError,"logif_evalfn",
                     _("LOGIF condition expression cannot be a string"),expr);
  else value = fast_eval(test_expr,env);
  if (FD_ABORTP(value)) return value;
  else if ( (FALSEP(value)) || (VOIDP(value)) ||
            (EMPTYP(value)) || (NILP(value)) )
    return VOID;
  else {
    lispval body = fd_get_body(expr,2);
    U8_OUTPUT *out = u8_open_output_string(1024);
    U8_OUTPUT *stream = u8_current_output;
    u8_condition condition = NULL;
    if (PAIRP(body)) {
      lispval cond_expr = FD_CAR(body);
      if (FD_SYMBOLP(cond_expr))
        condition = SYM_NAME(FD_CAR(body));
      else if (FD_EVALP(cond_expr)) {
        lispval condition_name = fd_stack_eval(cond_expr,env,_stack,0);
        if (FD_SYMBOLP(condition_name))
          condition = SYM_NAME(condition_name);
        else if (!(FD_VOIDP(condition_name)))
          fd_unparse(out,condition_name);
        else {}
        fd_decref(condition_name);}
      else {}
      body = FD_CDR(body);}
    fd_decref(value); u8_set_default_output(out);
    while (PAIRP(body)) {
      lispval value = fast_eval(FD_CAR(body),env);
      if (printout_helper(out,value)) fd_decref(value);
      else {
        u8_set_default_output(stream);
        u8_close_output(out);
        return value;}
      body = FD_CDR(body);}
    u8_set_default_output(stream);
    u8_logger(U8_LOG_MSG,condition,out->u8_outbuf);
    u8_close_output(out);
    return VOID;}
}

static lispval logifplus_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval test_expr = fd_get_arg(expr,1), value = FD_FALSE, loglevel_arg;
  if (FD_ABORTP(test_expr)) return test_expr;
  else if (PRED_FALSE(STRINGP(test_expr)))
    return fd_reterr(fd_SyntaxError,"logif_evalfn",
                     _("LOGIF condition expression cannot be a string"),expr);
  else value = fast_eval(test_expr,env);
  if (FD_ABORTP(value))
    return value;
  else if ((FALSEP(value)) || (VOIDP(value)) ||
           (EMPTYP(value)) || (NILP(value)))
    return VOID;
  fd_decref(value);
  loglevel_arg = fd_eval(fd_get_arg(expr,2),env);
  if (FD_ABORTP(loglevel_arg))
    return loglevel_arg;
  else if (VOIDP(loglevel_arg))
    return fd_reterr(fd_SyntaxError,"logif_plus_evalfn",
                     _("LOGIF+ loglevel invalid"),expr);
  else if (!(FD_FIXNUMP(loglevel_arg)))
    return fd_reterr(fd_TypeError,"logif_plus_evalfn",
                     _("LOGIF+ loglevel invalid"),loglevel_arg);
  else {
    lispval body = fd_get_body(expr,3);
    U8_OUTPUT *out = u8_open_output_string(1024);
    U8_OUTPUT *stream = u8_current_output;
    int priority = FIX2INT(loglevel_arg);
    u8_condition condition = NULL;
    if (PAIRP(body)) {
      lispval cond_expr = FD_CAR(body);
      if (FD_SYMBOLP(cond_expr))
        condition = SYM_NAME(FD_CAR(body));
      else if (FD_EVALP(cond_expr)) {
        lispval condition_name = fd_stack_eval(cond_expr,env,_stack,0);
        if (FD_SYMBOLP(condition_name))
          condition = SYM_NAME(condition_name);
        else if (!(FD_VOIDP(condition_name)))
          fd_unparse(out,condition_name);
        else {}
        fd_decref(condition_name);}
      else {}
      body = FD_CDR(body);}
    u8_set_default_output(out);
    while (PAIRP(body)) {
      lispval value = fast_eval(FD_CAR(body),env);
      if (printout_helper(out,value)) fd_decref(value);
      else {
        u8_set_default_output(stream);
        u8_close_output(out);
        return value;}
      body = FD_CDR(body);}
    u8_set_default_output(stream);
    u8_logger(-priority,condition,out->u8_outbuf);
    u8_close_output(out);
    return VOID;}
}

/* Printing a backtrace */

static u8_exception print_backtrace_entry
(U8_OUTPUT *out,u8_exception ex,int width)
{
  fd_print_exception(out,ex);
  return ex->u8x_prev;
}

static u8_exception log_backtrace_entry(int loglevel,u8_condition label,
                                        u8_exception ex,int width)
{
  struct U8_OUTPUT tmpout; u8_byte buf[16384];
  U8_INIT_OUTPUT_BUF(&tmpout,16384,buf);
  if ((ex->u8x_context) && (ex->u8x_details))
    u8_log(loglevel,ex->u8x_cond,"(%s) %m",(ex->u8x_context),(ex->u8x_details));
  else if (ex->u8x_context)
    u8_log(loglevel,ex->u8x_cond,"(%s)",(ex->u8x_context));
  else if (ex->u8x_details)
    u8_log(loglevel,ex->u8x_cond,"(%m)",(ex->u8x_details));
  else u8_log(loglevel,ex->u8x_cond,_("No more information"));
  return ex->u8x_prev;
}

FD_EXPORT
void fd_print_backtrace(U8_OUTPUT *out,u8_exception ex,int width)
{
  u8_exception scan = ex;
  while (scan) {
    u8_puts(out,";;=======================================================\n");
    scan = print_backtrace_entry(out,scan,width);}
}

FD_EXPORT void fd_log_backtrace(u8_exception ex,int level,u8_condition label,
                                int width)
{
  u8_exception scan = ex;
  while (scan) {
    scan = log_backtrace_entry(level,label,scan,width);}
}

FD_EXPORT
void fd_summarize_backtrace(U8_OUTPUT *out,u8_exception ex)
{
  u8_exception scan = ex; u8_condition cond = NULL;
  while (scan) {
    lispval irritant = fd_exception_xdata(scan); int show_irritant = 1;
    if (scan!=ex) u8_puts(out," <");
    if (scan->u8x_cond!=cond) {
      cond = scan->u8x_cond; u8_printf(out," (%m)",cond);}
    if (scan->u8x_context)
      u8_printf(out," %s",scan->u8x_context);
    if (scan->u8x_details)
      u8_printf(out," [%s]",scan->u8x_details);
    if (show_irritant)
      u8_printf(out," <%q>",irritant);
    scan = scan->u8x_prev;}
}

/* Table showing primitives */

static lispval lisp_show_table(lispval tables,lispval slotids,lispval portarg)
{
  U8_OUTPUT *out = get_output_port(portarg);
  DO_CHOICES(table,tables)
    if ((FALSEP(slotids)) || (VOIDP(slotids)))
      fd_display_table(out,table,VOID);
    else if (OIDP(table)) {
      U8_OUTPUT *tmp = u8_open_output_string(1024);
      u8_printf(out,"%q\n",table);
      {DO_CHOICES(slotid,slotids) {
        lispval values = fd_frame_get(table,slotid);
        tmp->u8_write = tmp->u8_outbuf; *(tmp->u8_outbuf)='\0';
        u8_printf(tmp,"   %q:   %q\n",slotid,values);
        if (u8_strlen(tmp->u8_outbuf)<80) u8_puts(out,tmp->u8_outbuf);
        else {
          u8_printf(out,"   %q:\n",slotid);
          {DO_CHOICES(value,values) u8_printf(out,"      %q\n",value);}}
        fd_decref(values);}}
      u8_close((u8_stream)tmp);}
    else fd_display_table(out,table,slotids);
  u8_flush(out);
  return VOID;
}

/* The init function */

FD_EXPORT void fd_init_logprims_c()
{
  u8_register_source_file(_FILEINFO);

  /* Logging functions for specific levels */
  fd_def_evalfn(fd_scheme_module,"NOTIFY","",notify_evalfn);
  fd_def_evalfn(fd_scheme_module,"STATUS","",status_evalfn);
  fd_def_evalfn(fd_scheme_module,"WARNING","",warning_evalfn);

  /* Generic logging function, always outputs */
  fd_def_evalfn(fd_scheme_module,"MESSAGE","",message_evalfn);
  fd_def_evalfn(fd_scheme_module,"%LOGGER","",message_evalfn);

  /* Logging with message level */
  fd_def_evalfn(fd_scheme_module,"LOGMSG","",log_evalfn);
  /* Conditional logging */
  fd_def_evalfn(fd_scheme_module,"LOGIF","",logif_evalfn);
  /* Conditional logging with priority level */
  fd_def_evalfn(fd_scheme_module,"LOGIF+","",logifplus_evalfn);

  fd_idefn3(fd_scheme_module,"%SHOW",lisp_show_table,1|FD_NDCALL,
            "Shows a table",
            -1,FD_VOID,-1,FD_VOID,-1,FD_VOID);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
