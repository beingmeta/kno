/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/eval.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/frames.h"
#include "kno/streams.h"
#include "kno/dtypeio.h"
#include "kno/ports.h"
#include "kno/pprint.h"
#include "kno/cprims.h"

#include <libu8/u8streamio.h>
#include <libu8/u8crypto.h>

#include <zlib.h>


static int printout_helper(U8_OUTPUT *out,lispval x)
{
  if (KNO_ABORTP(x)) return 0;
  else if (VOIDP(x)) return 1;
  if (out == NULL) out = u8_current_output;
  if (STRINGP(x))
    u8_puts(out,CSTRING(x));
  else kno_unparse(out,x);
  return 1;
}

static u8_output get_output_port(lispval portarg)
{
  if ((VOIDP(portarg))||(KNO_TRUEP(portarg)))
    return u8_current_output;
  else if (KNO_PORTP(portarg)) {
    struct KNO_PORT *p=
      kno_consptr(struct KNO_PORT *,portarg,kno_ioport_type);
    return p->port_output;}
  else return NULL;
}

static lispval log_helper_evalfn(int loglevel,u8_condition condition,
				 lispval body,
				 kno_lexenv env,
				 kno_stack stack)
{
  U8_OUTPUT *out = u8_open_output_string(1024);
  U8_OUTPUT *stream = u8_current_output;
  u8_set_default_output(out);
  while (PAIRP(body)) {
    lispval value = kno_eval(KNO_CAR(body),env,stack);
    if (printout_helper(out,value)) kno_decref(value);
    else {
      u8_set_default_output(stream);
      u8_close_output(out);
      return value;}
    body = KNO_CDR(body);}
  u8_set_default_output(stream);
  u8_logger(loglevel,condition,out->u8_outbuf);
  u8_close_output(out);
  return VOID;
}

DEFC_EVALFN("message",message_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(message *printout args...*)` generates a log message "
	    "with output specified by *printout args...*. This "
	    "message will not be controlled by loglevels but will "
	    "be handled by logging functions.")
static lispval message_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  return log_helper_evalfn(U8_LOG_MSG,NULL,kno_get_body(expr,1),env,_stack);
}


DEFC_EVALFN("notify",notify_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(notify *printout args...*)` generates a log message "
	    "with output specified by *printout args...*. This "
	    "message will have logging priority NOTICE.")
static lispval notify_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  return log_helper_evalfn(LOG_NOTIFY,NULL,kno_get_body(expr,1),env,_stack);
}

DEFC_EVALFN("status",status_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(status *printout args...*)` generates a log message "
	    "with output specified by *printout args...*. This "
	    "message will have logging priority INFO.")
static lispval status_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  return log_helper_evalfn(LOG_INFO,NULL,kno_get_body(expr,1),env,_stack);
}

DEFC_EVALFN("warning",warning_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(status *printout args...*)` generates a log message "
	    "with output specified by *printout args...*. This "
	    "message will have logging priority WARNING.")
static lispval warning_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  return log_helper_evalfn(LOG_WARN,NULL,kno_get_body(expr,1),env,_stack);
}

static int get_loglevel(lispval level_arg)
{
  if (KNO_INTP(level_arg)) return FIX2INT(level_arg);
  else return -1;
}

DEFC_EVALFN("log",log_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(log *priority* *condition* *printout args...*)` generates a "
	    "log message with priority *priority* (evaluated) and "
	    "condition name *condition*, which is either a symbol "
	    "(not evaluated) or anything else (evaluated)")
static lispval log_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval level_arg = kno_eval(kno_get_arg(expr,1),env,_stack);
  lispval body = kno_get_body(expr,2);
  int level = get_loglevel(level_arg);
  U8_OUTPUT *out = u8_open_output_string(1024);
  U8_OUTPUT *stream = u8_current_output;
  u8_condition condition = NULL;
  if (KNO_THROWP(level_arg))
    return level_arg;
  else if (KNO_ABORTP(level_arg)) {
    kno_clear_errors(1);}
  else kno_decref(level_arg);
  u8_set_default_output(out);
  if (PAIRP(body)) {
    lispval cond_expr = KNO_CAR(body);
    if (KNO_SYMBOLP(cond_expr))
      condition = SYM_NAME(KNO_CAR(body));
    else if (KNO_EVALP(cond_expr)) {
      lispval condition_name = kno_eval(cond_expr,env,_stack);
      if (KNO_ABORTED(condition_name)) {
	u8_close_output(out);
	return condition_name;}
      else if (KNO_SYMBOLP(condition_name))
	condition = SYM_NAME(condition_name);
      else if (!(KNO_VOIDP(condition_name)))
        kno_unparse(out,condition_name);
      else {}
      kno_decref(condition_name);}
    else NO_ELSE;
    body = KNO_CDR(body);}
  while (PAIRP(body)) {
    lispval value = kno_eval(KNO_CAR(body),env,_stack);
    if (printout_helper(out,value)) kno_decref(value);
    else {
      u8_set_default_output(stream);
      u8_close_output(out);
      return value;}
    body = KNO_CDR(body);}
  u8_set_default_output(stream);
  u8_logger(level,condition,out->u8_outbuf);
  u8_close_output(out);
  return VOID;
}

#if 0
DEFC_EVALFN("logif",logif_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(logif *test* *condition* *printout args...*)` generates a "
	    "log message if *test* evaluates to anything but {} or #f.")
static lispval logif_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval test_expr = kno_get_arg(expr,1), value = KNO_FALSE;
  if (KNO_ABORTP(test_expr)) return test_expr;
  else if (RARELY(STRINGP(test_expr)))
    return kno_reterr(kno_SyntaxError,"logif_evalfn",
                      _("LOGIF condition expression cannot be a string"),expr);
  else value = kno_eval(test_expr,env,_stack);
  if (KNO_ABORTP(value)) return value;
  else if ( (FALSEP(value)) || (VOIDP(value)) ||
            (EMPTYP(value)) || (NILP(value)) )
    return VOID;
  else {
    lispval body = kno_get_body(expr,2);
    U8_OUTPUT *out = u8_open_output_string(1024);
    U8_OUTPUT *stream = u8_current_output;
    u8_condition condition = NULL;
    if (PAIRP(body)) {
      lispval cond_expr = KNO_CAR(body);
      if (KNO_SYMBOLP(cond_expr))
        condition = SYM_NAME(KNO_CAR(body));
      else if (KNO_EVALP(cond_expr)) {
	lispval condition_name = kno_eval(cond_expr,env,_stack);
        if (KNO_SYMBOLP(condition_name))
          condition = SYM_NAME(condition_name);
        else if (!(KNO_VOIDP(condition_name)))
          kno_unparse(out,condition_name);
        else {}
        kno_decref(condition_name);}
      else {}
      body = KNO_CDR(body);}
    kno_decref(value);
    u8_set_default_output(out);
    while (PAIRP(body)) {
      lispval value = kno_eval(KNO_CAR(body),env,_stack);
      if (printout_helper(out,value)) kno_decref(value);
      else {
        u8_set_default_output(stream);
        u8_close_output(out);
        return value;}
      body = KNO_CDR(body);}
    u8_set_default_output(stream);
    u8_logger(U8_LOG_MSG,condition,out->u8_outbuf);
    u8_close_output(out);
    return VOID;}
}
#endif

DEFC_EVALFN("logif",logif_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(logif *test* *priority* *condition* *printout args...*)` generates a "
	    "log message")
static lispval logif_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval test_expr = kno_get_arg(expr,1), value = KNO_FALSE, loglevel_arg;
  if (KNO_ABORTP(test_expr)) return test_expr;
  else if (RARELY(STRINGP(test_expr)))
    return kno_reterr(kno_SyntaxError,"logif_evalfn",
                      _("LOGIF+ condition expression cannot be a string"),expr);
  else value = kno_eval(test_expr,env,_stack);
  if (KNO_ABORTP(value))
    return value;
  else if ((FALSEP(value)) || (VOIDP(value)) ||
           (EMPTYP(value)) || (NILP(value)))
    return VOID;
  kno_decref(value);
  loglevel_arg = kno_eval(kno_get_arg(expr,2),env,_stack);
  if (KNO_ABORTP(loglevel_arg))
    return loglevel_arg;
  else if (VOIDP(loglevel_arg))
    return kno_reterr(kno_SyntaxError,"logif_plus_evalfn",
                      _("LOGIF+ loglevel invalid"),expr);
  else if (!(KNO_FIXNUMP(loglevel_arg)))
    return kno_reterr(kno_TypeError,"logif_plus_evalfn",
                      _("LOGIF+ loglevel invalid"),loglevel_arg);
  else {
    lispval body = kno_get_body(expr,3);
    U8_OUTPUT *out = u8_open_output_string(1024);
    U8_OUTPUT *stream = u8_current_output;
    int priority = FIX2INT(loglevel_arg);
    u8_condition condition = NULL;
    if (PAIRP(body)) {
      lispval cond_expr = KNO_CAR(body);
      if (KNO_SYMBOLP(cond_expr))
        condition = SYM_NAME(KNO_CAR(body));
      else if (KNO_EVALP(cond_expr)) {
	lispval condition_name = kno_eval(cond_expr,env,_stack);
        if (KNO_SYMBOLP(condition_name))
          condition = SYM_NAME(condition_name);
        else if (!(KNO_VOIDP(condition_name)))
          kno_unparse(out,condition_name);
        else {}
        kno_decref(condition_name);}
      else {}
      body = KNO_CDR(body);}
    u8_set_default_output(out);
    while (PAIRP(body)) {
      lispval value = kno_eval(KNO_CAR(body),env,_stack);
      if (printout_helper(out,value)) kno_decref(value);
      else {
        u8_set_default_output(stream);
        u8_close_output(out);
        return value;}
      body = KNO_CDR(body);}
    u8_set_default_output(stream);
    u8_logger(-priority,condition,out->u8_outbuf);
    u8_close_output(out);
    return VOID;}
}

/* Logging with the stack */

DEFC_EVALFN("logstack",logstack_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(logstack *level* *condition* *printout args...*)` generates a "
	    "log message")
static lispval logstack_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval level_arg = kno_eval(kno_get_arg(expr,1),env,_stack);
  lispval body = kno_get_body(expr,2);
  int level = get_loglevel(level_arg);
  U8_OUTPUT *out = u8_open_output_string(1024);
  U8_OUTPUT *stream = u8_current_output;
  u8_condition condition = NULL;
  if (KNO_THROWP(level_arg))
    return level_arg;
  else if (KNO_ABORTP(level_arg)) {
    kno_clear_errors(1);}
  else kno_decref(level_arg);
  u8_set_default_output(out);
  if (PAIRP(body)) {
    lispval cond_expr = KNO_CAR(body);
    if (KNO_SYMBOLP(cond_expr))
      condition = SYM_NAME(KNO_CAR(body));
    else if (KNO_EVALP(cond_expr)) {
      lispval condition_name = kno_eval(cond_expr,env,_stack);
      if (KNO_SYMBOLP(condition_name))
        condition = SYM_NAME(condition_name);
      else if (!(KNO_VOIDP(condition_name)))
        kno_unparse(out,condition_name);
      else {}
      kno_decref(condition_name);}
    else {}
    body = KNO_CDR(body);}
  while (PAIRP(body)) {
    lispval value = kno_eval(KNO_CAR(body),env,_stack);
    if (printout_helper(out,value)) kno_decref(value);
    else {
      u8_set_default_output(stream);
      u8_close_output(out);
      return value;}
    body = KNO_CDR(body);}
  u8_set_default_output(stream);
  u8_logger(level,condition,out->u8_outbuf);
  u8_close_output(out);
  knodbg_log_stack(level,condition,1);
  return VOID;
}

/* Printing a backtrace */

static u8_exception print_backtrace_entry
(U8_OUTPUT *out,u8_exception ex,int width)
{
  kno_print_exception(out,ex);
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

KNO_EXPORT
void kno_print_backtrace(U8_OUTPUT *out,u8_exception ex,int width)
{
  u8_exception scan = ex;
  while (scan) {
    u8_puts(out,";;=======================================================\n");
    scan = print_backtrace_entry(out,scan,width);}
}

KNO_EXPORT void kno_log_backtrace(u8_exception ex,int level,u8_condition label,
                                  int width)
{
  u8_exception scan = ex;
  while (scan) {
    scan = log_backtrace_entry(level,label,scan,width);}
}

KNO_EXPORT
void kno_summarize_backtrace(U8_OUTPUT *out,u8_exception ex)
{
  u8_exception scan = ex; u8_condition cond = NULL;
  while (scan) {
    lispval irritant = kno_exception_xdata(scan); int show_irritant = 1;
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

DEFC_PRIM("%show",lisp_show_table,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "Shows a table",
	  {"tables",kno_any_type,KNO_VOID},
	  {"slotids",kno_any_type,KNO_VOID},
	  {"portarg",kno_any_type,KNO_VOID})
static lispval lisp_show_table(lispval tables,lispval slotids,lispval portarg)
{
  U8_OUTPUT *out = get_output_port(portarg);
  DO_CHOICES(table,tables)
    if ((FALSEP(slotids)) || (VOIDP(slotids)))
      kno_display_table(out,table,VOID);
    else if (OIDP(table)) {
      U8_OUTPUT *tmp = u8_open_output_string(1024);
      u8_printf(out,"%q\n",table);
      {DO_CHOICES(slotid,slotids) {
          lispval values = kno_frame_get(table,slotid);
          tmp->u8_write = tmp->u8_outbuf; *(tmp->u8_outbuf)='\0';
          u8_printf(tmp,"   %q:   %q\n",slotid,values);
          if (u8_strlen(tmp->u8_outbuf)<80) u8_puts(out,tmp->u8_outbuf);
          else {
            u8_printf(out,"   %q:\n",slotid);
            {DO_CHOICES(value,values) u8_printf(out,"      %q\n",value);}}
          kno_decref(values);}}
      u8_close((u8_stream)tmp);}
    else kno_display_table(out,table,slotids);
  u8_flush(out);
  return VOID;
}

/* The init function */

KNO_EXPORT void kno_init_logprims_c()
{
  u8_register_source_file(_FILEINFO);

  KNO_LINK_EVALFN(kno_sys_module,message_evalfn);
  KNO_LINK_EVALFN(kno_sys_module,notify_evalfn);
  KNO_LINK_EVALFN(kno_sys_module,status_evalfn);
  KNO_LINK_EVALFN(kno_sys_module,warning_evalfn);

  /* Generic logging function, always outputs */
  KNO_LINK_EVALFN(kno_sys_module,message_evalfn);
  KNO_LINK_EVALFN(kno_sys_module,message_evalfn);
  KNO_LINK_EVALFN(kno_sys_module,log_evalfn);
  KNO_LINK_EVALFN(kno_sys_module,logif_evalfn);
  KNO_LINK_EVALFN(kno_sys_module,logstack_evalfn);

  KNO_LINK_ALIAS("logif+",logif_evalfn,kno_sys_module);

  link_local_cprims();
}



static void link_local_cprims(){
  KNO_LINK_CPRIM("%show",lisp_show_table,3,kno_sys_module);
}
