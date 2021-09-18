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


static u8_output open_helper_stream(ssize_t size,u8_output dest)
{
  U8_OUTPUT *out = u8_open_output_string(size);
  out->u8_streaminfo |=
    ( U8_HUMAN_OUTPUT | ((dest->u8_streaminfo)&(U8_SUB_STREAM_MASK)) );
  return out;
}

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

static u8_context get_log_context(kno_stack stack)
{
  kno_stack scan = stack;
  while (scan) {
    if ((scan->stack_bits)&(KNO_STACK_LAMBDA_CALL)) break;
    else scan=scan->stack_caller;}
  if (scan==NULL) return NULL;
  lispval op = scan->stack_op;
  if (KNO_FUNCTIONP(op)) {
    struct KNO_FUNCTION *f = KNO_FUNCTION_INFO(op);
    lispval module = f->fcn_moduleid;
    u8_string name = f->fcn_name;
    if ( (name) && (KNO_SYMBOLP(module)) )
      return u8_mkstring("%s:%s",KNO_SYMBOL_NAME(module),name);
    else if (KNO_SYMBOLP(module))
      return u8_strdup(KNO_SYMBOL_NAME(module));
    else if ( (f->fcn_filename) && (name) )
      return u8_mkstring("%s:%s",f->fcn_filename,name);
    else if (name)
      return u8_strdup(name);
    else if (f->fcn_filename)
      return u8_mkstring("%s:lambda",f->fcn_filename);
    else return NULL;}
  else return NULL;
}

static lispval log_printout(lispval port,lispval body,kno_lexenv env,kno_stack stack)
{
  lispval result = KNO_VOID;
  U8_STATIC_OUTPUT(msg,1024);
  if (u8_logprefix) u8_puts(msgout,u8_logprefix);
  u8_byte _prefix[512];
  u8_string prefix = u8_message_prefix(_prefix,sizeof(_prefix));
  if (prefix) u8_puts(msgout,prefix);
  u8_output outer_output = u8_current_output;
  u8_set_default_output(msgout);
  while (PAIRP(body)) {
    lispval value = kno_eval(KNO_CAR(body),env,stack);
    if (printout_helper(msgout,value)) {
      kno_decref(value);}
    else {
      if (u8_logsuffix) u8_puts(msgout,u8_logsuffix);
      result=value;
      break;}
    body = KNO_CDR(body);}
  if (u8_logsuffix) u8_puts(msgout,u8_logsuffix);
  int close_real = 0;
  u8_output real = u8_current_output;
  if (KNO_PORTP(port)) {
    struct KNO_PORT *p=
      kno_consptr(struct KNO_PORT *,port,kno_ioport_type);
    if (p->port_output) real = p->port_output;}
  else if (KNO_STRINGP(port)) {
    /* Open file locked and in append mode */}
  else NO_ELSE;
  u8_set_default_output(outer_output);
  u8_putn(real,msg.u8_outbuf,msg.u8_write-msg.u8_outbuf);
  u8_close_output(msgout);
  if (close_real) u8_close_output(real);
  return result;
}

static lispval log_helper_evalfn(int loglevel,u8_condition condition,
				 lispval body,
				 kno_lexenv env,
				 kno_stack stack)
{
  U8_OUTPUT *stream = u8_current_output;
  U8_OUTPUT *out = open_helper_stream(1024,stream);
  u8_set_default_output(out);
  while (PAIRP(body)) {
    lispval value = kno_eval(KNO_CAR(body),env,stack);
    if (printout_helper(out,value)) kno_decref(value);
    else {
      u8_set_default_output(stream);
      u8_close_output(out);
      return value;}
    body = KNO_CDR(body);}
  u8_string temp_condition = (condition)?(NULL):(get_log_context(stack));
  u8_set_default_output(stream);
  if (condition)
    u8_logger(loglevel,condition,out->u8_outbuf);
  else u8_logger(loglevel,temp_condition,out->u8_outbuf);
  u8_close_output(out);
  if (temp_condition) u8_free(temp_condition);
  return VOID;
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

DEFC_EVALFN("logerror",logerror_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(logerror *printout args...*)` generates a log message "
	    "with output specified by *printout args...*. This "
	    "message will have logging priority ERROR.")
static lispval logerror_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  return log_helper_evalfn(LOG_WARN,NULL,kno_get_body(expr,1),env,_stack);
}

static int get_loglevel(lispval level_arg)
{
  if (KNO_INTP(level_arg)) return FIX2INT(level_arg);
  else return -1;
}

DEFC_EVALFN("logmsg",log_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(logmsg *priority* *condition* *printout args...*)` generates a "
	    "log message with priority *priority* (evaluated) and "
	    "condition name *condition*, which is either a symbol "
	    "(not evaluated) or anything else (evaluated)")
static lispval log_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval level_arg = kno_eval(kno_get_arg(expr,1),env,_stack);
  lispval body = kno_get_body(expr,2);
  int level = get_loglevel(level_arg);
  U8_OUTPUT *stream = u8_current_output;
  U8_OUTPUT *out = open_helper_stream(1024,stream);
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
    U8_OUTPUT *stream = u8_current_output;
    U8_OUTPUT *out = open_helper_stream(1024,stream);
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

/* Message and logging to stdout */

DEFC_EVALFN("message",message_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(message *printout args...*)` generates a log message "
	    "with output specified by *printout args...*. This "
	    "message will not be controlled by loglevels but will "
	    "be handled by logging functions.")
static lispval message_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  return log_printout(KNO_VOID,kno_get_body(expr,1),env,_stack);
}

DEFC_EVALFN("logprint",logprint_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(logprint *port* *printout args...*)` generates a log message "
	    "to *port")
static lispval logprint_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval port_arg = kno_get_arg(expr,1);
  lispval port_val = kno_eval(port_arg,env,_stack);
  lispval result = log_printout(port_val,kno_get_body(expr,2),env,_stack);
  kno_incref(port_val);
  return result;
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
  U8_OUTPUT *stream = u8_current_output;
  U8_OUTPUT *out = open_helper_stream(1024,stream);
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
      U8_OUTPUT *tmp = open_helper_stream(1024,out);
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
  KNO_LINK_EVALFN(kno_sys_module,logprint_evalfn);
  KNO_LINK_EVALFN(kno_sys_module,notify_evalfn);
  KNO_LINK_EVALFN(kno_sys_module,status_evalfn);
  KNO_LINK_EVALFN(kno_sys_module,warning_evalfn);
  KNO_LINK_EVALFN(kno_sys_module,logerror_evalfn);

  /* Generic logging function, always outputs */
  KNO_LINK_EVALFN(kno_sys_module,message_evalfn);
  KNO_LINK_EVALFN(kno_sys_module,log_evalfn);
  KNO_LINK_EVALFN(kno_sys_module,logif_evalfn);
  KNO_LINK_EVALFN(kno_sys_module,logstack_evalfn);

  KNO_LINK_ALIAS("%logger",message_evalfn,kno_sys_module);
  KNO_LINK_ALIAS("logif+",logif_evalfn,kno_sys_module);

  link_local_cprims();
}



static void link_local_cprims(){
  KNO_LINK_CPRIM("%show",lisp_show_table,3,kno_sys_module);
}
