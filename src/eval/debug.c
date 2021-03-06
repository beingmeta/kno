/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_CHOICES (!(KNO_AVOID_INLINE))
#define KNO_INLINE_TABLES  (!(KNO_AVOID_INLINE))
#define KNO_INLINE_FCNIDS  (!(KNO_AVOID_INLINE))
#define KNO_INLINE_STACKS  (!(KNO_AVOID_INLINE))
#define KNO_INLINE_LEXENV  (!(KNO_AVOID_INLINE))

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/support.h"
#include "kno/storage.h"
#include "kno/eval.h"
#include "kno/dtproc.h"
#include "kno/numbers.h"
#include "kno/sequences.h"
#include "kno/ports.h"
#include "kno/dtcall.h"
#include "kno/ffi.h"
#include "kno/cprims.h"

#include <libu8/u8timefns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8printf.h>
#include <libu8/u8stdio.h>

#include <math.h>
#include <pthread.h>
#include <errno.h>

#if HAVE_MTRACE && HAVE_MCHECK_H
#include <mcheck.h>
#endif

static char *cpu_profilename = NULL;
u8_string kno_bugdir = NULL;

static lispval unquote_symbol, else_symbol, apply_marker, logcxt_symbol;

/* Trace functions */

DEFC_EVALFN("TIMEVAL",timed_eval_evalfn,KNO_EVALFN_DEFAULTS,
	    "evaluates *expr* in *env*, logging "
	    "the execution time.");
static lispval timed_eval_evalfn(lispval expr,kno_lexenv env,kno_stack stack)
{
  lispval toeval = kno_get_arg(expr,1);
  double start = u8_elapsed_time();
  lispval value = kno_eval(toeval,env,stack);
  double finish = u8_elapsed_time();
  u8_message(";; Executed %q in %f seconds, yielding\n;;\t%q",
	     toeval,(finish-start),value);
  return value;
}

DEFC_EVALFN("%TIMEVAL",timed_evalx_evalfn,KNO_EVALFN_DEFAULTS,
	    "evaluates *expr* in *env*, "
	    "returning a vector of the result and the execution time. "
	    "If *expr* signals an error, the description exception object "
	    "is used as the value");
static lispval timed_evalx_evalfn(lispval expr,kno_lexenv env,kno_stack stack)
{
  lispval toeval = kno_get_arg(expr,1);
  double start = u8_elapsed_time();
  lispval value = kno_eval(toeval,env,stack);
  double finish = u8_elapsed_time();
  if (KNO_ABORTED(value)) {
    u8_exception ex = u8_erreify();
    lispval exception = kno_wrap_exception(ex);
    return kno_make_nvector(2,exception,kno_init_double(NULL,finish-start));}
  else return kno_make_nvector(2,value,kno_init_double(NULL,finish-start));
}

/* Take a stream and the an offset. Inserts a \n\t line break at the offset
   if the current output line is longer than max_len. Returns the new offset.
   We use offsets because we may realloc the stream's buffer during output. */
static int check_line_length(u8_output out,int last_off,int max_len)
{
  u8_byte *start = out->u8_outbuf, *end = out->u8_write, *scanner = end;
  int scan_off, line_len, len = end-start;
  /* Find the beginning of the current line: */
  while ((scanner>start)&&((*scanner)!='\n')) scanner--;
  line_len = end-scanner; scan_off = scanner-start;
  /* If the current line is less than max_len, return the current offset */
  if (line_len<max_len)
    return end-start;
  else if (last_off<=0)
    /* If we don't have an offset for insertion, just return the new
       offset. */
    return end-start;
  else {
    /* TODO: abstract out u8_need_len, abstract out whole "if longer
       than" logic */
    /* The line is too long, insert a \n\t at last_off */
    if ((end+5)>(out->u8_outlim)) {
      /* Grow the stream if needed */
      if (u8_grow_stream((u8_stream)out,U8_BUF_MIN_GROW)<=0)
	return -1;
      start = out->u8_outbuf; end = out->u8_write;
      scanner = start+scan_off;}
    /* Use memmove because it's overlapping */
    memmove(start+last_off+2,start+last_off,len-last_off);
    start[last_off]='\n'; start[last_off+1]='\t';
    out->u8_write = out->u8_write+2;
    start[len+2]='\0';
    return out->u8_write-out->u8_outbuf;}
}

static lispval watchcall(lispval expr,kno_lexenv env,
			 kno_stack _stack,
			 int with_proc)
{
  struct U8_OUTPUT out;
  u8_string dflt_label="%CALL", label = dflt_label, arglabel="%ARG";
  lispval watch, head = kno_get_arg(expr,1), *rail, result = EMPTY;
  int i = 0, n_args, expr_len = kno_seq_length(expr);
  if (VOIDP(head))
    return kno_err(kno_SyntaxError,"%WATCHCALL",NULL,expr);
  else if (STRINGP(head)) {
    if (expr_len==2)
      return kno_err(kno_SyntaxError,"%WATCHCALL",NULL,expr);
    else {
      label = u8_strdup(CSTRING(head));
      arglabel = u8_mkstring("%s/ARG",CSTRING(head));
      if ((expr_len>3)||(KNO_APPLICABLEP(kno_get_arg(expr,2)))) {
	watch = kno_get_body(expr,2);}
      else watch = kno_get_arg(expr,2);}}
  else if ((expr_len==2)&&(PAIRP(head)))
    watch = head;
  else watch = kno_get_body(expr,1);
  n_args = kno_seq_length(watch);
  rail = u8_alloc_n(n_args,lispval);
  U8_INIT_OUTPUT(&out,1024);
  u8_printf(&out,"Watched call %q",watch);
  u8_logger(U8_LOG_MSG,label,out.u8_outbuf);
  out.u8_write = out.u8_outbuf;
  while (i<n_args) {
    lispval arg = kno_get_arg(watch,i);
    lispval val = kno_eval(arg,env,_stack);
    val = kno_simplify_value(val);
    if (KNO_ABORTED(val)) {
      u8_string errstring = kno_errstring(NULL);
      i--; while (i>=0) {kno_decref(rail[i]); i--;}
      u8_free(rail);
      u8_printf(&out,"\t%q !!!> %s",arg,errstring);
      u8_logger(U8_LOG_MSG,arglabel,out.u8_outbuf);
      if (label!=dflt_label) {u8_free(label); u8_free(arglabel);}
      u8_free(out.u8_outbuf); u8_free(errstring);
      return val;}
    if ( (i==0) && (with_proc==0) && (SYMBOLP(arg)) ) {}
    else if (KNO_EVALP(arg)) {
      u8_printf(&out,"%q ==> %q",arg,val);
      u8_logger(U8_LOG_MSG,arglabel,out.u8_outbuf);
      out.u8_write = out.u8_outbuf;}
    else {
      u8_printf(&out,"%q",arg);
      u8_logger(U8_LOG_MSG,arglabel,out.u8_outbuf);
      out.u8_write = out.u8_outbuf;}
    rail[i++]=val;}
  if (CHOICEP(rail[0])) {
    DO_CHOICES(fn,rail[0]) {
      lispval r = kno_apply(fn,n_args-1,rail+1);
      if (KNO_ABORTED(r)) {
	u8_string errstring = kno_errstring(NULL);
	i--; while (i>=0) {kno_decref(rail[i]); i--;}
	u8_free(rail);
	kno_decref(result);
	if (label!=dflt_label) {u8_free(label); u8_free(arglabel);}
	u8_free(out.u8_outbuf); u8_free(errstring);
	return r;}
      else {CHOICE_ADD(result,r);}}}
  else result = kno_apply(rail[0],n_args-1,rail+1);
  if (KNO_ABORTED(result)) {
    u8_string errstring = kno_errstring(NULL);
    u8_printf(&out,"%q !!!> %s",watch,errstring);
    u8_free(errstring);}
  else u8_printf(&out,"%q ===> %q",watch,result);
  u8_logger(U8_LOG_MSG,label,out.u8_outbuf);
  i--; while (i>=0) {kno_decref(rail[i]); i--;}
  u8_free(rail);
  if (label!=dflt_label) {u8_free(label); u8_free(arglabel);}
  u8_free(out.u8_outbuf);
  return kno_simplify_value(result);
}

DEFC_EVALFN("%wc",watchcall_evalfn,KNO_EVALFN_DEFAULTS,
	    "applies *fn* to *args*, logging "
	    "each *arg* (and its value) as well as the final result of "
	    "applying *fn*.");
static lispval watchcall_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  return watchcall(expr,env,_stack,0);
}
DEFC_EVALFN("%wc+",watchcall_plus_evalfn,KNO_EVALFN_DEFAULTS,
	    "applies *fn* to *args*, logging "
	    "each *arg* (and its value) as well as the final result of "
	    "applying *fn*. This also displays the expression for *fn* "
	    "and the resulting function *objects* which are applied.");
static lispval watchcall_plus_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  return watchcall(expr,env,_stack,1);
}

static u8_string get_label(lispval arg,u8_byte *buf,size_t buflen)
{
  if (SYMBOLP(arg))
    return SYM_NAME(arg);
  else if (STRINGP(arg))
    return CSTRING(arg);
  else if (FIXNUMP(arg))
    return u8_write_long_long((FIX2INT(arg)),buf,buflen);
  else if ((KNO_BIGINTP(arg))&&(kno_modest_bigintp((kno_bigint)arg)))
    return u8_write_long_long
      (kno_bigint2int64((kno_bigint)arg),buf,buflen);
  else return NULL;
}

static void log_ptr(lispval val,lispval label_arg,lispval expr)
{
  u8_byte buf[64];
  u8_string label = get_label(label_arg,buf,64);
  if (KNO_IMMEDIATEP(val)) {
    unsigned long long itype = KNO_IMMEDIATE_TYPE(val);
    unsigned long long data = KNO_IMMEDIATE_DATA(val);
    u8_string type_name = kno_type2name(itype);;
    u8_log(U8_LOG_MSG,"Pointer/Immediate",
	   "%s%s%s%p [ T0x%llx(%s) data=%llu ] %q <= %q",
	   U8OPTSTR("",label,": "),val,itype,type_name,data,val,expr);}
  else if (FIXNUMP(val))
    u8_log(U8_LOG_MSG,"Pointer/Fixnum",
	   "%s%s%sFixnum %p == %q = %q",
	   U8OPTSTR("",label,": "),val,val,expr);
  else if (OIDP(val)) {
    KNO_OID addr = KNO_OID_ADDR(val);
    u8_log(U8_LOG_MSG,"Pointer/OID",
	   "%s%s%s%p [ [%llx]+%llx ] @%llx/%llx %q <= %q",
	   U8OPTSTR("",label,": "),val,
	   (unsigned long long)(KNO_OID_BASE_ID(val)),
	   (unsigned long long)(KNO_OID_BASE_OFFSET(val)),
	   (unsigned long long)(KNO_OID_HI(addr)),
	   (unsigned long long)(KNO_OID_LO(addr)),
	   val,expr);}
  else if (KNO_STATICP(val)) {
    kno_lisp_type ptype = KNO_CONS_TYPEOF((kno_cons)val);
    u8_string type_name = kno_lisp_typename(ptype);
    u8_log(U8_LOG_MSG,"Pointer/Static",
	   "%s%s%s%p [ T0x%llx(%s) ] %q <= %q",
	   U8OPTSTR("",label,": "),val,ptype,type_name,val,expr);}
  else if (CONSP(val)) {
    kno_cons c = (kno_cons) val;
    kno_lisp_type ptype = KNO_CONS_TYPEOF(c);
    u8_string type_name = kno_lisp_typename(ptype);
    unsigned int refcount = KNO_CONS_REFCOUNT(c);
    int choicep = KNO_CHOICEP(val);
    u8_log(U8_LOG_MSG,"Pointer/Consed",
	   "%s%s%s%s%p [ T0x%x(%s) refs=%d ] %q <= %q",
	   U8OPTSTR("",label,": "),
	   val,
	   (choicep)?("-16"):(""),
	   ptype,type_name,refcount,
	   val,expr);}
  else {}
}

DEFC_EVALFN("%watchptr",watchptr_evalfn,KNO_EVALFN_DEFAULTS,
	    "evaluates *arg* and describes the "
	    "resulting pointer value.");
static lispval watchptr_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval val_expr = kno_get_arg(expr,1);
  lispval value = kno_eval(val_expr,env,_stack);
  log_ptr(value,KNO_VOID,val_expr);
  return value;
}

DEFC_EVALFN("%watchrefs",watchrefs_evalfn,KNO_EVALFN_DEFAULTS,
	    "evaluates *exprs...* "
	    "reporting any changes to the reference count data for the "
	    "result of evaluating *ptrval*.");
static lispval watchrefs_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval val_expr = kno_get_arg(expr,1);
  lispval name_spec = kno_get_arg(expr,2);
  u8_string label = (KNO_SYMBOLP(name_spec)) ? (SYMBOL_NAME(name_spec)) :
    (KNO_STRINGP(name_spec)) ? (KNO_CSTRING(name_spec)) : (NULL);
  lispval body = (label) ? (kno_get_body(expr,3)) : (kno_get_body(expr,2));
  lispval value = kno_eval(val_expr,env,_stack);
  if (KNO_CONSP(value)) {
    int refcount = KNO_CONS_REFCOUNT((kno_cons)value);
    if (refcount == 0)
      u8_log(U8_LOGWARN,"%WATCHREFS","Not a cons: %q",value);
    else {
      lispval result = kno_eval_body(body,env,_stack,"%WATCHREFS",label,0);
      int after = KNO_CONS_REFCOUNT((kno_cons)value);
      if (refcount != after) {
	if (after == 0)
	  u8_log(U8_LOGWARN,"%WATCHREFS",
		 "STATIC %d => 0%s%s %q\n    %q",
		 refcount-1,U8IF(label," "),U8S(label),
		 value,val_expr);
	else if (after == 0)
	  u8_log(U8_LOGWARN,"%WATCHREFS",
		 "FREED %d => 0%s%s %q=>\n    %q",
		 refcount-1,
		 U8IF(label," "),U8S(label),
		 value,val_expr);
	else u8_log(U8_LOGWARN,"%WATCHREFS",
		    "%s%d (%d => %d)%s%s %q\n	 %q",
		    (after>refcount)?("+"):(""),
		    (after-refcount),
		    refcount-1,after-1,
		    U8IF(label," "),U8S(label),
		    value,val_expr);
	kno_decref(value);
	return result;}
      else {
	kno_decref(value);
	return result;}}}
  else u8_log(U8_LOGWARN,"%WATCHREFS","Not a cons: %q",value);
  return kno_eval_body(body,env,_stack,"%WATCHREFS",label,0);
}

DEFC_PRIM("%watchptrval",watchptr_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "Describes the Kno pointer characteristics of "
	  "*ptrval*, using a label string dervied from "
	  "*label* if provided",
	  {"val",kno_any_type,KNO_VOID},
	  {"label_arg",kno_any_type,KNO_VOID})
static lispval watchptr_prim(lispval val,lispval label_arg)
{
  log_ptr(val,label_arg,KNO_VOID);
  return kno_incref(val);
}

DEFC_EVALFN("%watch",watch_evalfn,KNO_EVALFN_NOTAIL,
	    "logs information from the  current environment. Unless the "
	    "first argument is a literal string, it is evaluated and "
	    "both the expression and result are logged. Any remaining "
	    "arguments are either expressions (which are displayed "
	    "as *expr*=*value* or a literal *valname* string followed by "
	    "an expression to be evaulated. The value is displayed as "
	    "*valname*=*result*. If *valname* starts with a newline "
	    "character, the display starts on a new line and the value "
	    "is displayed using `LISTDATA`.");
static lispval watch_evalfn(lispval expr,kno_lexenv env,kno_stack stack)
{
  lispval toeval = kno_get_arg(expr,1);
  double start; int oneout = 0;
  lispval scan = KNO_CDR(expr);
  u8_string label="%WATCH";
  if ((PAIRP(toeval))) {
    /* EXPR "label" . watchexprs */
    scan = KNO_CDR(scan);
    if ((PAIRP(scan)) && (STRINGP(KNO_CAR(scan)))) {
      label = CSTRING(KNO_CAR(scan)); scan = KNO_CDR(scan);}}
  else if (STRINGP(toeval)) {
    /* "label" . watchexprs, no expr, call should be side-effect */
    label = CSTRING(toeval); scan = KNO_CDR(scan);}
  else if (SYMBOLP(toeval)) {
    /* If the first argument is a symbol, we change the label and
       treat all the arguments as context variables and output
       them.  */
    label="%WATCHED";}
  else scan = KNO_CDR(scan);
  if (PAIRP(scan)) {
    int off = 0; struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,256);
    if (PAIRP(toeval)) {
      u8_printf(&out,"Context for %q: ",toeval);
      off = check_line_length(&out,off,50);}
    /* A watched expr can be just a symbol or pair, which is output as:
       <expr>=<value>
       or a series of "<label>" <expr>, which is output as:
       label=<value> */
    while (PAIRP(scan)) {
      /* A watched expr can be just a symbol or pair, which is output as:
	 <expr>=<value>
	 or a series of "<label>" <expr>, which is output as:
	 label=<value> */
      lispval towatch = KNO_CAR(scan), wval = VOID;
      if ((STRINGP(towatch)) && (KNO_STRLEN(towatch) > 32)) {
	u8_printf(&out," %s ",KNO_CSTRING(towatch));
	scan = KNO_CDR(scan);}
      else if ((STRINGP(towatch)) && (PAIRP(KNO_CDR(scan)))) {
	lispval label = towatch; u8_string lbl = CSTRING(label);
	towatch = KNO_CAR(KNO_CDR(scan)); scan = KNO_CDR(KNO_CDR(scan));
	wval = ((SYMBOLP(towatch))?(kno_symeval(towatch,env)):
		(kno_eval(towatch,env,stack)));
	if (KNO_ABORTED(wval)) {
	  u8_exception ex = u8_erreify();
	  wval = kno_wrap_exception(ex);}
	if (lbl[0]=='\n') {
	  if (KNO_EMPTYP(wval)) {
	    if (oneout) u8_puts(&out," // "); else oneout = 1;
	    u8_printf(&out," %s={}",lbl+1);}
	  else {
	    if (off > 2) {
	      u8_printf(&out,"\n ");
	      off=1;}
	    if (oneout) u8_puts(&out," // "); else oneout = 1;
	    if ( (KNO_PAIRP(wval)) || (KNO_VECTORP(wval)) || (KNO_CHOICEP(wval)) ||
		 (KNO_SLOTMAPP(wval)) || (KNO_SCHEMAPP(wval)) ) {
	      kno_list_object(&out,wval,lbl+1,NULL,"  ",NULL,100,0);
	      if (PAIRP(scan)) {
		u8_puts(&out,"\n ");}}
	    else {
	      u8_printf(&out,"%s=%q",lbl+1,wval);
	      if (PAIRP(scan)) {
		u8_puts(&out,"\n ");}}}}
	else {
	  if (oneout) u8_puts(&out," // "); else oneout = 1;
	  u8_printf(&out,"%s=%q",CSTRING(label),wval);}
	kno_decref(wval); wval = VOID;}
      else {
	wval = ((SYMBOLP(towatch))?(kno_symeval(towatch,env)):
		(kno_eval(towatch,env,stack)));
	if (KNO_ABORTED(wval)) {
	  u8_exception ex = u8_erreify();
	  wval = kno_wrap_exception(ex);}
	scan = KNO_CDR(scan);
	if (oneout)
	  u8_printf(&out," // %q=%q",towatch,wval);
	else {
	  u8_printf(&out,"%q=%q",towatch,wval);
	  oneout = 1;}
	kno_decref(wval);
	wval = VOID;}
      off = check_line_length(&out,off,80);}
    u8_logger(U8_LOG_MSG,label,out.u8_outbuf);
    u8_free(out.u8_outbuf);}
  start = u8_elapsed_time();
  if (SYMBOLP(toeval))
    return kno_eval(toeval,env,stack);
  else if (STRINGP(toeval)) {
    /* This is probably the 'side effect' form of %WATCH, so
       we don't bother 'timing' it. */
    return kno_incref(toeval);}
  else {
    lispval value = kno_eval_arg(toeval,env,stack);
    double howlong = u8_elapsed_time()-start;
    if (KNO_ABORTED(value)) {
      u8_exception ex = u8_erreify();
      value = kno_wrap_exception(ex);}
    if (howlong>1.0)
      u8_log(U8_LOG_MSG,label,"<%.3fs> %q => %q",howlong*1000,toeval,value);
    else if (howlong>0.001)
      u8_log(U8_LOG_MSG,label,"<%.3fms> %q => %q",howlong*1000,toeval,value);
    else if (remainder(howlong,0.0000001)>=0.001)
      u8_log(U8_LOG_MSG,label,"<%.3fus> %q => %q",howlong*1000000,toeval,value);
    else u8_log(U8_LOG_MSG,label,"<%.1fus> %q => %q",howlong*1000000,
		toeval,value);
    return value;}
}

DEFC_EVALFN("%watchcond",watched_cond_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(%WATCHCOND ((*test* *consequents...)) *more clauses...*) "
	    "executes a `COND` form but identifies which branch was taken.");
static lispval watched_cond_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval label_arg = KNO_VOID;
  u8_string label="%WCOND";
  lispval clauses = KNO_CDR(expr);
  if (KNO_STRINGP(KNO_CAR(clauses))) {
    label = KNO_CSTRING(label);
    clauses = KNO_CDR(clauses);}
  else if ( (KNO_PAIRP(KNO_CAR(clauses))) &&
	    (KNO_PAIRP(KNO_CDR(KNO_CAR(clauses)))) &&
	    (KNO_EQ(KNO_CAR(KNO_CAR(clauses)),unquote_symbol))) {
    lispval label_expr = KNO_CADR(KNO_CAR(clauses));
    clauses = KNO_CDR(clauses);
    label_arg = kno_eval(label_expr,env,_stack);
    if (KNO_ABORTP(label_arg)) {
      u8_exception ex = u8_erreify();
      u8_string errstring = kno_errstring(ex);
      u8_log(LOG_ERR,"BadWatchLabel",
	     "Watch label '%q' returned an error (%s)",
	     label_expr,errstring);
      u8_free(errstring);
      u8_free_exception(ex,0);
      label_arg = KNO_VOID;}
    else if (KNO_STRINGP(label_arg)) {
      int len = KNO_STRLEN(label_arg);
      u8_byte *tmplabel = alloca(len+1);
      strcpy(tmplabel,KNO_CSTRING(label_arg));
      label = tmplabel;}
    else u8_log(LOG_ERR,"BadWatchLabel",
		"Watch label '%q' value %q wasn't a string",
		label_expr,label_arg);
    kno_decref(label_arg);}
  KNO_DOLIST(clause,KNO_CDR(expr)) {
    lispval test_val;
    if (!(PAIRP(clause)))
      return kno_err(kno_SyntaxError,_("invalid cond clause"),NULL,expr);
    else if (KNO_EQ(KNO_CAR(clause),else_symbol)) {
      u8_log(U8_LOG_MSG,label,"COND ? ELSE => %Q",KNO_CDR(clause));
      lispval value = kno_eval_body
	(KNO_CDR(clause),env,_stack,"%WATCHCOND","else",0);
      u8_log(U8_LOG_MSG,label,"COND ? ELSE => %Q => ",KNO_CDR(clause),value);
      return value;}
    else test_val = kno_eval(KNO_CAR(clause),env,_stack);
    if (KNO_ABORTED(test_val)) return test_val;
    else if (FALSEP(test_val)) {}
    else {
      u8_log(U8_LOG_MSG,label,"COND => %q => %Q",
	     KNO_CAR(clause),KNO_CDR(clause));
      lispval applyp =
	((PAIRP(KNO_CDR(clause))) &&
	 (KNO_EQ(KNO_CAR(KNO_CDR(clause)),apply_marker)));
      if (applyp) {
	if (PAIRP(KNO_CDR(KNO_CDR(clause)))) {
	  lispval fnexpr = KNO_CAR(KNO_CDR(KNO_CDR(clause)));
	  lispval fn = kno_eval(fnexpr,env,_stack);
	  if (KNO_ABORTED(fn)) {
	    kno_decref(test_val);
	    return fn;}
	  else if (KNO_APPLICABLEP(fn)) {
	    lispval retval = kno_apply(fn,1,&test_val);
	    kno_decref(test_val); kno_decref(fn);
	    return retval;}
	  else {
	    kno_decref(test_val);
	    return kno_type_error("function","watched_cond_evalfn",fn);}}
	else return kno_err
	       (kno_SyntaxError,"watched_cond_evalfn","apply syntax",expr);}
      else {
	kno_decref(test_val);
	lispval value =
	  kno_eval_body(KNO_CDR(clause),env,_stack,"COND","==>",0);
	u8_log(U8_LOG_MSG,label,"COND => %q => %q",
	       KNO_CAR(clause),value);
	return value;}}}
  return VOID;
}

DEFC_EVALFN("%watchtry",watched_try_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(%WATCHTRY ((*test* *consequents...)) *more clauses...*) "
	    "executes a `TRY` form and logs which alternate returned "
	    "non empty.");
static lispval watched_try_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval label_arg = KNO_VOID;
  u8_string label="%WTRY";
  lispval clauses = KNO_CDR(expr);
  if (KNO_STRINGP(KNO_CAR(clauses))) {
    label = KNO_CSTRING(label);
    clauses = KNO_CDR(clauses);}
  else if ( (KNO_PAIRP(KNO_CAR(clauses))) &&
	    (KNO_PAIRP(KNO_CDR(KNO_CAR(clauses)))) &&
	    (KNO_EQ(KNO_CAR(KNO_CAR(clauses)),unquote_symbol))) {
    lispval label_expr = KNO_CADR(KNO_CAR(clauses));
    clauses = KNO_CDR(clauses);
    label_arg = kno_eval(label_expr,env,_stack);
    if (KNO_ABORTP(label_arg)) {
      u8_exception ex = u8_erreify();
      u8_string errstring = kno_errstring(ex);
      u8_log(LOG_ERR,"BadWatchLabel",
	     "Watch label '%q' returned an error (%s)",
	     label_expr,errstring);
      u8_free(errstring);
      u8_free_exception(ex,0);
      label_arg = KNO_VOID;}
    else if (KNO_STRINGP(label_arg)) {
      int len = KNO_STRLEN(label_arg);
      u8_byte *tmplabel = alloca(len+1);
      strcpy(tmplabel,KNO_CSTRING(label_arg));
      label = tmplabel;}
    else u8_log(LOG_ERR,"BadWatchLabel",
		"Watch label '%q' value %q wasn't a string",
		label_expr,label_arg);
    kno_decref(label_arg);}
  lispval value = EMPTY;
  KNO_DOLIST(clause,clauses) {
    int ipe_state = kno_ipeval_status();
    kno_decref(value);
    value = kno_eval(clause,env,_stack);
    if (KNO_ABORTED(value))
      return value;
    else if (VOIDP(value)) {
      kno_seterr(kno_VoidArgument,"try_evalfn",NULL,clause);
      return KNO_ERROR;}
    else if (!(EMPTYP(value))) {
      u8_log(U8_LOG_MSG,label,"TRY %q ==> %q",clause,value);
      return value;}
    else if (kno_ipeval_status()!=ipe_state)
      return value;}
  return value;
}

/* Debugging assistance */

#pragma GCC push_options
#pragma GCC optimize ("O0")

KNO_EXPORT lispval _kno_dbg(lispval x)
{
  lispval result=_kno_debug(x);
  if (result == x)
    return result;
  else {
    kno_incref(result);
    kno_decref(x);
    return result;}
}

int (*kno_dump_exception)(lispval ex);

DEFC_EVALFN("dbg",dbg_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(dbg *expr* *cond* [*pre*])` evaluates *expr* and returns its value. "
	    "When provided, *cond* is evaluated; if it is #f, dbg just "
	    "returns the value. Otherwise, a log message is emitted providing "
	    "information about the value and the result of calling *cond*")
static lispval dbg_evalfn(lispval dbg_expr,kno_lexenv env,kno_stack stack)
{
  lispval expr=kno_get_arg(dbg_expr,1);
  lispval msg_expr=kno_get_arg(dbg_expr,2);
  lispval pre_expr=kno_get_arg(dbg_expr,3);
  lispval msg= (KNO_VOIDP(msg_expr)) ? (KNO_VOID) :
    (kno_eval(msg_expr,env,stack));
  if (KNO_FALSEP(msg))
    return kno_eval(expr,env,stack);
  else if (!((KNO_VOIDP(pre_expr))||(KNO_FALSEP(pre_expr)))) {
  examine_expr:
    if (KNO_VOIDP(msg))
      u8_message("Evaluating %Q",expr);
    else u8_message("Evaluating (%q) %Q",msg,expr);}
  lispval arg = kno_eval(expr,env,stack);
  if (KNO_VOIDP(msg))
    u8_message("Debug %q <==\n    %Q",arg,expr);
  else u8_message("Debug (%q) %q <=\n	  %Q",KNO_LONGVAL(&arg),
		  msg,arg,expr);
  kno_decref(msg);
  log_ptr(arg,KNO_VOID,arg);
 examine_result:
  return arg;
}

#pragma GCC pop_options

KNO_EXPORT lispval kno_debug_wait(lispval obj,lispval msg,int global)
{
  volatile int looping = 1;
  u8_log(LOGCRIT,"DebuggerWait",
	 "Waiting for debugger to attach pid %s:%lld to examine %q (%q)",
	 kno_exe_name,(long long)getpid(),obj,msg);
  if (global) u8_pause("KnoDebugWait");
  while (looping) {
  waiting:
    sleep(1);} /* set looping=0 to continue */
  return obj;
}

DEFC_EVALFN("dbg/wait",dbg_wait_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(dbg *expr* *cond* [*pre*])` evaluates *expr* and returns its value. "
	    "When provided, *cond* is evaluated; if it is #f, dbg just "
	    "returns the value. Otherwise, a log message is emitted providing "
	    "information about the value and the result of calling *cond*")
static lispval dbg_wait_evalfn(lispval dbg_expr,kno_lexenv env,kno_stack stack)
{
  lispval expr=kno_get_arg(dbg_expr,1);
  lispval msg_expr=kno_get_arg(dbg_expr,2);
  lispval pre_expr=kno_get_arg(dbg_expr,3);
  lispval global_expr=kno_get_arg(dbg_expr,4);
  int global = (!((KNO_VOIDP(global_expr))||(KNO_FALSEP(global_expr))));
  lispval msg= (KNO_VOIDP(msg_expr)) ? (KNO_VOID) :
    (kno_eval(msg_expr,env,stack));
  if (KNO_FALSEP(msg))
    return kno_eval(expr,env,stack);
  else if (!((KNO_VOIDP(pre_expr))||(KNO_FALSEP(pre_expr)))) {
  examine_expr:
    if (KNO_VOIDP(msg))
      u8_message("Evaluating %Q",expr);
    else u8_message("Evaluating (%q) %Q",msg,expr);
    kno_debug_wait(expr,msg,global);}
  lispval arg = kno_eval(expr,env,stack);
  if (KNO_VOIDP(msg))
    u8_message("Debug %q <==\n    %Q",arg,expr);
  else u8_message("Debug (%q) %q <=\n	  %Q",KNO_LONGVAL(&arg),
		  msg,arg,expr);
  log_ptr(arg,KNO_VOID,arg);
  kno_debug_wait(arg,msg,global);
  kno_decref(msg);
 examine_result:
  return arg;
}

/* Exposing environments */

DEFC_EVALFN("expose-module",expose_module_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(expose-module *module* [*into*]) evaluates *module* and "
	    "imports all of its bindings into the environment *into* which "
	    "defaults to the current environment. If *into* is not specified, "
	    "this returns VOID; otherwise, the result of evaluating *into* "
	    "is returned.")
static lispval expose_module_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval module_arg = kno_get_arg(expr,1), use_bindings = KNO_VOID;
  kno_lexenv modify_env = env;
  if (KNO_VOIDP(module_arg))
    return kno_err(kno_SyntaxError,"absorb_module_evalfn",
		   NULL,expr);
  lispval module = kno_eval_arg(module_arg,env,_stack);
  if (KNO_SYMBOLP(module)) module = kno_find_module(module,0);
  if (KNO_ABORTED(module)) return module;
  if ( (KNO_LEXENVP(module)) &&
       (KNO_HASHTABLEP(((kno_lexenv)module)->env_bindings)) &&
       ( (((kno_lexenv)module)->env_copy) == ((kno_lexenv)module) ) )
    use_bindings = ((kno_lexenv)module)->env_bindings;
  else {
    lispval err = kno_err("WeirdModuleBindings","absorb_module_evalfn",NULL,module);
    kno_decref(module);
    return err;}
  lispval into_arg = kno_get_arg(expr,2);
  if (KNO_VOIDP(into_arg)) {}
  else {
    lispval into = kno_eval_arg(into_arg,env,_stack);
    if (KNO_LEXENVP(into))
      modify_env=(kno_lexenv)into;
    else {
      lispval err = kno_err("NotaLexenv","expose_module_evalfn",NULL,into);
      kno_decref(module);
      kno_decref(into);
      return err;}}
  kno_lexenv old_parent = modify_env->env_parent;
  modify_env->env_parent = kno_make_export_env(use_bindings,old_parent);
  /* We decref this because 'env' is no longer pointing to it
     and kno_make_export_env incref'd it again. */
  if (old_parent) kno_decref((lispval)(old_parent));
  if (KNO_VOIDP(into_arg)) return VOID;
  else return (lispval) modify_env;
}

/* Recording bugs */

KNO_EXPORT int kno_dump_bug(lispval ex,u8_string into)
{
  u8_string bugpath;
  if (into == NULL)
    return 0;
  else if (u8_directoryp(into)) {
    struct U8_XTIME now;
    u8_string bugdir = u8_abspath(into,NULL);
    long long pid = getpid();
    long long tid = u8_threadid();
    u8_string appid = u8_appid();
    u8_now(&now);
    u8_string filename = ( pid == tid ) ?
      (u8_mkstring("%s-p%lld-%4d-%02d-%02dT%02d:%02d:%02f.err",
		   appid,pid,
		   now.u8_year,now.u8_mon+1,now.u8_mday,
		   now.u8_hour,now.u8_min,
		   (1.0*now.u8_sec)+(now.u8_nsecs/1000000000.0))) :
      (u8_mkstring("%s-p%lld-%4d-%02d-%02dT%02d:%02d:%02f-t%lld.err",
		   appid,pid,
		   now.u8_year,now.u8_mon+1,now.u8_mday,
		   now.u8_hour,now.u8_min,
		   (1.0*now.u8_sec)+(now.u8_nsecs/1000000000.0),
		   pid,tid));
    bugpath = u8_mkpath(bugdir,filename);
    u8_free(bugdir);
    u8_free(filename);}
  else bugpath = u8_strdup(into);
  struct KNO_EXCEPTION *exo = (kno_exception) ex;
  int rv = kno_write_dtype_to_file(ex,bugpath);
  if (rv<0)
    u8_log(LOG_CRIT,"RecordBug",
	   "Couldn't write exception object to %s",bugpath);
  else if (exo->ex_details)
    u8_log(LOG_WARN,"RecordBug",
	   "Wrote exception %m <%s> (%s) to %s",
	   exo->ex_condition,exo->ex_caller,exo->ex_details,
	   bugpath);
  else u8_log(LOG_WARN,"RecordBug",
	      "Wrote exception %m <%s> to %s",
	      exo->ex_condition,exo->ex_caller,bugpath);
  u8_free(bugpath);
  return rv;
}

KNO_EXPORT int kno_record_bug(lispval ex)
{
  if (kno_bugdir == NULL)
    return 0;
  else return kno_dump_bug(ex,kno_bugdir);
}

DEFC_PRIM("dump-bug",dumpbug_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "(DUMP-BUG *err* [*to*]) "
	  "writes a DType representation of *err* to either "
	  "*to* or the configured BUGDIR. If *err* is #t, "
	  "returns a packet of the representation. Without "
	  "*to* or if *to* is #f or #default, writes the "
	  "exception into either './errors/' or './'",
	  {"ex",kno_exception_type,KNO_VOID},
	  {"where",kno_any_type,KNO_VOID})
static lispval dumpbug_prim(lispval ex,lispval where)
{
  if (KNO_TRUEP(where)) {
    struct KNO_OUTBUF bugout; KNO_INIT_BYTE_OUTPUT(&bugout,16000);
    int rv = kno_write_dtype(&bugout,ex);
    if (rv<0) {
      kno_close_outbuf(&bugout);
      return KNO_ERROR;}
    else {
      lispval packet = kno_make_packet(NULL,BUFIO_POINT(&bugout),bugout.buffer);
      kno_close_outbuf(&bugout);
      return packet;}}
  else if ( (KNO_VOIDP(where)) || (KNO_FALSEP(where)) || (KNO_DEFAULTP(where)) ) {
    u8_string bugdir = kno_bugdir;
    if (bugdir == NULL) {
      u8_string cwd = u8_getcwd();
      u8_string errpath = u8_mkpath(cwd,"_bugjar");
      if ( (u8_directoryp(errpath)) && (u8_file_writablep(errpath)) )
	bugdir = errpath;
      else if ( (u8_directoryp(cwd)) && (u8_file_writablep(cwd)) )
	bugdir = cwd;
      else bugdir = u8_strdup("/tmp/");
      if (bugdir != errpath) u8_free(errpath);
      if (bugdir != cwd) u8_free(cwd);}
    else NO_ELSE; /* got bugdir already */
    int rv = kno_dump_bug(ex,bugdir);
    if (rv<0)
      kno_seterr("RECORD-BUG failed","record_bug",NULL,where);
    if (bugdir != kno_bugdir) u8_free(bugdir);
    if (rv<0) return KNO_ERROR;
    else return KNO_TRUE;}
  else if (KNO_STRINGP(where)) {
    int rv = kno_dump_bug(ex,KNO_CSTRING(where));
    if (rv<0) {
      kno_seterr("RECORD-BUG failed","record_bug",
		 KNO_CSTRING(where),ex);
      return KNO_ERROR;}
    else return KNO_TRUE;}
  else return kno_type_error("filename","record_bug",where);
}

static int config_bugdir(lispval var,lispval val,void *state)
{
  if (KNO_FALSEP(val)) {
    kno_dump_exception = NULL;
    u8_free(kno_bugdir);
    kno_bugdir = NULL;
    return 0;}
  else if ( ( kno_dump_exception == NULL ) ||
	    ( kno_dump_exception == kno_record_bug ) ) {
    if (KNO_STRINGP(val)) {
      u8_string old = kno_bugdir;
      u8_string new = KNO_CSTRING(val);
      if (u8_directoryp(new)) {}
      else if (u8_file_existsp(new)) {
	kno_seterr("not a directory","config_bugdir",u8_strdup(new),VOID);
	return -1;}
      else {
	int rv = u8_mkdirs(new,0664);
	if (rv<0) {
	  u8_graberrno("config_bugdir/mkdir",u8_strdup(new));
	  u8_seterr(_("missing directory"),"config_bugdir",u8_strdup(new));
	  return -1;}}
      kno_bugdir = u8_strdup(new);
      if (old) u8_free(old);
      kno_dump_exception = kno_record_bug;
      return 1;}
    else {
      kno_seterr(kno_TypeError,"config_bugdir",u8_strdup("pathstring"),val);
      return -1;}}
  else {
    u8_seterr("existing bug handler","config_bugdir",NULL);
    return -1;}
}

/* Google profiler usage */

#if USING_GOOGLE_PROFILER
DEFC_EVALFN("google/profile",google_profile_evalfn,KNO_EVALFN_DEFAULTS,
	    "evaluates the expressions "
	    "in *body* with Google CPU profiling activated. If the second "
	    "argument is a string, it is used as the outfile for the profiling "
	    "information.");
static lispval gprofile_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  int start = 1; char *filename = NULL;
  if ( (PAIRP(KNO_CDR(expr))) && (STRINGP(KNO_CAR(KNO_CDR(expr)))) ) {
    filename = u8_strdup(CSTRING(KNO_CAR(KNO_CDR(expr))));
    start = 2;}
  else if ((PAIRP(KNO_CDR(expr)))&&(SYMBOLP(KNO_CAR(KNO_CDR(expr))))) {
    lispval val = kno_symeval(KNO_CADR(expr),env);
    if (KNO_ABORTED(val)) return val;
    else if (STRINGP(val)) {
      filename = u8_strdup(CSTRING(val));
      kno_decref(val);
      start = 2;}
    else return kno_type_error("filename","GOOGLE/PROFILE",val);}
  else if (cpu_profilename)
    filename = u8_strdup(cpu_profilename);
  else if (getenv("CPUPROFILE"))
    filename = u8_strdup(getenv("CPUPROFILE"));
  else filename = u8_mkstring("/tmp/gprof%ld.pid",(long)getpid());
  ProfilerStart(filename); {
    lispval value = VOID;
    lispval body = kno_get_body(expr,start);
    KNO_DOLIST(ex,body) {
      kno_decref(value); value = kno_eval(ex,env,_stack);
      if (KNO_ABORTED(value)) {
	ProfilerStop();
	return value;}
      else {}}
    ProfilerStop();
    u8_free(filename);
    return value;}
}

static lispval gprofile_stop()
{
  ProfilerStop();
  return VOID;
}
#endif

/* Profiling functions */

static lispval profile_symbol;

DEFC_EVALFN("PROFILE",profiled_eval_evalfn,KNO_EVALFN_DEFAULTS,
	    "evaluates *expr*, returning "
	    "the resulting value. The time taken to evaluate *expr* is "
	    "tracked and added the the value of *label* in the hashtable "
	    "which is the value of ``profile`` in the current enviroment.");
static lispval profiled_eval_evalfn(lispval expr,kno_lexenv env,kno_stack stack)
{
  lispval toeval = kno_get_arg(expr,1);
  double start = u8_elapsed_time();
  lispval value = kno_eval(toeval,env,stack);
  if (KNO_ABORTED(value)) return value;
  double finish = u8_elapsed_time();
  lispval tag = kno_get_arg(expr,2);
  lispval profile_info = kno_symeval(profile_symbol,env), profile_data;
  if (VOIDP(profile_info)) return value;
  profile_data = kno_get(profile_info,tag,VOID);
  if (KNO_ABORTED(profile_data)) {
    kno_decref(value); kno_decref(profile_info);
    return profile_data;}
  else if (VOIDP(profile_data)) {
    lispval time = kno_init_double(NULL,(finish-start));
    profile_data = kno_conspair(KNO_INT(1),time);
    kno_store(profile_info,tag,profile_data);}
  else {
    struct KNO_PAIR *p = kno_consptr(kno_pair,profile_data,kno_pair_type);
    struct KNO_FLONUM *d = kno_consptr(kno_flonum,(p->cdr),kno_flonum_type);
    p->car = KNO_INT(kno_getint(p->car)+1);
    d->floval = d->floval+(finish-start);}
  kno_decref(profile_data); kno_decref(profile_info);
  return value;
}

/* MTrace */

#if HAVE_MTRACE
static int mtracing=0;
#endif

DEFC_PRIM("mtrace",mtrace_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	  "Activates libc heap tracing via MALLOC_TRACE and "
	  "returns true if it worked. Optional argument is a "
	  "filename to set as heap log.",
	  {"arg",kno_any_type,KNO_VOID})
static lispval mtrace_prim(lispval arg)
{
  /* TODO: Check whether we're running under libc malloc (so mtrace
     will do anything). */
#if HAVE_MTRACE
  if (mtracing)
    return KNO_TRUE;
  else if (getenv("MALLOC_TRACE")) {
    mtrace();
    mtracing=1;
    return KNO_TRUE;}
  else if ((STRINGP(arg)) &&
	   (u8_file_writablep(CSTRING(arg)))) {
    setenv("MALLOC_TRACE",CSTRING(arg),1);
    mtrace();
    mtracing=1;
    return KNO_TRUE;}
  else return KNO_FALSE;
#else
  return KNO_FALSE;
#endif
}

DEFC_PRIM("muntrace",muntrace_prim,
	  KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
	  "Deactivates libc heap tracing, returns true if it "
	  "did anything")
static lispval muntrace_prim()
{
#if HAVE_MTRACE
  if (mtracing) {
    muntrace();
    return KNO_TRUE;}
  else return KNO_FALSE;
#else
  return KNO_FALSE;
#endif
}

/* WITH-CONTEXT */

DEFC_EVALFN("with-log-context",with_log_context_evalfn,KNO_EVALFN_DEFAULTS,
	    "evaluates each expression in *body* while setting the "
	    "*log context* to the result (a string or symbol) of "
	    "evaluating *label-expr*. This log context will be displayed "
	    "together with all the log messages emitted during the "
	    "execution of *body*.");
static lispval with_log_context_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval label_expr=kno_get_arg(expr,1);
  if (KNO_VOIDP(label_expr))
    return kno_err(kno_SyntaxError,"with_context_evalfn",NULL,expr);
  else {
    lispval label=kno_eval(label_expr,env,_stack);
    if (KNO_ABORTED(label))
      return label;
    else {
      lispval outer_lisp_context = kno_thread_get(logcxt_symbol);
      u8_string outer_context = u8_log_context;
      u8_string local_context = (KNO_SYMBOLP(label)) ? (KNO_SYMBOL_NAME(label)) :
	(KNO_STRINGP(label)) ? (KNO_CSTRING(label)) : (NULL);
      kno_thread_set(logcxt_symbol,label);
      u8_set_log_context(local_context);
      lispval result=kno_eval_body(kno_get_body(expr,2),env,_stack,
				   "with_log_context",KNO_CSTRING(label),0);
      kno_thread_set(logcxt_symbol,outer_lisp_context);
      u8_set_log_context(outer_context);
      kno_decref(label);
      return result;}}
}

DEFC_PRIM("set-log-context!",set_log_context_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "only makes sense in the dynamic thread-specific "
	  "scope of a `WITH-LOG-CONTEXT` expression and it "
	  "changes the displayed log context to the string "
	  "or symbol *label*.",
	  {"label",kno_any_type,KNO_VOID})
static lispval set_log_context_prim(lispval label)
{
  u8_string local_context = (KNO_SYMBOLP(label)) ? (KNO_SYMBOL_NAME(label)) :
    (KNO_STRINGP(label)) ? (KNO_CSTRING(label)) : (NULL);
  kno_thread_set(logcxt_symbol,label);
  u8_set_log_context(local_context);
  return KNO_VOID;
}

/* Primitives for testing purposes */

#if __clang__
#define DONT_OPTIMIZE  __attribute__((optnone))
#else
#define DONT_OPTIMIZE __attribute__((optimize("O0")))
#endif

/* These are for wrapping around Scheme code to see in C profilers */
DEFC_EVALFN("eval1",eval1,KNO_EVALFN_DEFAULTS,
	    "evaulates *expr* and returns the "
	    "result. This is used for debugging by labelling eval "
	    "contexts in a way which is visible in C debuggers.");
static DONT_OPTIMIZE lispval eval1(lispval expr,kno_lexenv env,kno_stack s)
{
  lispval result = kno_eval(kno_get_arg(expr,1),env,s);
  return result;
}

DEFC_EVALFN("eval2",eval2,KNO_EVALFN_DEFAULTS,
	    "evaulates *expr* and returns the "
	    "result. This is used for debugging by labelling eval "
	    "contexts in a way which is visible in C debuggers.");
static DONT_OPTIMIZE lispval eval2(lispval expr,kno_lexenv env,kno_stack s)
{
  lispval result = kno_eval(kno_get_arg(expr,1),env,s);
  return result;
}

DEFC_EVALFN("eval3",eval3,KNO_EVALFN_DEFAULTS,
	    "evaulates *expr* and returns the "
	    "result. This is used for debugging by labelling eval "
	    "contexts in a way which is visible in C debuggers.");
static DONT_OPTIMIZE lispval eval3(lispval expr,kno_lexenv env,kno_stack s)
{
  lispval result = kno_eval(kno_get_arg(expr,1),env,s);
  return result;
}

DEFC_EVALFN("eval4",eval4,KNO_EVALFN_DEFAULTS,
	    "evaulates *expr* and returns the "
	    "result. This is used for debugging by labelling eval "
	    "contexts in a way which is visible in C debuggers.");
static DONT_OPTIMIZE lispval eval4(lispval expr,kno_lexenv env,kno_stack s)
{
  lispval result = kno_eval(kno_get_arg(expr,1),env,s);
  return result;
}

DEFC_EVALFN("eval5",eval5,KNO_EVALFN_DEFAULTS,
	    "evaulates *expr* and returns the "
	    "result. This is used for debugging by labelling eval "
	    "contexts in a way which is visible in C debuggers.");
static DONT_OPTIMIZE lispval eval5(lispval expr,kno_lexenv env,kno_stack s)
{
  lispval result = kno_eval(kno_get_arg(expr,1),env,s);
  return result;
}

DEFC_EVALFN("eval6",eval6,KNO_EVALFN_DEFAULTS,
	    "evaulates *expr* and returns the "
	    "result. This is used for debugging by labelling eval "
	    "contexts in a way which is visible in C debuggers.");
static DONT_OPTIMIZE lispval eval6(lispval expr,kno_lexenv env,kno_stack s)
{
  lispval result = kno_eval(kno_get_arg(expr,1),env,s);
  return result;
}

DEFC_EVALFN("eval7",eval7,KNO_EVALFN_DEFAULTS,
	    "evaulates *expr* and returns the "
	    "result. This is used for debugging by labelling eval "
	    "contexts in a way which is visible in C debuggers.");
static DONT_OPTIMIZE lispval eval7(lispval expr,kno_lexenv env,kno_stack s)
{
  lispval result = kno_eval(kno_get_arg(expr,1),env,s);
  return result;
}

DEFC_PRIM("list9",list9,
	  KNO_MAX_ARGS(9)|KNO_MIN_ARGS(0),
	  "Returns a nine-element list (for testing the knox "
	  "engine)",
	  {"arg1",kno_any_type,KNO_FALSE},
	  {"arg2",kno_any_type,KNO_FALSE},
	  {"arg3",kno_any_type,KNO_FALSE},
	  {"arg4",kno_any_type,KNO_FALSE},
	  {"arg5",kno_any_type,KNO_FALSE},
	  {"arg6",kno_any_type,KNO_FALSE},
	  {"arg7",kno_any_type,KNO_FALSE},
	  {"arg8",kno_any_type,KNO_FALSE},
	  {"arg9",kno_any_type,KNO_FALSE})
static lispval list9(lispval arg1,lispval arg2,
		     lispval arg3,lispval arg4,
		     lispval arg5,lispval arg6,
		     lispval arg7,lispval arg8,
		     lispval arg9)
{
  return kno_make_list(9,kno_incref(arg1),kno_incref(arg2),
		       kno_incref(arg3),kno_incref(arg4),
		       kno_incref(arg5),kno_incref(arg6),
		       kno_incref(arg7),kno_incref(arg8),
		       kno_incref(arg9));
}

DEFC_PRIM("_plus4",plus4,
	  KNO_MAX_ARGS(4)|KNO_MIN_ARGS(1),
	  "Add up to four numbers (for testing the knox "
	  "engine)",
	  {"arg1",kno_any_type,KNO_INT(0)},
	  {"arg2",kno_any_type,KNO_INT(0)},
	  {"arg3",kno_any_type,KNO_INT(0)},
	  {"arg4",kno_any_type,KNO_INT(0)})
static lispval plus4(lispval arg1,lispval arg2,
		     lispval arg3,lispval arg4)
{
  int sum = KNO_FIX2INT(arg1) + KNO_FIX2INT(arg2) + KNO_FIX2INT(arg3) +
    KNO_FIX2INT(arg4);
  return KNO_INT(sum);
}

DEFC_PRIM("_plus5",plus5,
	  KNO_MAX_ARGS(5)|KNO_MIN_ARGS(1),
	  "Add up to five numbers (for testing the knox "
	  "engine)",
	  {"arg1",kno_any_type,KNO_INT(0)},
	  {"arg2",kno_any_type,KNO_INT(0)},
	  {"arg3",kno_any_type,KNO_INT(0)},
	  {"arg4",kno_any_type,KNO_INT(0)},
	  {"arg5",kno_any_type,KNO_INT(0)})
static lispval plus5(lispval arg1,lispval arg2,
		     lispval arg3,lispval arg4,
		     lispval arg5)
{
  int sum = KNO_FIX2INT(arg1) + KNO_FIX2INT(arg2) + KNO_FIX2INT(arg3) +
    KNO_FIX2INT(arg4) + KNO_FIX2INT(arg5);
  return KNO_INT(sum);
}

DEFC_PRIM("_plus6",plus6,
	  KNO_MAX_ARGS(6)|KNO_MIN_ARGS(2),
	  "Add up to six numbers (for testing the knox "
	  "engine)",
	  {"arg1",kno_any_type,KNO_INT(0)},
	  {"arg2",kno_any_type,KNO_INT(0)},
	  {"arg3",kno_any_type,KNO_INT(0)},
	  {"arg4",kno_any_type,KNO_INT(0)},
	  {"arg5",kno_any_type,KNO_INT(0)},
	  {"arg6",kno_any_type,KNO_INT(0)})
static lispval plus6(lispval arg1,lispval arg2,
		     lispval arg3,lispval arg4,
		     lispval arg5,lispval arg6)
{
  int sum = KNO_FIX2INT(arg1) + KNO_FIX2INT(arg2) + KNO_FIX2INT(arg3) +
    KNO_FIX2INT(arg4) + KNO_FIX2INT(arg5) + KNO_FIX2INT(arg6);
  return KNO_INT(sum);
}

DEFC_PRIM("_plus7",plus7,
	  KNO_MAX_ARGS(7)|KNO_MIN_ARGS(3),
	  "Add up to seven numbers (for testing the knox "
	  "engine)",
	  {"arg1",kno_any_type,KNO_INT(0)},
	  {"arg2",kno_any_type,KNO_INT(0)},
	  {"arg3",kno_any_type,KNO_INT(0)},
	  {"arg4",kno_any_type,KNO_INT(0)},
	  {"arg5",kno_any_type,KNO_INT(0)},
	  {"arg6",kno_any_type,KNO_INT(0)},
	  {"arg7",kno_any_type,KNO_INT(0)})
static lispval plus7(lispval arg1,lispval arg2,
		     lispval arg3,lispval arg4,
		     lispval arg5,lispval arg6,
		     lispval arg7)
{
  int sum = KNO_FIX2INT(arg1) + KNO_FIX2INT(arg2) + KNO_FIX2INT(arg3) +
    KNO_FIX2INT(arg4) + KNO_FIX2INT(arg5) + KNO_FIX2INT(arg6) +
    KNO_FIX2INT(arg7);
  return KNO_INT(sum);
}

DEFC_PRIM("_plus8",plus8,
	  KNO_MAX_ARGS(8)|KNO_MIN_ARGS(3),
	  "Add up to eight numbers (for testing the knox "
	  "engine)",
	  {"arg1",kno_any_type,KNO_INT(0)},
	  {"arg2",kno_any_type,KNO_INT(0)},
	  {"arg3",kno_any_type,KNO_INT(0)},
	  {"arg4",kno_any_type,KNO_INT(0)},
	  {"arg5",kno_any_type,KNO_INT(0)},
	  {"arg6",kno_any_type,KNO_INT(0)},
	  {"arg7",kno_any_type,KNO_INT(0)},
	  {"arg8",kno_any_type,KNO_INT(0)})
static lispval plus8(lispval arg1,lispval arg2,
		     lispval arg3,lispval arg4,
		     lispval arg5,lispval arg6,
		     lispval arg7,
		     lispval arg8)
{
  int sum = KNO_FIX2INT(arg1) + KNO_FIX2INT(arg2) + KNO_FIX2INT(arg3) +
    KNO_FIX2INT(arg4) + KNO_FIX2INT(arg5) + KNO_FIX2INT(arg6) +
    KNO_FIX2INT(arg7) + KNO_FIX2INT(arg8);
  return KNO_INT(sum);
}

DEFC_PRIM("_plus9",plus9,
	  KNO_MAX_ARGS(9)|KNO_MIN_ARGS(3),
	  "Add numberss (for testing the knox engine)",
	  {"arg1",kno_any_type,KNO_INT(0)},
	  {"arg2",kno_any_type,KNO_INT(0)},
	  {"arg3",kno_any_type,KNO_INT(0)},
	  {"arg4",kno_any_type,KNO_INT(0)},
	  {"arg5",kno_any_type,KNO_INT(0)},
	  {"arg6",kno_any_type,KNO_INT(0)},
	  {"arg7",kno_any_type,KNO_INT(0)},
	  {"arg8",kno_any_type,KNO_INT(0)},
	  {"arg9",kno_any_type,KNO_INT(0)})
static lispval plus9(lispval arg1,lispval arg2,
		     lispval arg3,lispval arg4,
		     lispval arg5,lispval arg6,
		     lispval arg7,
		     lispval arg8,
		     lispval arg9)
{
  int sum = KNO_FIX2INT(arg1) + KNO_FIX2INT(arg2) + KNO_FIX2INT(arg3) +
    KNO_FIX2INT(arg4) + KNO_FIX2INT(arg5) + KNO_FIX2INT(arg6) +
    KNO_FIX2INT(arg7) + KNO_FIX2INT(arg8) + KNO_FIX2INT(arg9);
  return KNO_INT(sum);
}

DEFC_PRIM("_plus10",plus10,
	  KNO_MAX_ARGS(10)|KNO_MIN_ARGS(3),
	  "Add numberss (for testing the knox engine)",
	  {"arg1",kno_any_type,KNO_INT(0)},
	  {"arg2",kno_any_type,KNO_INT(0)},
	  {"arg3",kno_any_type,KNO_INT(0)},
	  {"arg4",kno_any_type,KNO_INT(0)},
	  {"arg5",kno_any_type,KNO_INT(0)},
	  {"arg6",kno_any_type,KNO_INT(0)},
	  {"arg7",kno_any_type,KNO_INT(0)},
	  {"arg8",kno_any_type,KNO_INT(0)},
	  {"arg9",kno_any_type,KNO_INT(0)},
	  {"arg10",kno_any_type,KNO_INT(0)})
static lispval plus10(lispval arg1,lispval arg2,
		      lispval arg3,lispval arg4,
		      lispval arg5,lispval arg6,
		      lispval arg7,
		      lispval arg8,
		      lispval arg9,
		      lispval arg10)
{
  int sum = KNO_FIX2INT(arg1) + KNO_FIX2INT(arg2) + KNO_FIX2INT(arg3) +
    KNO_FIX2INT(arg4) + KNO_FIX2INT(arg5) + KNO_FIX2INT(arg6) +
    KNO_FIX2INT(arg7) + KNO_FIX2INT(arg8) + KNO_FIX2INT(arg9) +
    KNO_FIX2INT(arg10);
  return KNO_INT(sum);
}

DEFC_PRIM("_plus11",plus11,
	  KNO_MAX_ARGS(11)|KNO_MIN_ARGS(3),
	  "Add numberss (for testing the knox engine)",
	  {"arg1",kno_any_type,KNO_INT(0)},
	  {"arg2",kno_any_type,KNO_INT(0)},
	  {"arg3",kno_any_type,KNO_INT(0)},
	  {"arg4",kno_any_type,KNO_INT(0)},
	  {"arg5",kno_any_type,KNO_INT(0)},
	  {"arg6",kno_any_type,KNO_INT(0)},
	  {"arg7",kno_any_type,KNO_INT(0)},
	  {"arg8",kno_any_type,KNO_INT(0)},
	  {"arg9",kno_any_type,KNO_INT(0)},
	  {"arg10",kno_any_type,KNO_INT(0)},
	  {"arg11",kno_any_type,KNO_INT(0)})
static lispval plus11(lispval arg1,lispval arg2,
		      lispval arg3,lispval arg4,
		      lispval arg5,lispval arg6,
		      lispval arg7,
		      lispval arg8,
		      lispval arg9,
		      lispval arg10,
		      lispval arg11)
{
  int sum = KNO_FIX2INT(arg1) + KNO_FIX2INT(arg2) + KNO_FIX2INT(arg3) +
    KNO_FIX2INT(arg4) + KNO_FIX2INT(arg5) + KNO_FIX2INT(arg6) +
    KNO_FIX2INT(arg7) + KNO_FIX2INT(arg8) + KNO_FIX2INT(arg9) +
    KNO_FIX2INT(arg10) + KNO_FIX2INT(arg11);
  return KNO_INT(sum);
}

DEFC_PRIM("_plus12",plus12,
	  KNO_MAX_ARGS(12)|KNO_MIN_ARGS(3),
	  "Add numberss (for testing the knox engine)",
	  {"arg1",kno_any_type,KNO_INT(0)},
	  {"arg2",kno_any_type,KNO_INT(0)},
	  {"arg3",kno_any_type,KNO_INT(0)},
	  {"arg4",kno_any_type,KNO_INT(0)},
	  {"arg5",kno_any_type,KNO_INT(0)},
	  {"arg6",kno_any_type,KNO_INT(0)},
	  {"arg7",kno_any_type,KNO_INT(0)},
	  {"arg8",kno_any_type,KNO_INT(0)},
	  {"arg9",kno_any_type,KNO_INT(0)},
	  {"arg10",kno_any_type,KNO_INT(0)},
	  {"arg11",kno_any_type,KNO_INT(0)},
	  {"arg12",kno_any_type,KNO_INT(0)})
static lispval plus12(lispval arg1,lispval arg2,
		      lispval arg3,lispval arg4,
		      lispval arg5,lispval arg6,
		      lispval arg7,
		      lispval arg8,
		      lispval arg9,
		      lispval arg10,
		      lispval arg11,
		      lispval arg12)
{
  int sum = KNO_FIX2INT(arg1) + KNO_FIX2INT(arg2) + KNO_FIX2INT(arg3) +
    KNO_FIX2INT(arg4) + KNO_FIX2INT(arg5) + KNO_FIX2INT(arg6) +
    KNO_FIX2INT(arg7) + KNO_FIX2INT(arg8) + KNO_FIX2INT(arg9) +
    KNO_FIX2INT(arg10) + KNO_FIX2INT(arg11) + KNO_FIX2INT(arg12);
  return KNO_INT(sum);
}

DEFC_PRIM("_plus13",plus13,
	  KNO_MAX_ARGS(13)|KNO_MIN_ARGS(3),
	  "Add numberss (for testing the knox engine)",
	  {"arg1",kno_any_type,KNO_INT(0)},
	  {"arg2",kno_any_type,KNO_INT(0)},
	  {"arg3",kno_any_type,KNO_INT(0)},
	  {"arg4",kno_any_type,KNO_INT(0)},
	  {"arg5",kno_any_type,KNO_INT(0)},
	  {"arg6",kno_any_type,KNO_INT(0)},
	  {"arg7",kno_any_type,KNO_INT(0)},
	  {"arg8",kno_any_type,KNO_INT(0)},
	  {"arg9",kno_any_type,KNO_INT(0)},
	  {"arg10",kno_any_type,KNO_INT(0)},
	  {"arg11",kno_any_type,KNO_INT(0)},
	  {"arg12",kno_any_type,KNO_INT(0)},
	  {"arg13",kno_any_type,KNO_INT(0)})
static lispval plus13(lispval arg1,lispval arg2,
		      lispval arg3,lispval arg4,
		      lispval arg5,lispval arg6,
		      lispval arg7,
		      lispval arg8,
		      lispval arg9,
		      lispval arg10,
		      lispval arg11,
		      lispval arg12,
		      lispval arg13)
{
  int sum = KNO_FIX2INT(arg1) + KNO_FIX2INT(arg2) + KNO_FIX2INT(arg3) +
    KNO_FIX2INT(arg4) + KNO_FIX2INT(arg5) + KNO_FIX2INT(arg6) +
    KNO_FIX2INT(arg7) + KNO_FIX2INT(arg8) + KNO_FIX2INT(arg9) +
    KNO_FIX2INT(arg10) + KNO_FIX2INT(arg11) + KNO_FIX2INT(arg12) +
    KNO_FIX2INT(arg13);
  return KNO_INT(sum);
}

DEFC_PRIM("_plus14",plus14,
	  KNO_MAX_ARGS(14)|KNO_MIN_ARGS(3),
	  "Add numbers (for testing the knox engine)",
	  {"arg1",kno_any_type,KNO_INT(0)},
	  {"arg2",kno_any_type,KNO_INT(0)},
	  {"arg3",kno_any_type,KNO_INT(0)},
	  {"arg4",kno_any_type,KNO_INT(0)},
	  {"arg5",kno_any_type,KNO_INT(0)},
	  {"arg6",kno_any_type,KNO_INT(0)},
	  {"arg7",kno_any_type,KNO_INT(0)},
	  {"arg8",kno_any_type,KNO_INT(0)},
	  {"arg9",kno_any_type,KNO_INT(0)},
	  {"arg10",kno_any_type,KNO_INT(0)},
	  {"arg11",kno_any_type,KNO_INT(0)},
	  {"arg12",kno_any_type,KNO_INT(0)},
	  {"arg13",kno_any_type,KNO_INT(0)},
	  {"arg14",kno_any_type,KNO_INT(0)})
static lispval plus14(lispval arg1,lispval arg2,
		      lispval arg3,lispval arg4,
		      lispval arg5,lispval arg6,
		      lispval arg7,
		      lispval arg8,
		      lispval arg9,
		      lispval arg10,
		      lispval arg11,
		      lispval arg12,
		      lispval arg13,
		      lispval arg14)
{
  int sum = KNO_FIX2INT(arg1) + KNO_FIX2INT(arg2) + KNO_FIX2INT(arg3) +
    KNO_FIX2INT(arg4) + KNO_FIX2INT(arg5) + KNO_FIX2INT(arg6) +
    KNO_FIX2INT(arg7) + KNO_FIX2INT(arg8) + KNO_FIX2INT(arg9) +
    KNO_FIX2INT(arg10) + KNO_FIX2INT(arg11) + KNO_FIX2INT(arg12) +
    KNO_FIX2INT(arg13) + KNO_FIX2INT(arg14);;
  return KNO_INT(sum);
}

DEFC_PRIM("_plus15",plus15,
	  KNO_MAX_ARGS(15)|KNO_MIN_ARGS(3),
	  "Add numbers (for testing the knox engine)",
	  {"arg1",kno_any_type,KNO_INT(0)},
	  {"arg2",kno_any_type,KNO_INT(0)},
	  {"arg3",kno_any_type,KNO_INT(0)},
	  {"arg4",kno_any_type,KNO_INT(0)},
	  {"arg5",kno_any_type,KNO_INT(0)},
	  {"arg6",kno_any_type,KNO_INT(0)},
	  {"arg7",kno_any_type,KNO_INT(0)},
	  {"arg8",kno_any_type,KNO_INT(0)},
	  {"arg9",kno_any_type,KNO_INT(0)},
	  {"arg10",kno_any_type,KNO_INT(0)},
	  {"arg11",kno_any_type,KNO_INT(0)},
	  {"arg12",kno_any_type,KNO_INT(0)},
	  {"arg13",kno_any_type,KNO_INT(0)},
	  {"arg14",kno_any_type,KNO_INT(0)},
	  {"arg15",kno_any_type,KNO_INT(0)})
static lispval plus15(lispval arg1,lispval arg2,
		      lispval arg3,lispval arg4,
		      lispval arg5,lispval arg6,
		      lispval arg7,
		      lispval arg8,
		      lispval arg9,
		      lispval arg10,
		      lispval arg11,
		      lispval arg12,
		      lispval arg13,
		      lispval arg14,
		      lispval arg15)
{
  int sum = KNO_FIX2INT(arg1) + KNO_FIX2INT(arg2) + KNO_FIX2INT(arg3) +
    KNO_FIX2INT(arg4) + KNO_FIX2INT(arg5) + KNO_FIX2INT(arg6) +
    KNO_FIX2INT(arg7) + KNO_FIX2INT(arg8) + KNO_FIX2INT(arg9) +
    KNO_FIX2INT(arg10) + KNO_FIX2INT(arg11) + KNO_FIX2INT(arg12) +
    KNO_FIX2INT(arg13) + KNO_FIX2INT(arg14) + KNO_FIX2INT(arg15);
  return KNO_INT(sum);
}

/* C debugging functions */

KNO_EXPORT void knodbg_show_env(u8_output out,kno_lexenv start,int limit)
{
  int depth = 0;
  kno_lexenv env=start;
  if (limit<0)  {
    lispval bindings = env->env_bindings;
    lispval keys = kno_getkeys(bindings);
    u8_byte buf[128];
    KNO_DO_CHOICES(key,keys) {
      if (KNO_SYMBOLP(key)) {
        lispval val=kno_get(bindings,key,KNO_VOID);
        u8_string vstring=u8_sprintf(buf,128,"%q",val);
	u8_printf(out,"  %s\t 0x%llx\t%s\n",
		  KNO_SYMBOL_NAME(key),(unsigned long long)val,
		  vstring);
	kno_decref(val);}}
    kno_decref(keys);}
  else while ( (env) && (depth < limit) ) {
      lispval bindings = env->env_bindings;
      kno_lisp_type btype = KNO_TYPEOF(bindings);
      lispval name = kno_get(bindings,KNOSYM_MODULEID,KNO_VOID);
      if (KNO_VOIDP(name)) {
        lispval keys = kno_getkeys(bindings);
        u8_printf(out,"  env#%d %q\t\t\t(%s[%d]) %p/%p\n",
		  depth,keys,kno_type_names[btype],KNO_CHOICE_SIZE(keys),
		  bindings,env);
	kno_decref(keys);}
      else u8_printf(out,"  env#%d module %q\t\t%p\n",depth,name,env);
      kno_decref(name);
      env=env->env_parent;
      depth++;}
}

#if 0
KNO_EXPORT struct KNO_STACK *knodbg_get_stack_frame(void *arg)
{
  struct KNO_STACK *curstack=kno_stackptr;
  unsigned long long intval=KNO_LONGVAL( arg);
  if (arg==NULL)
    return curstack;
  else if ((intval < 100000) && (curstack) &&
           (intval <= (curstack->stack_depth))) {
    struct KNO_STACK *scan=curstack;
    while (scan) {
      if ( scan->stack_depth == intval )
        return scan;
      else scan=scan->stack_caller;}
    return NULL;}
  else return (kno_stack)arg;
}

KNO_EXPORT lispval knodbg_get_stack_arg(void *arg,int n)
{
  struct KNO_STACK *stack=knodbg_get_stack_frame(arg);
  if (STACK_ARGS(stack))
    if ( (n>=0) && (n < (STACK_WIDTH(stack)) ) )
      return STACK_ARGS(stack)[n];
    else return KNO_NULL;
  else return KNO_NULL;
}

KNO_EXPORT lispval knodbg_get_stack_var(void *arg,u8_string varname)
{
  struct KNO_STACK *stack=(kno_stack)knodbg_get_stack_frame(arg);
  if (KNO_STACK_LIVEP(stack)) {
    if (stack->eval_env) {
      lispval sym = kno_getsym(varname);
      return kno_symeval(sym,stack->eval_env);}}
  return KNO_NULL;
}

KNO_EXPORT lispval knodbg_find_stack_var(FILE *out,void *arg,u8_string varname)
{
  struct KNO_STACK *stack=(kno_stack)knodbg_get_stack_frame(arg);
  if (out==NULL) out=stderr;
  lispval sym = kno_getsym(varname);
  while ( (stack) && (KNO_STACK_LIVEP(stack)) ) {
    if (stack->eval_env) {
      lispval v = kno_symeval(sym,stack->eval_env);
      if (!(KNO_VOIDP(v))) {
	knodbg_concise_stack_frame(out,stack);
	return v;}}
    stack = stack->stack_caller;}
  return KNO_NULL;
}

KNO_EXPORT void knodbg_show_stack(FILE *out,void *arg,int limit)
{
  int count=0;
  if (out==NULL) out=stderr;
  struct KNO_STACK *stack=knodbg_get_stack_frame(arg);
  if (stack==NULL) {
    fprintf(out,"!! No stack\n");
    return;}
  while (stack) {
    knodbg_concise_stack_frame(out,stack);
    count++;
    stack=stack->stack_caller;
    if ( (limit > 0) && (count >= limit) ) break;}
}

KNO_EXPORT void knodbg_show_stack_env(FILE *out,void *arg)
{
  struct KNO_STACK *stack=(kno_stack)knodbg_get_stack_frame(arg);
  if (out==NULL) out=stderr;
  if (stack==NULL) {
    fprintf(out,"!! No stack frame\n");
    return;}
  else if (KNO_STACK_LIVEP(stack)) {
    fprintf(out,"!! Dead stack frame\n");
    return;}
  else {
    knodbg_concise_stack_frame(out,(kno_stack)stack);
    if (stack->eval_env)
      knodbg_show_env(out,stack->eval_env,-1);
    else fprintf(out,"!! No env\n");}
}
#endif

/* Initialization */

KNO_EXPORT void kno_init_eval_debug_c()
{
  u8_register_source_file(_FILEINFO);

  KNO_LINK_EVALFN(kno_scheme_module,dbg_evalfn);
  KNO_LINK_EVALFN(kno_scheme_module,dbg_wait_evalfn);

  KNO_LINK_EVALFN(kno_scheme_module,eval1);
  KNO_LINK_EVALFN(kno_scheme_module,eval2);
  KNO_LINK_EVALFN(kno_scheme_module,eval3);
  KNO_LINK_EVALFN(kno_scheme_module,eval4);
  KNO_LINK_EVALFN(kno_scheme_module,eval5);
  KNO_LINK_EVALFN(kno_scheme_module,eval6);
  KNO_LINK_EVALFN(kno_scheme_module,eval7);

  kno_register_config
    ("BUGDIR","Save exceptions to this directory",
     kno_sconfig_get,config_bugdir,&kno_bugdir);


  link_local_cprims();

  KNO_LINK_EVALFN(kno_scheme_module,timed_eval_evalfn);
  KNO_LINK_EVALFN(kno_scheme_module,timed_evalx_evalfn);
  KNO_LINK_EVALFN(kno_scheme_module,watchptr_evalfn);
  KNO_LINK_EVALFN(kno_scheme_module,watch_evalfn);
  KNO_LINK_EVALFN(kno_scheme_module,profiled_eval_evalfn);
  KNO_LINK_EVALFN(kno_scheme_module,watchcall_evalfn);
  KNO_LINK_EVALFN(kno_scheme_module,watchcall_plus_evalfn);
  KNO_LINK_EVALFN(kno_scheme_module,watchrefs_evalfn);

  KNO_LINK_EVALFN(kno_scheme_module,watched_cond_evalfn);
  KNO_LINK_EVALFN(kno_scheme_module,watched_try_evalfn);

  KNO_LINK_EVALFN(kno_scheme_module,expose_module_evalfn);

  /* This up to six pushes log context	` reports*/
  KNO_LINK_EVALFN(kno_scheme_module,with_log_context_evalfn);

#if USING_GOOGLE_PROFILER
  KNO_LINK_EVALFN(kno_scheme_module,gprofile_evalfn);
#endif
  kno_register_config
    ("GPROFILE","Set filename for the Google CPU profiler",
     kno_sconfig_get,kno_sconfig_set,&cpu_profilename);

  profile_symbol = kno_intern("%profile");
  unquote_symbol = kno_intern("unquote");
  else_symbol = kno_intern("else");
  apply_marker = kno_intern("=>");
  logcxt_symbol = kno_intern("logcxt");
}

static void link_local_cprims()
{
  lispval scheme_module = kno_scheme_module;

  KNO_LINK_CPRIM("_plus15",plus15,15,scheme_module);
  KNO_LINK_CPRIM("_plus14",plus14,14,scheme_module);
  KNO_LINK_CPRIM("_plus13",plus13,13,scheme_module);
  KNO_LINK_CPRIM("_plus12",plus12,12,scheme_module);
  KNO_LINK_CPRIM("_plus11",plus11,11,scheme_module);
  KNO_LINK_CPRIM("_plus10",plus10,10,scheme_module);
  KNO_LINK_CPRIM("_plus9",plus9,9,scheme_module);
  KNO_LINK_CPRIM("_plus8",plus8,8,scheme_module);
  KNO_LINK_CPRIM("_plus7",plus7,7,scheme_module);
  KNO_LINK_CPRIM("_plus6",plus6,6,scheme_module);
  KNO_LINK_CPRIM("_plus5",plus5,5,scheme_module);
  KNO_LINK_CPRIM("_plus4",plus4,4,scheme_module);
  KNO_LINK_CPRIM("list9",list9,9,scheme_module);
  KNO_LINK_CPRIM("set-log-context!",set_log_context_prim,1,scheme_module);
  KNO_LINK_CPRIM("muntrace",muntrace_prim,0,scheme_module);
  KNO_LINK_CPRIM("mtrace",mtrace_prim,1,scheme_module);
  KNO_LINK_CPRIM("%watchptrval",watchptr_prim,2,scheme_module);
  
  KNO_LINK_CPRIM("dump-bug",dumpbug_prim,2,scheme_module);
}
