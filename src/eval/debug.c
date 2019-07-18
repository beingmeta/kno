/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_CHOICES (!(KNO_AVOID_CHOICES))
#define KNO_INLINE_TABLES (!(KNO_AVOID_CHOICES))
#define KNO_INLINE_FCNIDS (!(KNO_AVOID_CHOICES))
#define KNO_INLINE_STACKS (!(KNO_AVOID_CHOICES))
#define KNO_INLINE_LEXENV (!(KNO_AVOID_CHOICES))

#define KNO_PROVIDE_FASTEVAL (!(KNO_AVOID_CHOICES))

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

#include "eval_internals.h"

#include <libu8/u8timefns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8printf.h>

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

static lispval timed_eval_evalfn(lispval expr,kno_lexenv env,kno_stack stack)
{
  lispval toeval = kno_get_arg(expr,1);
  double start = u8_elapsed_time();
  lispval value = kno_eval(toeval,env);
  double finish = u8_elapsed_time();
  u8_message(";; Executed %q in %f seconds, yielding\n;;\t%q",
             toeval,(finish-start),value);
  return value;
}

static lispval timed_evalx_evalfn(lispval expr,kno_lexenv env,kno_stack stack)
{
  lispval toeval = kno_get_arg(expr,1);
  double start = u8_elapsed_time();
  lispval value = kno_eval(toeval,env);
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

static lispval watchcall(lispval expr,kno_lexenv env,int with_proc)
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
    lispval val = kno_eval(arg,env);
    val = simplify_value(val);
    if (KNO_ABORTED(val)) {
      u8_string errstring = kno_errstring(NULL);
      i--; while (i>=0) {kno_decref(rail[i]); i--;}
      u8_free(rail);
      u8_printf(&out,"\t%q !!!> %s",arg,errstring);
      u8_logger(U8_LOG_MSG,arglabel,out.u8_outbuf);
      if (label!=dflt_label) {u8_free(label); u8_free(arglabel);}
      u8_free(out.u8_outbuf); u8_free(errstring);
      return val;}
    if ((i==0)&&(with_proc==0)&&(SYMBOLP(arg))) {}
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
  return simplify_value(result);
}

static lispval watchcall_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  return watchcall(expr,env,0);
}
static lispval watchcall_plus_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  return watchcall(expr,env,1);
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
  u8_string src_string = (KNO_VOIDP(expr)) ? (NULL) :
    (kno_lisp2string(expr));
  if (KNO_IMMEDIATEP(val)) {
    unsigned long long itype = KNO_IMMEDIATE_TYPE(val);
    unsigned long long data = KNO_IMMEDIATE_DATA(val);
    u8_string type_name = kno_type2name(itype);;
    u8_log(U8_LOG_MSG,"Pointer/Immediate",
           "%s%s%s0x%llx [ T0x%llx(%s) data=%llu ] == %q%s%s%s",
           U8OPTSTR("",label,": "),
           (KNO_LONGVAL(val)),
           itype,type_name,data,val,
           U8OPTSTR(" <== ",src_string,""));}
  else if (FIXNUMP(val))
    u8_log(U8_LOG_MSG,"Pointer/Fixnum",
           "%s%s%sFixnum","0x%llx == %d%s%s%s",
           U8OPTSTR("",label,": "),
           (KNO_LONGVAL(val)),FIX2INT(val),
           U8OPTSTR(" <== ",src_string,""));
  else if (OIDP(val)) {
    KNO_OID addr = KNO_OID_ADDR(val);
    u8_log(U8_LOG_MSG,"Pointer/OID",
           "%s%s%s0x%llx [ base=%llx off=%llx ] == %llx/%llx%s%s%s",
           U8OPTSTR("",label,": "),
           (KNO_LONGVAL(val)),
           (KNO_OID_BASE_ID(val)),(KNO_OID_BASE_OFFSET(val)),
           KNO_OID_HI(addr),KNO_OID_LO(addr),
           U8OPTSTR(" <== ",src_string,""));}
  else if (KNO_STATICP(val)) {
    kno_lisp_type ptype = KNO_CONS_TYPE((kno_cons)val);
    u8_string type_name = kno_lisp_typename(ptype);
    u8_log(U8_LOG_MSG,"Pointer/Static",
           "%s%s%s0x%llx [ T0x%llx(%s) ] == %q%s%s%s",
           U8OPTSTR("",label,": "),
           (KNO_LONGVAL(val)),
           ptype,type_name,val,
           U8OPTSTR(" <== ",src_string,""));}
  else if (CONSP(val)) {
    kno_cons c = (kno_cons) val;
    kno_lisp_type ptype = KNO_CONS_TYPE(c);
    u8_string type_name = kno_lisp_typename(ptype);
    unsigned int refcount = KNO_CONS_REFCOUNT(c);
    u8_log(U8_LOG_MSG,"Pointer/Consed",
           "%s%s%s0x%llx [ T0x%llx(%s) refs=%d ] == %q%s%s%s",
           U8OPTSTR("",label,": "),
           (KNO_LONGVAL(val)),
           ptype,type_name,refcount,val,
           U8OPTSTR(" <== ",src_string,""));}
  else {}
  if (src_string) u8_free(src_string);
}

static lispval watchptr_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval val_expr = kno_get_arg(expr,1);
  lispval value = kno_stack_eval(val_expr,env,_stack,0);
  log_ptr(value,KNO_VOID,val_expr);
  return value;
}

DEFPRIM2("%watchptrval",watchptr_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1)|KNO_NDCALL,
         "`(%WATCHPTRVAL *arg0* [*arg1*])` **undocumented**",
         kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval watchptr_prim(lispval val,lispval label_arg)
{
  log_ptr(val,label_arg,KNO_VOID);
  return kno_incref(val);
}

static lispval watchpoint_evalfn(lispval expr,kno_lexenv env,kno_stack stack)
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
                (kno_eval(towatch,env)));
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
                (kno_eval(towatch,env)));
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
    return kno_eval(toeval,env);
  else if (STRINGP(toeval)) {
    /* This is probably the 'side effect' form of %WATCH, so
       we don't bother 'timing' it. */
    return kno_incref(toeval);}
  else {
    lispval value = kno_eval(toeval,env);
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
    label_arg = kno_stack_eval(label_expr,env,_stack,0);
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
      lispval value = kno_eval_exprs(KNO_CDR(clause),env,_stack,0);
      u8_log(U8_LOG_MSG,label,"COND ? ELSE => %Q => ",KNO_CDR(clause),value);
      return value;}
    else test_val = kno_eval(KNO_CAR(clause),env);
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
          lispval fn = kno_eval(fnexpr,env);
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
        lispval value = kno_eval_exprs(KNO_CDR(clause),env,_stack,0);
        u8_log(U8_LOG_MSG,label,"COND => %q => %q",
               KNO_CAR(clause),value);
        return value;}}}
  return VOID;
}

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
    label_arg = kno_stack_eval(label_expr,env,_stack,0);
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
    value = kno_eval(clause,env);
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

static lispval dbg_evalfn(lispval expr,kno_lexenv env,kno_stack stack)
{
  lispval arg_expr=kno_get_arg(expr,1);
  lispval msg_expr=kno_get_arg(expr,2);
  lispval arg=kno_eval(arg_expr,env);
  if (VOIDP(msg_expr))
    u8_message("Debug %q",arg);
  else {
    lispval msg=kno_eval(msg_expr,env);
    if (VOIDP(msg_expr))
      u8_message("Debug %q",arg);
    else if (FALSEP(msg)) {}
    else if (VOIDP(arg))
      u8_message("Debug called");
    else u8_message("Debug (%q) %q",msg,arg);
    kno_decref(msg);}
  return _kno_dbg(arg);
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

DEFPRIM2("dump-bug",dumpbug_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
         "(DUMP-BUG *err* [*to*]) "
         "writes a DType representation of *err* to either "
         "*to* or the configured BUGDIR. If *err* is #t, "
         "returns a packet of the representation. Without "
         "*to* or if *to* is #f or #default, writes the "
         "exception into either './errors/' or './'",
         kno_exception_type,KNO_VOID,kno_any_type,KNO_VOID);
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
static lispval gprofile_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  int start = 1; char *filename = NULL;
  if ((PAIRP(KNO_CDR(expr)))&&(STRINGP(KNO_CAR(KNO_CDR(expr))))) {
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
      kno_decref(value); value = kno_eval(ex,env);
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

static lispval profiled_eval_evalfn(lispval expr,kno_lexenv env,kno_stack stack)
{
  lispval toeval = kno_get_arg(expr,1);
  double start = u8_elapsed_time();
  lispval value = kno_eval(toeval,env);
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

DEFPRIM1("mtrace",mtrace_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
         "Activates LIBC heap tracing to MALLOC_TRACE and "
         "returns true if it worked. Optional argument is a "
         "filename to set as MALLOC_TRACE",
         kno_any_type,KNO_VOID);
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

DEFPRIM("muntrace",muntrace_prim,KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
        "Deactivates LIBC heap tracing, returns true if it "
        "did anything");
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

static lispval with_log_context_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval label_expr=kno_get_arg(expr,1);
  if (KNO_VOIDP(label_expr))
    return kno_err(kno_SyntaxError,"with_context_evalfn",NULL,expr);
  else {
    lispval label=kno_stack_eval(label_expr,env,_stack,0);
    if (KNO_ABORTED(label))
      return label;
    else {
      lispval outer_lisp_context = kno_thread_get(logcxt_symbol);
      u8_string outer_context = u8_log_context;
      u8_string local_context = (KNO_SYMBOLP(label)) ? (KNO_SYMBOL_NAME(label)) :
        (KNO_STRINGP(label)) ? (KNO_CSTRING(label)) : (NULL);
      kno_thread_set(logcxt_symbol,label);
      u8_set_log_context(local_context);
      lispval result=eval_body(kno_get_body(expr,2),env,_stack,
                               "with_log_context",KNO_CSTRING(label),1);
      kno_thread_set(logcxt_symbol,outer_lisp_context);
      u8_set_log_context(outer_context);
      kno_decref(label);
      return result;}}
}

DEFPRIM1("set-log-context!",set_log_context_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
         "`(SET-LOG-CONTEXT! *label*)` "
         "sets the current log context to the string or "
         "symbol *label*.",
         kno_any_type,KNO_VOID);
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
static DONT_OPTIMIZE lispval eval1(lispval expr,kno_lexenv env,kno_stack s)
{
  lispval result = kno_stack_eval(kno_get_arg(expr,1),env,s,0);
  return result;
}
static DONT_OPTIMIZE lispval eval2(lispval expr,kno_lexenv env,kno_stack s)
{
  lispval result = kno_stack_eval(kno_get_arg(expr,1),env,s,0);
  return result;
}
static DONT_OPTIMIZE lispval eval3(lispval expr,kno_lexenv env,kno_stack s)
{
  lispval result = kno_stack_eval(kno_get_arg(expr,1),env,s,0);
  return result;
}
static DONT_OPTIMIZE lispval eval4(lispval expr,kno_lexenv env,kno_stack s)
{
  lispval result = kno_stack_eval(kno_get_arg(expr,1),env,s,0);
  return result;
}
static DONT_OPTIMIZE lispval eval5(lispval expr,kno_lexenv env,kno_stack s)
{
  lispval result = kno_stack_eval(kno_get_arg(expr,1),env,s,0);
  return result;
}
static DONT_OPTIMIZE lispval eval6(lispval expr,kno_lexenv env,kno_stack s)
{
  lispval result = kno_stack_eval(kno_get_arg(expr,1),env,s,0);
  return result;
}
static DONT_OPTIMIZE lispval eval7(lispval expr,kno_lexenv env,kno_stack s)
{
  lispval result = kno_stack_eval(kno_get_arg(expr,1),env,s,0);
  return result;
}

DEFPRIM9("list9",list9,KNO_MAX_ARGS(9)|KNO_MIN_ARGS(0),
         "Returns a nine-element list",
         kno_any_type,KNO_FALSE,kno_any_type,KNO_FALSE,
         kno_any_type,KNO_FALSE,kno_any_type,KNO_FALSE,
         kno_any_type,KNO_FALSE,kno_any_type,KNO_FALSE,
         kno_any_type,KNO_FALSE,kno_any_type,KNO_FALSE,
         kno_any_type,KNO_FALSE);
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

DEFPRIM4("_plus4",plus4,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(1),
         "Add numbers",
         kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
         kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0));
static lispval plus4(lispval arg1,lispval arg2,
                     lispval arg3,lispval arg4)
{
  int sum = KNO_FIX2INT(arg1) + KNO_FIX2INT(arg2) + KNO_FIX2INT(arg3) +
    KNO_FIX2INT(arg4);
  return KNO_INT(sum);
}

DEFPRIM5("_plus5",plus5,KNO_MAX_ARGS(5)|KNO_MIN_ARGS(1),
         "Add numbers",
         kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
         kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
         kno_any_type,KNO_CPP_INT(0));
static lispval plus5(lispval arg1,lispval arg2,
                     lispval arg3,lispval arg4,
                     lispval arg5)
{
  int sum = KNO_FIX2INT(arg1) + KNO_FIX2INT(arg2) + KNO_FIX2INT(arg3) +
    KNO_FIX2INT(arg4) + KNO_FIX2INT(arg5);
  return KNO_INT(sum);
}

DEFPRIM6("_plus6",plus6,KNO_MAX_ARGS(6)|KNO_MIN_ARGS(2),
         "Add numbers",
         kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
         kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
         kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0));
static lispval plus6(lispval arg1,lispval arg2,
                     lispval arg3,lispval arg4,
                     lispval arg5,lispval arg6)
{
  int sum = KNO_FIX2INT(arg1) + KNO_FIX2INT(arg2) + KNO_FIX2INT(arg3) +
    KNO_FIX2INT(arg4) + KNO_FIX2INT(arg5) + KNO_FIX2INT(arg6);
  return KNO_INT(sum);
}

DEFPRIM7("_plus7",plus7,KNO_MAX_ARGS(7)|KNO_MIN_ARGS(3),
         "Add numbers",
         kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
         kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
         kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
         kno_any_type,KNO_CPP_INT(0));
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

DEFPRIM8("_plus8",plus8,KNO_MAX_ARGS(8)|KNO_MIN_ARGS(3),
         "Add numbers",
         kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
         kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
         kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
         kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0));
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

DEFPRIM9("_plus9",plus9,KNO_MAX_ARGS(9)|KNO_MIN_ARGS(3),
         "Add numbers",
         kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
         kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
         kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
         kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
         kno_any_type,KNO_CPP_INT(0));
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

DEFPRIM10("_plus10",plus10,KNO_MAX_ARGS(10)|KNO_MIN_ARGS(3),
          "Add numbers",
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0));
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

DEFPRIM11("_plus11",plus11,KNO_MAX_ARGS(11)|KNO_MIN_ARGS(3),
          "Add numbers",
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
          kno_any_type,KNO_CPP_INT(0));
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

DEFPRIM12("_plus12",plus12,KNO_MAX_ARGS(12)|KNO_MIN_ARGS(3),
          "Add numbers",
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0));
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

DEFPRIM13("_plus13",plus13,KNO_MAX_ARGS(13)|KNO_MIN_ARGS(3),
          "Add numbers",
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
          kno_any_type,KNO_CPP_INT(0));
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

DEFPRIM14("_plus14",plus14,KNO_MAX_ARGS(14)|KNO_MIN_ARGS(3),
          "Add numbers",
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0));
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

DEFPRIM15("_plus15",plus15,KNO_MAX_ARGS(15)|KNO_MIN_ARGS(3),
          "Add numbers",
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
          kno_any_type,KNO_CPP_INT(0),kno_any_type,KNO_CPP_INT(0),
          kno_any_type,KNO_CPP_INT(0));
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

/* Initialization */

KNO_EXPORT void kno_init_eval_debug_c()
{
  u8_register_source_file(_FILEINFO);

  kno_def_evalfn(kno_scheme_module,"DBG","",dbg_evalfn);

  kno_def_evalfn(kno_scheme_module,"EVAL1","",eval1);
  kno_def_evalfn(kno_scheme_module,"EVAL2","",eval2);
  kno_def_evalfn(kno_scheme_module,"EVAL3","",eval3);
  kno_def_evalfn(kno_scheme_module,"EVAL4","",eval4);
  kno_def_evalfn(kno_scheme_module,"EVAL5","",eval5);
  kno_def_evalfn(kno_scheme_module,"EVAL6","",eval6);
  kno_def_evalfn(kno_scheme_module,"EVAL7","",eval7);

  kno_register_config
    ("BUGDIR","Save exceptions to this directory",
     kno_sconfig_get,config_bugdir,&kno_bugdir);


  link_local_cprims();

  kno_def_evalfn(kno_scheme_module,"TIMEVAL","",timed_eval_evalfn);
  kno_def_evalfn(kno_scheme_module,"%TIMEVAL","",timed_evalx_evalfn);
  kno_def_evalfn(kno_scheme_module,"%WATCHPTR","",watchptr_evalfn);
  kno_def_evalfn(kno_scheme_module,"%WATCH","",watchpoint_evalfn);
  kno_def_evalfn(kno_scheme_module,"PROFILE","",profiled_eval_evalfn);
  kno_def_evalfn(kno_scheme_module,"%WATCHCALL","",watchcall_evalfn);
  kno_defalias(kno_scheme_module,"%WC","%WATCHCALL");
  kno_def_evalfn(kno_scheme_module,"%WATCHCALL+","",watchcall_plus_evalfn);
  kno_defalias(kno_scheme_module,"%WC+","%WATCHCALL+");

  kno_def_evalfn(kno_scheme_module,"%WCOND",
                 "Reports (watches) which branch of a COND was taken",
                 watched_cond_evalfn);
  kno_def_evalfn(kno_scheme_module,"%WTRY",
                 "Reports (watches) which clause of a TRY succeeded",
                 watched_try_evalfn);

  /* This pushes a log context */
  kno_def_evalfn(kno_scheme_module,"WITH-LOG-CONTEXT","",with_log_context_evalfn);
#if USING_GOOGLE_PROFILER
  kno_def_evalfn(kno_scheme_module,"GOOGLE/PROFILE","",gprofile_evalfn);
  kno_idefn(kno_scheme_module,
            kno_make_cprim0("GOOGLE/PROFILE/STOP",gprofile_stop));
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

  KNO_LINK_PRIM("_plus15",plus15,15,scheme_module);
  KNO_LINK_PRIM("_plus14",plus14,14,scheme_module);
  KNO_LINK_PRIM("_plus13",plus13,13,scheme_module);
  KNO_LINK_PRIM("_plus12",plus12,12,scheme_module);
  KNO_LINK_PRIM("_plus11",plus11,11,scheme_module);
  KNO_LINK_PRIM("_plus10",plus10,10,scheme_module);
  KNO_LINK_PRIM("_plus9",plus9,9,scheme_module);
  KNO_LINK_PRIM("_plus8",plus8,8,scheme_module);
  KNO_LINK_PRIM("_plus7",plus7,7,scheme_module);
  KNO_LINK_PRIM("_plus6",plus6,6,scheme_module);
  KNO_LINK_PRIM("_plus5",plus5,5,scheme_module);
  KNO_LINK_PRIM("_plus4",plus4,4,scheme_module);
  KNO_LINK_PRIM("list9",list9,9,scheme_module);
  KNO_LINK_PRIM("set-log-context!",set_log_context_prim,1,scheme_module);
  KNO_LINK_PRIM("muntrace",muntrace_prim,0,scheme_module);
  KNO_LINK_PRIM("mtrace",mtrace_prim,1,scheme_module);
  KNO_LINK_PRIM("%watchptrval",watchptr_prim,2,scheme_module);

  KNO_LINK_PRIM("dump-bug",dumpbug_prim,2,scheme_module);
}
