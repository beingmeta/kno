/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_CHOICES 1
#define FD_INLINE_TABLES 1
#define FD_INLINE_FCNIDS 1
#define FD_INLINE_STACKS 1
#define FD_INLINE_LEXENV 1

#define FD_PROVIDE_FASTEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/support.h"
#include "framerd/storage.h"
#include "framerd/eval.h"
#include "framerd/dtproc.h"
#include "framerd/numbers.h"
#include "framerd/sequences.h"
#include "framerd/ports.h"
#include "framerd/dtcall.h"
#include "framerd/ffi.h"

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
u8_string fd_bugdir = NULL;

static lispval unquote_symbol, else_symbol, apply_marker;

/* Trace functions */

static lispval timed_eval_evalfn(lispval expr,fd_lexenv env,fd_stack stack)
{
  lispval toeval = fd_get_arg(expr,1);
  double start = u8_elapsed_time();
  lispval value = fd_eval(toeval,env);
  double finish = u8_elapsed_time();
  u8_message(";; Executed %q in %f seconds, yielding\n;;\t%q",
             toeval,(finish-start),value);
  return value;
}

static lispval timed_evalx_evalfn(lispval expr,fd_lexenv env,fd_stack stack)
{
  lispval toeval = fd_get_arg(expr,1);
  double start = u8_elapsed_time();
  lispval value = fd_eval(toeval,env);
  double finish = u8_elapsed_time();
  if (FD_ABORTED(value)) {
    u8_exception ex = u8_erreify();
    lispval exception = fd_wrap_exception(ex);
    return fd_make_nvector(2,exception,fd_init_double(NULL,finish-start));}
  else return fd_make_nvector(2,value,fd_init_double(NULL,finish-start));
}

/* Inserts a \n\t line break if the current output line is longer
    than max_len.  *off*, if > 0, is the last offset on the current line,
    which is where the line break goes if inserted;  */
static int check_line_length(u8_output out,int off,int max_len)
{
  u8_byte *start = out->u8_outbuf, *end = out->u8_write, *scanner = end;
  int scan_off, line_len, len = end-start;
  while ((scanner>start)&&((*scanner)!='\n')) scanner--;
  line_len = end-scanner; scan_off = scanner-start;
  /* If the current line is less than max_len, return the current offset */
  if (line_len<max_len) return len;
  /* If the offset is non-positive, the last item was the first
     item on a line and gets the whole line to itself, so we still
     return the current offset.  We don't insert a \n\t now because
     it might be the last item output. */
  else if (off<=0)
    return len;
  else {
    /* TODO: abstract out u8_need_len, abstract out whole "if longer
       than" logic */
    /* The line is too long, insert a \n\t at off */
    if ((end+5)>(out->u8_outlim)) {
      /* Grow the stream if needed */
      if (u8_grow_stream((u8_stream)out,U8_BUF_MIN_GROW)<=0)
        return -1;
      start = out->u8_outbuf; end = out->u8_write;
      scanner = start+scan_off;}
    /* Use memmove because it's overlapping */
    memmove(start+off+2,start+off,len-off);
    start[off]='\n'; start[off+1]='\t';
    out->u8_write = out->u8_write+2;
    start[len+2]='\0';
    return out->u8_write-(start+off+2);}
}

static lispval watchcall(lispval expr,fd_lexenv env,int with_proc)
{
  struct U8_OUTPUT out;
  u8_string dflt_label="%CALL", label = dflt_label, arglabel="%ARG";
  lispval watch, head = fd_get_arg(expr,1), *rail, result = EMPTY;
  int i = 0, n_args, expr_len = fd_seq_length(expr);
  if (VOIDP(head))
    return fd_err(fd_SyntaxError,"%WATCHCALL",NULL,expr);
  else if (STRINGP(head)) {
    if (expr_len==2)
      return fd_err(fd_SyntaxError,"%WATCHCALL",NULL,expr);
    else {
      label = u8_strdup(CSTRING(head));
      arglabel = u8_mkstring("%s/ARG",CSTRING(head));
      if ((expr_len>3)||(FD_APPLICABLEP(fd_get_arg(expr,2)))) {
        watch = fd_get_body(expr,2);}
      else watch = fd_get_arg(expr,2);}}
  else if ((expr_len==2)&&(PAIRP(head)))
    watch = head;
  else watch = fd_get_body(expr,1);
  n_args = fd_seq_length(watch);
  rail = u8_alloc_n(n_args,lispval);
  U8_INIT_OUTPUT(&out,1024);
  u8_printf(&out,"Watched call %q",watch);
  u8_logger(U8_LOG_MSG,label,out.u8_outbuf);
  out.u8_write = out.u8_outbuf;
  while (i<n_args) {
    lispval arg = fd_get_arg(watch,i);
    lispval val = fd_eval(arg,env);
    val = simplify_value(val);
    if (FD_ABORTED(val)) {
      u8_string errstring = fd_errstring(NULL);
      i--; while (i>=0) {fd_decref(rail[i]); i--;}
      u8_free(rail);
      u8_printf(&out,"\t%q !!!> %s",arg,errstring);
      u8_logger(U8_LOG_MSG,arglabel,out.u8_outbuf);
      if (label!=dflt_label) {u8_free(label); u8_free(arglabel);}
      u8_free(out.u8_outbuf); u8_free(errstring);
      return val;}
    if ((i==0)&&(with_proc==0)&&(SYMBOLP(arg))) {}
    else if (FD_EVALP(arg)) {
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
      lispval r = fd_apply(fn,n_args-1,rail+1);
      if (FD_ABORTED(r)) {
        u8_string errstring = fd_errstring(NULL);
        i--; while (i>=0) {fd_decref(rail[i]); i--;}
        u8_free(rail);
        fd_decref(result);
        if (label!=dflt_label) {u8_free(label); u8_free(arglabel);}
        u8_free(out.u8_outbuf); u8_free(errstring);
        return r;}
      else {CHOICE_ADD(result,r);}}}
  else result = fd_apply(rail[0],n_args-1,rail+1);
  if (FD_ABORTED(result)) {
    u8_string errstring = fd_errstring(NULL);
    u8_printf(&out,"%q !!!> %s",watch,errstring);
    u8_free(errstring);}
  else u8_printf(&out,"%q ===> %q",watch,result);
  u8_logger(U8_LOG_MSG,label,out.u8_outbuf);
  i--; while (i>=0) {fd_decref(rail[i]); i--;}
  u8_free(rail);
  if (label!=dflt_label) {u8_free(label); u8_free(arglabel);}
  u8_free(out.u8_outbuf);
  return simplify_value(result);
}

static lispval watchcall_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  return watchcall(expr,env,0);
}
static lispval watchcall_plus_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
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
  else if ((FD_BIGINTP(arg))&&(fd_modest_bigintp((fd_bigint)arg)))
    return u8_write_long_long
      (fd_bigint2int64((fd_bigint)arg),buf,buflen);
  else return NULL;
}

static void log_ptr(lispval val,lispval label_arg,lispval expr)
{
  u8_byte buf[64];
  u8_string label = get_label(label_arg,buf,64);
  u8_string src_string = (FD_VOIDP(expr)) ? (NULL) :
    (fd_lisp2string(expr));
  if (FD_IMMEDIATEP(val)) {
    unsigned long long itype = FD_IMMEDIATE_TYPE(val);
    unsigned long long data = FD_IMMEDIATE_DATA(val);
    u8_string type_name = fd_type2name(itype);;
    u8_log(U8_LOG_MSG,"Pointer/Immediate",
           "%s%s%s0x%llx [ T0x%llx(%s) data=%llu ] == %q%s%s%s",
           U8OPTSTR("",label,": "),
           ((unsigned long long)val),
           itype,type_name,data,val,
           U8OPTSTR(" <== ",src_string,""));}
  else if (FIXNUMP(val))
    u8_log(U8_LOG_MSG,"Pointer/Fixnum",
           "%s%s%sFixnum","0x%llx == %d%s%s%s",
           U8OPTSTR("",label,": "),
           ((unsigned long long)val),FIX2INT(val),
           U8OPTSTR(" <== ",src_string,""));
  else if (OIDP(val)) {
    FD_OID addr = FD_OID_ADDR(val);
    u8_log(U8_LOG_MSG,"Pointer/OID",
           "%s%s%s0x%llx [ base=%llx off=%llx ] == %llx/%llx%s%s%s",
           U8OPTSTR("",label,": "),
           ((unsigned long long)val),
           (FD_OID_BASE_ID(val)),(FD_OID_BASE_OFFSET(val)),
           FD_OID_HI(addr),FD_OID_LO(addr),
           U8OPTSTR(" <== ",src_string,""));}
  else if (FD_STATICP(val)) {
    fd_ptr_type ptype = FD_CONS_TYPE((fd_cons)val);
    u8_string type_name = fd_ptr_typename(ptype);
    u8_log(U8_LOG_MSG,"Pointer/Static",
           "%s%s%s0x%llx [ T0x%llx(%s) ] == %q%s%s%s",
           U8OPTSTR("",label,": "),
           ((unsigned long long)val),
           ptype,type_name,val,
           U8OPTSTR(" <== ",src_string,""));}
  else if (CONSP(val)) {
    fd_cons c = (fd_cons) val;
    fd_ptr_type ptype = FD_CONS_TYPE(c);
    u8_string type_name = fd_ptr_typename(ptype);
    unsigned int refcount = FD_CONS_REFCOUNT(c);
    u8_log(U8_LOG_MSG,"Pointer/Consed",
           "%s%s%s0x%llx [ T0x%llx(%s) refs=%d ] == %q%s%s%s",
           U8OPTSTR("",label,": "),
           ((unsigned long long)val),
           ptype,type_name,refcount,val,
           U8OPTSTR(" <== ",src_string,""));}
  else {}
  if (src_string) u8_free(src_string);
}

static lispval watchptr_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval val_expr = fd_get_arg(expr,1);
  lispval value = fd_stack_eval(val_expr,env,_stack,0);
  log_ptr(value,FD_VOID,val_expr);
  return value;
}

static lispval watchptr_prim(lispval val,lispval label_arg)
{
  log_ptr(val,label_arg,FD_VOID);
  return fd_incref(val);
}

static lispval watchpoint_evalfn(lispval expr,fd_lexenv env,fd_stack stack)
{
  lispval toeval = fd_get_arg(expr,1);
  double start; int oneout = 0;
  lispval scan = FD_CDR(expr);
  u8_string label="%WATCH";
  if ((PAIRP(toeval))) {
    /* EXPR "label" . watchexprs */
    scan = FD_CDR(scan);
    if ((PAIRP(scan)) && (STRINGP(FD_CAR(scan)))) {
      label = CSTRING(FD_CAR(scan)); scan = FD_CDR(scan);}}
  else if (STRINGP(toeval)) {
    /* "label" . watchexprs, no expr, call should be side-effect */
    label = CSTRING(toeval); scan = FD_CDR(scan);}
  else if (SYMBOLP(toeval)) {
    /* If the first argument is a symbol, we change the label and
       treat all the arguments as context variables and output
       them.  */
    label="%WATCHED";}
  else scan = FD_CDR(scan);
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
      lispval towatch = FD_CAR(scan), wval = VOID;
      if ((STRINGP(towatch)) && (FD_STRLEN(towatch) > 32)) {
        u8_printf(&out," %s ",FD_CSTRING(towatch));
        scan = FD_CDR(scan);}
      else if ((STRINGP(towatch)) && (PAIRP(FD_CDR(scan)))) {
        lispval label = towatch; u8_string lbl = CSTRING(label);
        towatch = FD_CAR(FD_CDR(scan)); scan = FD_CDR(FD_CDR(scan));
        wval = ((SYMBOLP(towatch))?(fd_symeval(towatch,env)):
              (fd_eval(towatch,env)));
        if (FD_ABORTED(wval)) {
          u8_exception ex = u8_erreify();
          wval = fd_wrap_exception(ex);}
        if (lbl[0]=='\n') {
          if (FD_EMPTYP(wval)) {
            if (oneout) u8_puts(&out," // "); else oneout = 1;
            u8_printf(&out," %s={}",lbl+1);}
          else {
            if (off > 2) {
              u8_printf(&out,"\n ");
              off=1;}
            if (oneout) u8_puts(&out," // "); else oneout = 1;
            if ( (FD_PAIRP(wval)) || (FD_VECTORP(wval)) || (FD_CHOICEP(wval)) ||
                 (FD_SLOTMAPP(wval)) || (FD_SCHEMAPP(wval)) ) {
              fd_list_object(&out,wval,lbl+1,NULL,"  ",NULL,100,0);
              if (PAIRP(scan)) {u8_puts(&out,"\n "); off = 0;}}
            else {
              u8_printf(&out,"%s=%q",lbl+1,wval);
              if (PAIRP(scan)) {u8_puts(&out,"\n "); off = 0;}
              off = check_line_length(&out,off,100);}}}
        else {
          if (oneout) u8_puts(&out," // "); else oneout = 1;
          u8_printf(&out,"%s=%q",CSTRING(label),wval);
          off = check_line_length(&out,off,100);}
        fd_decref(wval); wval = VOID;}
      else {
        wval = ((SYMBOLP(towatch))?(fd_symeval(towatch,env)):
              (fd_eval(towatch,env)));
        if (FD_ABORTED(wval)) {
          u8_exception ex = u8_erreify();
          wval = fd_wrap_exception(ex);}
        scan = FD_CDR(scan);
        if (oneout) u8_printf(&out," // %q=%q",towatch,wval);
        else {
          u8_printf(&out,"%q=%q",towatch,wval);
          oneout = 1;}
        off = check_line_length(&out,off,50);
        fd_decref(wval); wval = VOID;}}
    u8_logger(U8_LOG_MSG,label,out.u8_outbuf);
    u8_free(out.u8_outbuf);}
  start = u8_elapsed_time();
  if (SYMBOLP(toeval))
    return fd_eval(toeval,env);
  else if (STRINGP(toeval)) {
    /* This is probably the 'side effect' form of %WATCH, so
       we don't bother 'timing' it. */
    return fd_incref(toeval);}
  else {
    lispval value = fd_eval(toeval,env);
    double howlong = u8_elapsed_time()-start;
    if (FD_ABORTED(value)) {
      u8_exception ex = u8_erreify();
      value = fd_wrap_exception(ex);}
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

static lispval watched_cond_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval label_arg = FD_VOID;
  u8_string label="%WCOND";
  lispval clauses = FD_CDR(expr);
  if (FD_STRINGP(FD_CAR(clauses))) {
    label = FD_CSTRING(label);
    clauses = FD_CDR(clauses);}
  else if ( (FD_PAIRP(FD_CAR(clauses))) &&
            (FD_PAIRP(FD_CDR(FD_CAR(clauses)))) &&
            (FD_EQ(FD_CAR(FD_CAR(clauses)),unquote_symbol))) {
    lispval label_expr = FD_CADR(FD_CAR(clauses));
    clauses = FD_CDR(clauses);
    label_arg = fd_stack_eval(label_expr,env,_stack,0);
    if (FD_ABORTP(label_arg)) {
      u8_exception ex = u8_erreify();
      u8_string errstring = fd_errstring(ex);
      u8_log(LOG_ERR,"BadWatchLabel",
             "Watch label '%q' returned an error (%s)",
             label_expr,errstring);
      u8_free(errstring);
      u8_free_exception(ex,0);
      label_arg = FD_VOID;}
    else if (FD_STRINGP(label_arg)) {
      int len = FD_STRLEN(label_arg);
      u8_byte *tmplabel = alloca(len+1);
      strcpy(tmplabel,FD_CSTRING(label_arg));
      label = tmplabel;}
    else u8_log(LOG_ERR,"BadWatchLabel",
                "Watch label '%q' value %q wasn't a string",
                label_expr,label_arg);
    fd_decref(label_arg);}
  FD_DOLIST(clause,FD_CDR(expr)) {
    lispval test_val;
    if (!(PAIRP(clause)))
      return fd_err(fd_SyntaxError,_("invalid cond clause"),NULL,expr);
    else if (FD_EQ(FD_CAR(clause),else_symbol)) {
      u8_log(U8_LOG_MSG,label,"COND ? ELSE => %Q",FD_CDR(clause));
      lispval value = fd_eval_exprs(FD_CDR(clause),env,_stack,0);
      u8_log(U8_LOG_MSG,label,"COND ? ELSE => %Q => ",FD_CDR(clause),value);
      return value;}
    else test_val = fd_eval(FD_CAR(clause),env);
    if (FD_ABORTED(test_val)) return test_val;
    else if (FALSEP(test_val)) {}
    else {
      u8_log(U8_LOG_MSG,label,"COND => %q => %Q",
             FD_CAR(clause),FD_CDR(clause));
      lispval applyp =
        ((PAIRP(FD_CDR(clause))) &&
         (FD_EQ(FD_CAR(FD_CDR(clause)),apply_marker)));
      if (applyp) {
        if (PAIRP(FD_CDR(FD_CDR(clause)))) {
          lispval fnexpr = FD_CAR(FD_CDR(FD_CDR(clause)));
          lispval fn = fd_eval(fnexpr,env);
          if (FD_ABORTED(fn)) {
            fd_decref(test_val);
            return fn;}
          else if (FD_APPLICABLEP(fn)) {
            lispval retval = fd_apply(fn,1,&test_val);
            fd_decref(test_val); fd_decref(fn);
            return retval;}
          else {
            fd_decref(test_val);
            return fd_type_error("function","watched_cond_evalfn",fn);}}
        else return fd_err
               (fd_SyntaxError,"watched_cond_evalfn","apply syntax",expr);}
      else {
        fd_decref(test_val);
        lispval value = fd_eval_exprs(FD_CDR(clause),env,_stack,0);
        u8_log(U8_LOG_MSG,label,"COND => %q => %q",
               FD_CAR(clause),value);
        return value;}}}
  return VOID;
}

static lispval watched_try_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval label_arg = FD_VOID;
  u8_string label="%WTRY";
  lispval clauses = FD_CDR(expr);
  if (FD_STRINGP(FD_CAR(clauses))) {
    label = FD_CSTRING(label);
    clauses = FD_CDR(clauses);}
  else if ( (FD_PAIRP(FD_CAR(clauses))) &&
            (FD_PAIRP(FD_CDR(FD_CAR(clauses)))) &&
            (FD_EQ(FD_CAR(FD_CAR(clauses)),unquote_symbol))) {
    lispval label_expr = FD_CADR(FD_CAR(clauses));
    clauses = FD_CDR(clauses);
    label_arg = fd_stack_eval(label_expr,env,_stack,0);
    if (FD_ABORTP(label_arg)) {
      u8_exception ex = u8_erreify();
      u8_string errstring = fd_errstring(ex);
      u8_log(LOG_ERR,"BadWatchLabel",
             "Watch label '%q' returned an error (%s)",
             label_expr,errstring);
      u8_free(errstring);
      u8_free_exception(ex,0);
      label_arg = FD_VOID;}
    else if (FD_STRINGP(label_arg)) {
      int len = FD_STRLEN(label_arg);
      u8_byte *tmplabel = alloca(len+1);
      strcpy(tmplabel,FD_CSTRING(label_arg));
      label = tmplabel;}
    else u8_log(LOG_ERR,"BadWatchLabel",
                "Watch label '%q' value %q wasn't a string",
                label_expr,label_arg);
    fd_decref(label_arg);}
  lispval value = EMPTY;
  FD_DOLIST(clause,clauses) {
    int ipe_state = fd_ipeval_status();
    fd_decref(value);
    value = fd_eval(clause,env);
    if (FD_ABORTED(value))
      return value;
    else if (VOIDP(value)) {
      fd_seterr(fd_VoidArgument,"try_evalfn",NULL,clause);
      return FD_ERROR;}
    else if (!(EMPTYP(value))) {
      u8_log(U8_LOG_MSG,label,"TRY %q ==> %q",clause,value);
      return value;}
    else if (fd_ipeval_status()!=ipe_state)
      return value;}
  return value;
}

/* Debugging assistance */

FD_EXPORT lispval _fd_dbg(lispval x)
{
  lispval result=_fd_debug(x);
  if (result == x)
    return result;
  else {
    fd_incref(result);
    fd_decref(x);
    return result;}
}

int (*fd_dump_exception)(lispval ex);

static lispval dbg_evalfn(lispval expr,fd_lexenv env,fd_stack stack)
{
  lispval arg_expr=fd_get_arg(expr,1);
  lispval msg_expr=fd_get_arg(expr,2);
  lispval arg=fd_eval(arg_expr,env);
  if (VOIDP(msg_expr))
    u8_message("Debug %q",arg);
  else {
    lispval msg=fd_eval(msg_expr,env);
    if (VOIDP(msg_expr))
      u8_message("Debug %q",arg);
    else if (FALSEP(msg)) {}
    else if (VOIDP(arg))
      u8_message("Debug called");
    else u8_message("Debug (%q) %q",msg,arg);
    fd_decref(msg);}
  return _fd_dbg(arg);
}

/* Recording bugs */

FD_EXPORT int fd_dump_bug(lispval ex,u8_string into)
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
  struct FD_EXCEPTION *exo = (fd_exception) ex;
  int rv = fd_write_dtype_to_file(ex,bugpath);
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

FD_EXPORT int fd_record_bug(lispval ex)
{
  if (fd_bugdir == NULL)
    return 0;
  else return fd_dump_bug(ex,fd_bugdir);
}

FD_EXPORT lispval dumpbug_prim(lispval ex,lispval where)
{
  if (FD_TRUEP(where)) {
    struct FD_OUTBUF bugout; FD_INIT_BYTE_OUTPUT(&bugout,16000);
    int rv = fd_write_dtype(&bugout,ex);
    if (rv<0) {
      fd_close_outbuf(&bugout);
      return FD_ERROR;}
    else {
      lispval packet = fd_make_packet(NULL,BUFIO_POINT(&bugout),bugout.buffer);
      fd_close_outbuf(&bugout);
      return packet;}}
  else if ( (FD_VOIDP(where)) || (FD_FALSEP(where)) || (FD_DEFAULTP(where)) ) {
    u8_string bugdir = fd_bugdir;
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
    int rv = fd_dump_bug(ex,bugdir);
    if (rv<0)
      fd_seterr("RECORD-BUG failed","record_bug",NULL,where);
    if (bugdir != fd_bugdir) u8_free(bugdir);
    if (rv<0) return FD_ERROR;
    else return FD_TRUE;}
  else if (FD_STRINGP(where)) {
    int rv = fd_dump_bug(ex,FD_CSTRING(where));
    if (rv<0) {
      fd_seterr("RECORD-BUG failed","record_bug",
                FD_CSTRING(where),ex);
      return FD_ERROR;}
    else return FD_TRUE;}
  else return fd_type_error("filename","record_bug",where);
}

static int config_bugdir(lispval var,lispval val,void *state)
{
  if (FD_FALSEP(val)) {
    fd_dump_exception = NULL;
    u8_free(fd_bugdir);
    fd_bugdir = NULL;
    return 0;}
  else if ( ( fd_dump_exception == NULL ) ||
            ( fd_dump_exception == fd_record_bug ) ) {
    if (FD_STRINGP(val)) {
      u8_string old = fd_bugdir;
      u8_string new = FD_CSTRING(val);
      if (u8_directoryp(new)) {}
      else if (u8_file_existsp(new)) {
        fd_seterr("not a directory","config_bugdir",u8_strdup(new),VOID);
        return -1;}
      else {
        int rv = u8_mkdirs(new,0664);
        if (rv<0) {
          u8_graberrno("config_bugdir/mkdir",u8_strdup(new));
          u8_seterr(_("missing directory"),"config_bugdir",u8_strdup(new));
          return -1;}}
      fd_bugdir = u8_strdup(new);
      if (old) u8_free(old);
      fd_dump_exception = fd_record_bug;
      return 1;}
    else {
      fd_seterr(fd_TypeError,"config_bugdir",u8_strdup("pathstring"),val);
      return -1;}}
  else {
    u8_seterr("existing bug handler","config_bugdir",NULL);
    return -1;}
}

/* Google profiler usage */

#if USING_GOOGLE_PROFILER
static lispval gprofile_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  int start = 1; char *filename = NULL;
  if ((PAIRP(FD_CDR(expr)))&&(STRINGP(FD_CAR(FD_CDR(expr))))) {
    filename = u8_strdup(CSTRING(FD_CAR(FD_CDR(expr))));
    start = 2;}
  else if ((PAIRP(FD_CDR(expr)))&&(SYMBOLP(FD_CAR(FD_CDR(expr))))) {
    lispval val = fd_symeval(FD_CADR(expr),env);
    if (FD_ABORTED(val)) return val;
    else if (STRINGP(val)) {
      filename = u8_strdup(CSTRING(val));
      fd_decref(val);
      start = 2;}
    else return fd_type_error("filename","GOOGLE/PROFILE",val);}
  else if (cpu_profilename)
    filename = u8_strdup(cpu_profilename);
  else if (getenv("CPUPROFILE"))
    filename = u8_strdup(getenv("CPUPROFILE"));
  else filename = u8_mkstring("/tmp/gprof%ld.pid",(long)getpid());
  ProfilerStart(filename); {
    lispval value = VOID;
    lispval body = fd_get_body(expr,start);
    FD_DOLIST(ex,body) {
      fd_decref(value); value = fd_eval(ex,env);
      if (FD_ABORTED(value)) {
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

static lispval profiled_eval_evalfn(lispval expr,fd_lexenv env,fd_stack stack)
{
  lispval toeval = fd_get_arg(expr,1);
  double start = u8_elapsed_time();
  lispval value = fd_eval(toeval,env);
  if (FD_ABORTED(value)) return value;
  double finish = u8_elapsed_time();
  lispval tag = fd_get_arg(expr,2);
  lispval profile_info = fd_symeval(profile_symbol,env), profile_data;
  if (VOIDP(profile_info)) return value;
  profile_data = fd_get(profile_info,tag,VOID);
  if (FD_ABORTED(profile_data)) {
    fd_decref(value); fd_decref(profile_info);
    return profile_data;}
  else if (VOIDP(profile_data)) {
    lispval time = fd_init_double(NULL,(finish-start));
    profile_data = fd_conspair(FD_INT(1),time);
    fd_store(profile_info,tag,profile_data);}
  else {
    struct FD_PAIR *p = fd_consptr(fd_pair,profile_data,fd_pair_type);
    struct FD_FLONUM *d = fd_consptr(fd_flonum,(p->cdr),fd_flonum_type);
    p->car = FD_INT(fd_getint(p->car)+1);
    d->floval = d->floval+(finish-start);}
  fd_decref(profile_data); fd_decref(profile_info);
  return value;
}

/* MTrace */

#if HAVE_MTRACE
static int mtracing=0;
#endif

static lispval mtrace_prim(lispval arg)
{
  /* TODO: Check whether we're running under libc malloc (so mtrace
     will do anything). */
#if HAVE_MTRACE
  if (mtracing)
    return FD_TRUE;
  else if (getenv("MALLOC_TRACE")) {
    mtrace();
    mtracing=1;
    return FD_TRUE;}
  else if ((STRINGP(arg)) &&
           (u8_file_writablep(CSTRING(arg)))) {
    setenv("MALLOC_TRACE",CSTRING(arg),1);
    mtrace();
    mtracing=1;
    return FD_TRUE;}
  else return FD_FALSE;
#else
  return FD_FALSE;
#endif
}

static lispval muntrace_prim()
{
#if HAVE_MTRACE
  if (mtracing) {
    muntrace();
    return FD_TRUE;}
  else return FD_FALSE;
#else
  return FD_FALSE;
#endif
}

/* WITH-CONTEXT */

static lispval with_log_context_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval label_expr=fd_get_arg(expr,1);
  if (FD_VOIDP(label_expr))
    return fd_err(fd_SyntaxError,"with_context_evalfn",NULL,expr);
  else {
    lispval label=fd_stack_eval(label_expr,env,_stack,0);
    if (FD_ABORTED(label)) return label;
    else if (!(FD_STRINGP(label)))
      return fd_type_error("string","with_context_evalfn",label);
    else {
      u8_string outer_context=u8_log_context;
      u8_set_log_context(FD_CSTRING(label));
      lispval result=eval_inner_body("with_log_context",FD_CSTRING(label),
                               expr,2,env,_stack);
      u8_set_log_context(outer_context);
      fd_decref(label);
      return result;}}
}

/* Primitives for testing purposes */

#if __clang__
#define DONT_OPTIMIZE  __attribute__((optnone))
#else
#define DONT_OPTIMIZE __attribute__((optimize("O0")))
#endif

/* These are for wrapping around Scheme code to see in C profilers */
static DONT_OPTIMIZE lispval eval1(lispval expr,fd_lexenv env,fd_stack s)
{
  lispval result = fd_stack_eval(fd_get_arg(expr,1),env,s,0);
  return result;
}
static DONT_OPTIMIZE lispval eval2(lispval expr,fd_lexenv env,fd_stack s)
{
  lispval result = fd_stack_eval(fd_get_arg(expr,1),env,s,0);
  return result;
}
static DONT_OPTIMIZE lispval eval3(lispval expr,fd_lexenv env,fd_stack s)
{
  lispval result = fd_stack_eval(fd_get_arg(expr,1),env,s,0);
  return result;
}
static DONT_OPTIMIZE lispval eval4(lispval expr,fd_lexenv env,fd_stack s)
{
  lispval result = fd_stack_eval(fd_get_arg(expr,1),env,s,0);
  return result;
}
static DONT_OPTIMIZE lispval eval5(lispval expr,fd_lexenv env,fd_stack s)
{
  lispval result = fd_stack_eval(fd_get_arg(expr,1),env,s,0);
  return result;
}
static DONT_OPTIMIZE lispval eval6(lispval expr,fd_lexenv env,fd_stack s)
{
  lispval result = fd_stack_eval(fd_get_arg(expr,1),env,s,0);
  return result;
}
static DONT_OPTIMIZE lispval eval7(lispval expr,fd_lexenv env,fd_stack s)
{
  lispval result = fd_stack_eval(fd_get_arg(expr,1),env,s,0);
  return result;
}

static lispval list9(lispval arg1,lispval arg2,
                    lispval arg3,lispval arg4,
                    lispval arg5,lispval arg6,
                    lispval arg7,lispval arg8,
                    lispval arg9)
{
  return fd_make_list(9,fd_incref(arg1),fd_incref(arg2),
                      fd_incref(arg3),fd_incref(arg4),
                      fd_incref(arg5),fd_incref(arg6),
                      fd_incref(arg7),fd_incref(arg8),
                      fd_incref(arg9));
}



/* Initialization */

FD_EXPORT void fd_init_eval_debug_c()
{
  u8_register_source_file(_FILEINFO);

  fd_def_evalfn(fd_scheme_module,"DBG","",dbg_evalfn);

  fd_def_evalfn(fd_scheme_module,"EVAL1","",eval1);
  fd_def_evalfn(fd_scheme_module,"EVAL2","",eval2);
  fd_def_evalfn(fd_scheme_module,"EVAL3","",eval3);
  fd_def_evalfn(fd_scheme_module,"EVAL4","",eval4);
  fd_def_evalfn(fd_scheme_module,"EVAL5","",eval5);
  fd_def_evalfn(fd_scheme_module,"EVAL6","",eval6);
  fd_def_evalfn(fd_scheme_module,"EVAL7","",eval7);

  fd_register_config
    ("BUGDIR","Save exceptions to this directory",
     fd_sconfig_get,config_bugdir,&fd_bugdir);


  /* for testing */
  fd_idefn9(fd_scheme_module,"LIST9",list9,0,"Returns a nine-element list",
            -1,FD_FALSE,-1,FD_FALSE,-1,FD_FALSE,
            -1,FD_FALSE,-1,FD_FALSE,-1,FD_FALSE,
            -1,FD_FALSE,-1,FD_FALSE, -1,FD_FALSE);

  fd_def_evalfn(fd_scheme_module,"TIMEVAL","",timed_eval_evalfn);
  fd_def_evalfn(fd_scheme_module,"%TIMEVAL","",timed_evalx_evalfn);
  fd_def_evalfn(fd_scheme_module,"%WATCHPTR","",watchptr_evalfn);
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("%WATCHPTRVAL",watchptr_prim,1)));
  fd_def_evalfn(fd_scheme_module,"%WATCH","",watchpoint_evalfn);
  fd_def_evalfn(fd_scheme_module,"PROFILE","",profiled_eval_evalfn);
  fd_def_evalfn(fd_scheme_module,"%WATCHCALL","",watchcall_evalfn);
  fd_defalias(fd_scheme_module,"%WC","%WATCHCALL");
  fd_def_evalfn(fd_scheme_module,"%WATCHCALL+","",watchcall_plus_evalfn);
  fd_defalias(fd_scheme_module,"%WC+","%WATCHCALL+");

  fd_def_evalfn(fd_scheme_module,"%WCOND",
                "Reports (watches) which branch of a COND was taken",
                watched_cond_evalfn);
  fd_def_evalfn(fd_scheme_module,"%WTRY",
                "Reports (watches) which clause of a TRY succeeded",
                watched_try_evalfn);

  /* This pushes a log context */
  fd_def_evalfn(fd_scheme_module,"WITH-LOG-CONTEXT","",with_log_context_evalfn);

  fd_idefn2(fd_scheme_module,"DUMP-BUG",dumpbug_prim,1,
            "(DUMP-BUG *err* [*to*]) writes a DType representation of *err* "
            "to either *to* or the configured BUGDIR. If *err* is #t, "
            "returns a packet of the representation. Without *to* or "
            "if *to* is #f or #default, writes the exception into either "
            "'./errors/' or './'",
            fd_exception_type,FD_VOID,-1,FD_VOID);

  fd_idefn1(fd_xscheme_module,"MTRACE",mtrace_prim,FD_NEEDS_0_ARGS,
            "Activates LIBC heap tracing to MALLOC_TRACE and "
            "returns true if it worked. Optional argument is a "
            "filename to set as MALLOC_TRACE",
            -1,VOID);
  fd_idefn0(fd_xscheme_module,"MUNTRACE",muntrace_prim,
            "Deactivates LIBC heap tracing, returns true if it did anything");

#if USING_GOOGLE_PROFILER
  fd_def_evalfn(fd_scheme_module,"GOOGLE/PROFILE","",gprofile_evalfn);
  fd_idefn(fd_scheme_module,
           fd_make_cprim0("GOOGLE/PROFILE/STOP",gprofile_stop));
#endif
  fd_register_config
    ("GPROFILE","Set filename for the Google CPU profiler",
     fd_sconfig_get,fd_sconfig_set,&cpu_profilename);

  profile_symbol = fd_intern("%PROFILE");
  unquote_symbol = fd_intern("UNQUOTE");
  else_symbol = fd_intern("ELSE");
  apply_marker = fd_intern("=>");
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
